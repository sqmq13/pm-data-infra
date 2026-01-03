from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests
import websockets

from pm_arb.book import BookParseError, parse_ws_message
from pm_arb.clob_ws import build_handshake_payload
from pm_arb.engine import EventLogger, SCHEMA_VERSION
from pm_arb.gamma import fetch_markets, parse_clob_token_ids
from pm_arb.ws_decode import decode_ws_raw

DEFAULT_MARKET_REGEX = r"(?i)^(btc|eth|sol|xrp)-updown-(5m|15m)-[0-9]+$"
NO_FEED_SECONDS = 30.0
BOOK_WINDOW_SECONDS = 30.0
USER_AGENT = "pm-arb-py/base-scan"


@dataclass
class BaseScanConfig:
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    clob_rest_base_url: str = "https://clob.polymarket.com"
    clob_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    market_regex: str = DEFAULT_MARKET_REGEX
    markets_poll_interval: float = 5.0
    max_markets: int = 20
    gamma_limit: int = 200
    rest_timeout: float = 10.0
    data_dir: str = "./data"
    heartbeat_interval: float = 5.0
    log_rotate_mb: int = 256
    log_rotate_keep: int = 20
    min_free_disk_gb: int = 5


def _now_wall() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_mono_ns() -> int:
    return time.monotonic_ns()


def _prune_times(window: deque[float], now: float, max_age: float) -> None:
    while window and now - window[0] > max_age:
        window.popleft()


class Recorder:
    def __init__(self, config: BaseScanConfig) -> None:
        self._logger = EventLogger(
            data_dir=config.data_dir,
            log_rotate_mb=config.log_rotate_mb,
            log_rotate_keep=config.log_rotate_keep,
            min_free_disk_gb=config.min_free_disk_gb,
        )
        self.run_id = uuid.uuid4().hex
        self.tainted = False
        self._disk_alarm_sent = False

    def record(self, record_type: str, **fields: Any) -> None:
        record = {
            "schema_version": SCHEMA_VERSION,
            "record_type": record_type,
            "run_id": self.run_id,
            "ts_wall": _now_wall(),
            "ts_mono_ns": _now_mono_ns(),
            "tainted": self.tainted,
        }
        record.update(fields)
        try:
            self._logger.write(record)
        except RuntimeError as exc:
            if "low disk" in str(exc):
                if not self._disk_alarm_sent:
                    self._disk_alarm_sent = True
                    self.tainted = True
                    alarm_record = {
                        "schema_version": SCHEMA_VERSION,
                        "record_type": "alarm",
                        "run_id": self.run_id,
                        "ts_wall": _now_wall(),
                        "ts_mono_ns": _now_mono_ns(),
                        "tainted": True,
                        "code": "disk_low",
                        "message": str(exc),
                    }
                    self._logger.write(alarm_record, skip_disk_check=True)
                raise SystemExit(2) from exc
            raise

    def alarm(self, code: str, message: str, **fields: Any) -> None:
        self.tainted = True
        self.record("alarm", code=code, message=message, **fields)


def _fetch_book(session: requests.Session, base_url: str, token_id: str, timeout: float) -> tuple[bool, str]:
    url = f"{base_url.rstrip('/')}/book"
    try:
        resp = session.get(url, params={"token_id": token_id}, timeout=timeout)
    except requests.RequestException as exc:
        return False, f"http_error:{exc}"
    if resp.status_code != 200:
        return False, f"http_status:{resp.status_code}"
    try:
        data = resp.json()
    except ValueError:
        return False, "malformed_json"
    if isinstance(data, dict) and "error" in data:
        error_text = str(data.get("error", ""))
        if "No orderbook exists" in error_text:
            return False, "no_orderbook"
        return False, f"error:{error_text}"
    if isinstance(data, dict) and "bids" in data and "asks" in data:
        return True, "book_ok"
    return False, "missing_bids_asks"


def _discover_and_validate(
    config: BaseScanConfig, regex: re.Pattern[str]
) -> tuple[list[str], dict[str, Any], list[tuple[str, str, bool, str]]]:
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    markets = fetch_markets(
        config.gamma_base_url,
        config.rest_timeout,
        limit=config.gamma_limit,
        max_markets=None,
    )
    try:
        markets = sorted(markets, key=lambda m: int(m.get("id", 0)), reverse=True)
    except (TypeError, ValueError):
        pass
    selected_tokens: list[str] = []
    details: list[tuple[str, str, bool, str]] = []
    candidates_checked = 0
    valid_books = 0
    candidate_markets = 0
    seen_tokens: set[str] = set()
    for market in markets:
        slug = str(market.get("slug", ""))
        if not regex.search(slug):
            continue
        token_ids = parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids")
        )
        if len(token_ids) != 2:
            continue
        candidate_markets += 1
        for token_id in token_ids:
            if token_id in seen_tokens:
                continue
            seen_tokens.add(token_id)
            candidates_checked += 1
            ok, reason = _fetch_book(session, config.clob_rest_base_url, token_id, config.rest_timeout)
            details.append((slug, token_id, ok, reason))
            if ok:
                valid_books += 1
                if len(selected_tokens) < config.max_markets:
                    selected_tokens.append(token_id)
            if len(selected_tokens) >= config.max_markets:
                break
        if len(selected_tokens) >= config.max_markets:
            break
    summary = {
        "candidates": candidates_checked,
        "candidate_markets": candidate_markets,
        "valid_books": valid_books,
        "selected": len(selected_tokens),
    }
    return selected_tokens, summary, details


class WsClient:
    def __init__(self, url: str, recorder: Recorder):
        self.url = url
        self.recorder = recorder
        self.ws: websockets.WebSocketClientProtocol | None = None
        self.subscribed: list[str] = []
        self.connected = False

    async def connect(self, tokens: list[str]) -> None:
        await self.close()
        if not tokens:
            self.subscribed = []
            return
        try:
            self.ws = await websockets.connect(self.url, ping_interval=20, ping_timeout=10)
            await self.ws.send(json.dumps(build_handshake_payload(tokens)))
            self.subscribed = list(tokens)
            self.connected = True
        except Exception as exc:
            self.connected = False
            self.ws = None
            self.recorder.alarm("ws_connect_failed", str(exc), ws_url=self.url)

    async def close(self) -> None:
        if self.ws is not None:
            try:
                await self.ws.close()
            except Exception:
                pass
        self.ws = None
        self.connected = False

    async def recv(self, timeout: float) -> Any | None:
        if self.ws is None:
            return None
        try:
            return await asyncio.wait_for(self.ws.recv(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        except Exception as exc:
            await self.close()
            self.recorder.alarm("ws_error", str(exc), ws_url=self.url)
            return None


async def _base_scan_loop(config: BaseScanConfig) -> int:
    recorder = Recorder(config)
    recorder.record(
        "run_header",
        mode="base_scan",
        ws_url=config.clob_ws_url,
        market_regex=config.market_regex,
        max_markets=config.max_markets,
        markets_poll_interval=config.markets_poll_interval,
    )
    regex = re.compile(config.market_regex)
    ws_client = WsClient(config.clob_ws_url, recorder)
    subscribed_tokens: list[str] = []
    last_book_any = time.monotonic()
    last_book_by_token: dict[str, float] = {}
    book_events: deque[float] = deque()
    pong_events: deque[float] = deque()
    no_feed_alarm_sent = False
    next_poll = time.monotonic()
    next_heartbeat = time.monotonic()
    discovery_task: asyncio.Task | None = None

    while True:
        now = time.monotonic()
        if now >= next_poll and discovery_task is None:
            discovery_task = asyncio.create_task(
                asyncio.to_thread(_discover_and_validate, config, regex)
            )
        if discovery_task is not None and discovery_task.done():
            try:
                selected_tokens, summary, _ = discovery_task.result()
            except Exception as exc:
                recorder.alarm("discovery_failed", str(exc))
                discovery_task = None
                next_poll = now + config.markets_poll_interval
                selected_tokens = subscribed_tokens
                summary = None
            if summary:
                recorder.record("market_poll_summary", **summary)
            discovery_task = None
            if selected_tokens != subscribed_tokens:
                await ws_client.connect(selected_tokens)
                subscribed_tokens = list(selected_tokens)
                last_book_by_token = {token: last_book_by_token.get(token, now) for token in subscribed_tokens}
                last_book_any = now
                no_feed_alarm_sent = False
            next_poll = now + config.markets_poll_interval

        raw = await ws_client.recv(timeout=0.2)
        if raw is not None:
            now = time.monotonic()
            # WS payloads are list-of-events; empty [] and "PONG" are keepalives.
            decoded = decode_ws_raw(raw)
            if decoded.is_pong:
                pong_events.append(now)
            elif not decoded.json_error:
                if decoded.book_items:
                    book_events.append(now)
                    last_book_any = now
                    for item in decoded.book_items:
                        try:
                            token_id, _, _ = parse_ws_message(item)
                        except BookParseError:
                            continue
                        last_book_by_token[token_id] = now
                    no_feed_alarm_sent = False

        _prune_times(book_events, now, BOOK_WINDOW_SECONDS)
        _prune_times(pong_events, now, BOOK_WINDOW_SECONDS)

        if subscribed_tokens and now - last_book_any > NO_FEED_SECONDS:
            if not no_feed_alarm_sent:
                stale_tokens = [
                    token
                    for token in subscribed_tokens
                    if now - last_book_by_token.get(token, 0.0) > NO_FEED_SECONDS
                ]
                recorder.alarm("no_feed", "no decodable book in 30s", tokens=stale_tokens)
                no_feed_alarm_sent = True
                await ws_client.connect(subscribed_tokens)
                last_book_any = now

        if now >= next_heartbeat:
            recorder.record(
                "heartbeat",
                ws_connected=ws_client.connected,
                subscribed_count=len(subscribed_tokens),
                books_last_30s=len(book_events),
                pongs_last_30s=len(pong_events),
            )
            next_heartbeat = now + config.heartbeat_interval

        await asyncio.sleep(0.05)


async def _probe_token(config: BaseScanConfig, token: str, timeout: float) -> int:
    async with websockets.connect(config.clob_ws_url, ping_interval=20, ping_timeout=10) as ws:
        await ws.send(json.dumps(build_handshake_payload([token])))
        end = time.monotonic() + timeout
        while True:
            remaining = end - time.monotonic()
            if remaining <= 0:
                print("no book-like message received")
                return 2
            raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            decoded = decode_ws_raw(raw)
            if decoded.is_pong or decoded.json_error or decoded.empty_array:
                continue
            if decoded.book_items:
                item = decoded.book_items[0]
                try:
                    token_id, asks, _ = parse_ws_message(item)
                except BookParseError:
                    continue
                bids = item.get("bids", [])
                keys = list(item.keys())
                print(
                    f"token={token_id}\tbids={len(bids)}\tasks={len(asks)}\tkeys={keys}"
                )
                return 0


def _print_once(config: BaseScanConfig) -> int:
    regex = re.compile(config.market_regex)
    _, summary, details = _discover_and_validate(config, regex)
    for slug, token_id, ok, reason in details:
        print(f"{slug}\t{token_id}\tbook_ok={str(ok).lower()}\t{reason}")
    print(
        f"candidates={summary['candidates']}\tvalid_books={summary['valid_books']}\tselected={summary['selected']}"
    )
    return 0


def _audit_http(
    config: BaseScanConfig, regex: re.Pattern[str], sample: int
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    markets = fetch_markets(
        config.gamma_base_url,
        config.rest_timeout,
        limit=config.gamma_limit,
        max_markets=None,
    )
    try:
        markets = sorted(markets, key=lambda m: int(m.get("id", 0)), reverse=True)
    except (TypeError, ValueError):
        pass
    tokens: list[tuple[str, str]] = []
    seen: set[str] = set()
    for market in markets:
        slug = str(market.get("slug", ""))
        if not regex.search(slug):
            continue
        token_ids = parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids")
        )
        for token_id in token_ids:
            if token_id in seen:
                continue
            seen.add(token_id)
            tokens.append((slug, token_id))
            if len(tokens) >= sample:
                break
        if len(tokens) >= sample:
            break
    results: list[dict[str, Any]] = []
    counts = {"BOOK_OK": 0, "NO_ORDERBOOK": 0, "HTTP_ERROR": 0, "MALFORMED": 0}
    for slug, token_id in tokens:
        status = "MALFORMED"
        reason = "unknown"
        url = f"{config.clob_rest_base_url.rstrip('/')}/book"
        try:
            resp = session.get(url, params={"token_id": token_id}, timeout=config.rest_timeout)
            if resp.status_code != 200:
                status = "HTTP_ERROR"
                body = resp.text[:200].replace("\n", " ")
                reason = f"http_status:{resp.status_code} body:{body}"
                if resp.status_code in {403, 429} or resp.status_code >= 500:
                    time.sleep(0.5)
            else:
                try:
                    data = resp.json()
                    if isinstance(data, dict) and "error" in data:
                        error_text = str(data.get("error", ""))
                        if "No orderbook exists" in error_text:
                            status = "NO_ORDERBOOK"
                            reason = error_text
                        else:
                            status = "MALFORMED"
                            reason = error_text
                    elif isinstance(data, dict) and "bids" in data and "asks" in data:
                        status = "BOOK_OK"
                        reason = "book_ok"
                    else:
                        status = "MALFORMED"
                        reason = "unexpected_shape"
                except ValueError:
                    status = "MALFORMED"
                    reason = "json_parse_error"
        except requests.RequestException as exc:
            status = "HTTP_ERROR"
            reason = str(exc)
        counts[status] += 1
        results.append(
            {
                "slug": slug,
                "token_id": token_id,
                "status": status,
                "reason": reason,
            }
        )
    return results, counts


def _audit_ws_probe(
    ws_url: str,
    tokens: list[str],
    audit_seconds: int,
    recorder: Recorder,
) -> dict[str, Any]:
    try:
        import websocket  # type: ignore
    except ImportError as exc:
        raise RuntimeError("websocket-client is required for audit mode") from exc

    if os.environ.get("WS_TRACE") == "1":
        websocket.enableTrace(True)

    ping_interval = 20
    ping_timeout = 10
    if ping_interval <= ping_timeout:
        ping_interval = ping_timeout + 5
    print(f"WS ping_interval={ping_interval} ping_timeout={ping_timeout}")

    stats = {
        "total_messages": 0,
        "pongs": 0,
        "empty_arrays": 0,
        "book_like": 0,
        "close_code": None,
        "close_reason": None,
        "errors": [],
        "samples": [],
        "per_token_last_book_ts": {},
        "ping_interval": ping_interval,
        "ping_timeout": ping_timeout,
    }
    lock = threading.Lock()

    def on_open(ws):
        ws.send(json.dumps(build_handshake_payload(tokens)))

    def on_message(ws, message):
        now = time.time()
        raw_text = message.decode("utf-8", errors="ignore") if isinstance(message, bytes) else str(message)
        decoded = decode_ws_raw(raw_text)
        with lock:
            stats["total_messages"] += 1
            if decoded.is_pong:
                stats["pongs"] += 1
                return
            if len(stats["samples"]) < 2:
                stats["samples"].append(raw_text[:1000])
            if decoded.json_error:
                stats["errors"].append("json_decode_error")
                return
            if decoded.empty_array:
                stats["empty_arrays"] += 1
            if decoded.book_items:
                stats["book_like"] += len(decoded.book_items)
                for item in decoded.book_items:
                    try:
                        token_id, _, _ = parse_ws_message(item)
                    except BookParseError:
                        continue
                    stats["per_token_last_book_ts"][token_id] = now

    def on_error(ws, error):
        with lock:
            stats["errors"].append(str(error))

    def on_close(ws, close_status_code, close_msg):
        with lock:
            stats["close_code"] = close_status_code
            stats["close_reason"] = close_msg

    ws_app = websocket.WebSocketApp(
        ws_url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close
    )
    thread = threading.Thread(
        target=ws_app.run_forever,
        kwargs={"ping_interval": ping_interval, "ping_timeout": ping_timeout},
        daemon=True,
    )
    thread.start()
    time.sleep(audit_seconds)
    ws_app.close()
    thread.join(timeout=5)
    for idx, sample in enumerate(stats["samples"]):
        recorder.record(
            "audit_ws_sample",
            sample_index=idx,
            message=sample,
        )
    return stats


def _audit(config: BaseScanConfig, args: argparse.Namespace) -> int:
    recorder = Recorder(config)
    regex = re.compile(args.audit_market_regex)
    recorder.record(
        "run_header",
        mode="audit",
        ws_url=config.clob_ws_url,
        market_regex=args.audit_market_regex,
    )
    results, counts = _audit_http(config, regex, args.audit_sample)
    book_ok = [r for r in results if r["status"] == "BOOK_OK"]
    print("HTTP audit counts:", counts)
    if counts["BOOK_OK"] == 0:
        print("No BOOK_OK tokens. Top candidates:")
        for row in results[:10]:
            print(f"{row['slug']}\t{row['token_id']}\t{row['status']}\t{row['reason']}")
        recorder.record("audit_summary", http_counts=counts, ws_counts=None, verdict="FAIL")
        return 1
    tokens = [row["token_id"] for row in book_ok[:5]]
    print("WS audit tokens:", tokens)
    ws_stats = _audit_ws_probe(config.clob_ws_url, tokens, args.audit_seconds, recorder)
    print("WS audit counts:", {k: ws_stats[k] for k in ("total_messages", "pongs", "empty_arrays", "book_like")})
    if ws_stats.get("close_code") is not None:
        print("WS close:", ws_stats.get("close_code"), ws_stats.get("close_reason"))
    if ws_stats.get("errors"):
        print("WS errors:", ws_stats.get("errors")[:3])
    for sample in ws_stats.get("samples", []):
        print("sample:", sample[:1000])
    verdict = "PASS" if ws_stats["book_like"] > 0 else "FAIL"
    likely_causes: list[str] = []
    if counts["BOOK_OK"] == 0:
        likely_causes.append(
            "Gamma market->token mapping isn't compatible with CLOB token_id selection; need different token field or filter for markets with active CLOB."
        )
    elif ws_stats["book_like"] == 0:
        if ws_stats["pongs"] > 0 or ws_stats["empty_arrays"] > 0:
            likely_causes.append(
                "WS subscription payload format mismatch OR wrong ws endpoint; verify exact subscribe message schema; compare to official client in repo (py-clob-client) and replicate."
            )
        elif ws_stats["total_messages"] == 0:
            likely_causes.append(
                "network/security group/firewall or TLS/WS handshake issue; check outbound 443 and verify websocket-client handshake logs."
            )
        if ws_stats.get("close_code") is not None:
            likely_causes.append(
                "rate limiting or protocol violation; add backoff and print close code/reason."
            )
    recorder.record(
        "audit_summary",
        http_counts=counts,
        ws_counts=ws_stats,
        verdict=verdict,
        likely_causes=likely_causes,
        tokens=tokens,
    )
    print("AUDIT VERDICT:", verdict)
    if likely_causes:
        for cause in likely_causes:
            print("likely_cause:", cause)
    return 0 if verdict == "PASS" else 2


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pm_arb.tools.base_scan")
    parser.add_argument("--gamma-base-url", default=BaseScanConfig.gamma_base_url)
    parser.add_argument("--clob-rest-base-url", default=BaseScanConfig.clob_rest_base_url)
    parser.add_argument("--clob-ws-url", default=BaseScanConfig.clob_ws_url)
    parser.add_argument("--market-regex", default=DEFAULT_MARKET_REGEX)
    parser.add_argument("--markets-poll-interval", type=float, default=BaseScanConfig.markets_poll_interval)
    parser.add_argument("--max-markets", type=int, default=BaseScanConfig.max_markets)
    parser.add_argument("--gamma-limit", type=int, default=BaseScanConfig.gamma_limit)
    parser.add_argument("--rest-timeout", type=float, default=BaseScanConfig.rest_timeout)
    parser.add_argument("--data-dir", default=BaseScanConfig.data_dir)
    parser.add_argument("--heartbeat-interval", type=float, default=BaseScanConfig.heartbeat_interval)
    parser.add_argument("--print-once", action="store_true", default=False)
    parser.add_argument("--probe-token")
    parser.add_argument("--probe-timeout", type=float, default=10.0)
    parser.add_argument("--audit", action="store_true", default=False)
    parser.add_argument("--audit-seconds", type=int, default=30)
    parser.add_argument("--audit-sample", type=int, default=50)
    parser.add_argument("--audit-market-regex", default=DEFAULT_MARKET_REGEX)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    config = BaseScanConfig(
        gamma_base_url=args.gamma_base_url,
        clob_rest_base_url=args.clob_rest_base_url,
        clob_ws_url=args.clob_ws_url,
        market_regex=args.market_regex,
        markets_poll_interval=args.markets_poll_interval,
        max_markets=args.max_markets,
        gamma_limit=args.gamma_limit,
        rest_timeout=args.rest_timeout,
        data_dir=args.data_dir,
        heartbeat_interval=args.heartbeat_interval,
    )
    if args.probe_token:
        return asyncio.run(_probe_token(config, args.probe_token, args.probe_timeout))
    if args.print_once:
        return _print_once(config)
    if args.audit:
        return _audit(config, args)
    return asyncio.run(_base_scan_loop(config))


if __name__ == "__main__":
    raise SystemExit(main())
