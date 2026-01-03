from __future__ import annotations

import asyncio
import json
import random
import re
import shutil
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .book import BookParseError, OrderBook, parse_ws_message
from .clob_rest import RestClient, RestError, fetch_book_with_retries, load_rest_book_fixture
from .clob_ws import build_handshake_payload
from .fixed import SIZE_SCALE, units_to_shares
from .gamma import fetch_markets, load_markets_fixture, parse_clob_token_ids
from .reconcile import Reconciler
from .sweep import sweep_cost
from .ws_decode import decode_ws_raw

SCHEMA_VERSION = 1


@dataclass
class TokenState:
    token_id: str
    book: OrderBook
    last_update_ns: int | None = None
    last_msg_ts: int | None = None
    last_book_ns: int | None = None
    updates_count: int = 0
    ooo_drops: int = 0
    missing_ts: int = 0
    no_feed: bool = False
    desynced: bool = False
    rest_unavailable: bool = False
    subscription_ns: int | None = None


@dataclass
class WindowState:
    active: bool = False
    start_ns: int = 0
    best_edge_micro: int = 0
    start_updates_a: int = 0
    start_updates_b: int = 0


@dataclass
class MarketState:
    market_id: str
    token_a: str
    token_b: str
    windows: dict[int, WindowState] = field(default_factory=dict)


class EventLogger:
    def __init__(self, data_dir: str, log_rotate_mb: int, log_rotate_keep: int, min_free_disk_gb: int):
        self.data_dir = Path(data_dir)
        self.log_rotate_mb = log_rotate_mb
        self.log_rotate_keep = log_rotate_keep
        self.min_free_disk_gb = min_free_disk_gb
        self.log_path = self._resolve_log_path()

    def _resolve_log_path(self) -> Path:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        log_dir = self.data_dir / "logs" / now
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir / "events.ndjson"

    def _rotate_if_needed(self, next_len: int) -> None:
        if self.log_path.exists():
            size = self.log_path.stat().st_size
        else:
            size = 0
        limit = self.log_rotate_mb * 1024 * 1024
        if size + next_len < limit:
            return
        oldest = self.log_path.with_name(f"events.ndjson.{self.log_rotate_keep}")
        if oldest.exists():
            oldest.unlink()
        for idx in range(self.log_rotate_keep - 1, 0, -1):
            src = self.log_path.with_name(f"events.ndjson.{idx}")
            dst = self.log_path.with_name(f"events.ndjson.{idx + 1}")
            if src.exists():
                src.rename(dst)
        rotated = self.log_path.with_name("events.ndjson.1")
        if rotated.exists():
            rotated.unlink()
        if self.log_path.exists():
            self.log_path.rename(rotated)

    def _check_disk(self) -> None:
        usage = shutil.disk_usage(self.data_dir)
        free_gb = usage.free / (1024**3)
        if free_gb < self.min_free_disk_gb:
            raise RuntimeError(f"low disk: {free_gb:.2f} GB free")

    def write(self, record: dict[str, Any], skip_disk_check: bool = False) -> None:
        payload = json.dumps(record, separators=(",", ":"), ensure_ascii=True)
        if not skip_disk_check:
            self._check_disk()
        self._rotate_if_needed(len(payload) + 1)
        with self.log_path.open("a", encoding="utf-8") as handle:
            handle.write(payload + "\n")


class Engine:
    def __init__(self, config, fixtures_dir: str | Path | None = None):
        self.config = config
        self.fixtures_dir = Path(fixtures_dir) if fixtures_dir else Path("testdata/fixtures")
        self.run_id = uuid.uuid4().hex
        self.start_ns = 0
        self.logger = EventLogger(
            data_dir=config.data_dir,
            log_rotate_mb=config.log_rotate_mb,
            log_rotate_keep=config.log_rotate_keep,
            min_free_disk_gb=config.min_free_disk_gb,
        )
        self.tainted = False
        self.ws_disconnected = False
        self.ws_disconnect_alarm_sent = False
        self.parse_failures = 0
        self.parse_total = 0
        self.coverage_low_polls = 0
        self._disk_alarm_sent = False
        self.ws_total_messages = 0
        self.ws_pongs = 0
        self.ws_empty_arrays = 0
        self.ws_book_like = 0
        self.ws_json_errors = 0
        self.last_ws_nonpong_ns: int | None = None
        self.last_ws_book_ns: int | None = None
        self._book_ok_cache: dict[str, bool] = {}
        self.market_states: dict[str, MarketState] = {}
        self.token_states: dict[str, TokenState] = {}
        self.reconciler = Reconciler(
            persist_n=config.reconcile_mismatch_persist_n,
            tick_tolerance=config.reconcile_tick_tolerance,
            rel_tol=config.reconcile_rel_tol,
        )
        self._sizes_shares: list[int] | None = None
        self._sizes_frozen = False
        self._auto_samples: list[int] = []
        self._rng = random.Random(0)

    def _now_wall(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _record(self, record_type: str, ts_ns: int, **fields: Any) -> None:
        record = {
            "schema_version": SCHEMA_VERSION,
            "record_type": record_type,
            "run_id": self.run_id,
            "ts_wall": self._now_wall(),
            "ts_mono_ns": ts_ns,
            "tainted": self.tainted,
        }
        record.update(fields)
        try:
            self.logger.write(record)
        except RuntimeError as exc:
            if "low disk" in str(exc):
                if not self._disk_alarm_sent:
                    self._disk_alarm_sent = True
                    self.tainted = True
                    alarm_record = {
                        "schema_version": SCHEMA_VERSION,
                        "record_type": "alarm",
                        "run_id": self.run_id,
                        "ts_wall": self._now_wall(),
                        "ts_mono_ns": ts_ns,
                        "tainted": True,
                        "code": "disk_low",
                        "message": str(exc),
                    }
                    self.logger.write(alarm_record, skip_disk_check=True)
                raise SystemExit(2) from exc
            raise

    def _alarm(self, ts_ns: int, code: str, message: str, **fields: Any) -> None:
        self.tainted = True
        self._record("alarm", ts_ns, code=code, message=message, **fields)

    def _load_markets(self) -> list[dict[str, Any]]:
        if self.config.offline:
            return load_markets_fixture(self.fixtures_dir / "gamma_markets.json")
        return fetch_markets(
            self.config.gamma_base_url,
            self.config.rest_timeout,
            limit=self.config.gamma_limit,
            max_markets=self.config.max_markets,
        )

    def _book_ok(self, rest_client: RestClient, token_id: str) -> bool:
        if token_id in self._book_ok_cache:
            return self._book_ok_cache[token_id]
        try:
            data = rest_client.fetch_book(token_id)
        except Exception:
            self._book_ok_cache[token_id] = False
            return False
        ok = isinstance(data, dict) and "bids" in data and "asks" in data
        self._book_ok_cache[token_id] = ok
        return ok

    def _filter_markets_by_book(self, markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rest_client = RestClient(
            base_url=self.config.clob_rest_base_url,
            timeout=self.config.rest_timeout,
            rate_per_sec=self.config.rest_rate_per_sec,
            burst=self.config.rest_burst,
        )
        filtered: list[dict[str, Any]] = []
        for market in markets:
            token_ids = parse_clob_token_ids(
                market.get("clobTokenIds") or market.get("clob_token_ids")
            )
            if len(token_ids) != 2:
                continue
            if not self._book_ok(rest_client, token_ids[0]):
                continue
            if not self._book_ok(rest_client, token_ids[1]):
                continue
            filtered.append(market)
            if len(filtered) >= self.config.max_markets:
                break
        return filtered

    def _market_text(self, market: dict[str, Any]) -> str:
        return " ".join(
            str(market.get(key, "")) for key in ("question", "title", "description", "slug")
        )

    def _market_regex(self) -> re.Pattern[str]:
        return re.compile(self.config.market_regex, re.MULTILINE)

    def _regex_haystack(self, market: dict[str, Any]) -> str:
        return f"{market.get('slug','')}\n{market.get('question','')}"

    def _secondary_match(self, text: str) -> bool:
        lower = text.lower()
        has_15 = "15" in lower
        has_min = ("min" in lower) or ("minute" in lower)
        has_asset = any(x in lower for x in ("btc", "eth", "sol"))
        return has_15 and has_min and has_asset

    def _filter_markets(self, markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        regex = self._market_regex()
        selected = []
        for market in markets:
            active = market.get("active")
            if active is not None and not active:
                continue
            token_ids = parse_clob_token_ids(
                market.get("clobTokenIds") or market.get("clob_token_ids")
            )
            if len(token_ids) != 2:
                continue
            text = self._market_text(market)
            regex_haystack = self._regex_haystack(market)
            reasons: list[str] = []
            if regex.search(regex_haystack):
                reasons.append("regex")
            if self.config.secondary_filter_enable and self._secondary_match(text):
                reasons.append("secondary")
            if reasons:
                market["_match_reasons"] = reasons
                selected.append(market)
        return selected[: self.config.max_markets]

    def _discover_candidates(self, markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        regex = self._market_regex()
        candidates = []
        for market in markets:
            active = market.get("active")
            if active is not None and not active:
                continue
            token_ids = parse_clob_token_ids(
                market.get("clobTokenIds") or market.get("clob_token_ids")
            )
            if len(token_ids) != 2:
                continue
            text = self._market_text(market)
            regex_haystack = self._regex_haystack(market)
            reasons: list[str] = []
            if regex.search(regex_haystack):
                reasons.append("regex")
            if self.config.secondary_filter_enable and self._secondary_match(text):
                reasons.append("secondary")
            market["_match_reasons"] = reasons
            candidates.append(market)
        return candidates

    def _init_states(self, markets: list[dict[str, Any]], now_ns: int) -> None:
        for market in markets:
            token_ids = parse_clob_token_ids(
                market.get("clobTokenIds") or market.get("clob_token_ids")
            )
            if len(token_ids) != 2:
                continue
            token_a, token_b = str(token_ids[0]), str(token_ids[1])
            self.market_states[market.get("id", token_a + ":" + token_b)] = MarketState(
                market_id=market.get("id", token_a + ":" + token_b),
                token_a=token_a,
                token_b=token_b,
            )
            for token_id in (token_a, token_b):
                if token_id not in self.token_states:
                    state = TokenState(token_id=token_id, book=OrderBook(token_id=token_id))
                    state.subscription_ns = now_ns
                    self.token_states[token_id] = state

    def _sizes(self) -> list[int]:
        if self._sizes_frozen and self._sizes_shares:
            return self._sizes_shares
        if self._sizes_shares:
            return self._sizes_shares
        parsed = self.config.sizes_list()
        if parsed is None:
            return []
        self._sizes_shares = parsed
        return parsed

    def _maybe_freeze_sizes(self, now_ns: int, start_ns: int) -> None:
        if self.config.sizes.strip().lower() != "auto":
            return
        if self._sizes_frozen:
            return
        warmup_ns = self.config.sizes_auto_warmup_minutes * 60 * 1_000_000_000
        if now_ns - start_ns < warmup_ns:
            return
        if not self._auto_samples:
            self._sizes_shares = [10]
        else:
            sorted_samples = sorted(self._auto_samples)
            def percentile(p: float) -> int:
                idx = int(len(sorted_samples) * p)
                idx = min(max(idx, 0), len(sorted_samples) - 1)
                return sorted_samples[idx]
            sizes = [percentile(0.50), percentile(0.75), percentile(0.90)]
            sizes = [s for s in sizes if s > 0]
            if 10 not in sizes:
                sizes.append(10)
            sizes = sorted(set(sizes))
            self._sizes_shares = sizes
        self._sizes_frozen = True
        self._record(
            "run_header",
            now_ns,
            sizes=self._sizes_shares,
            auto_sizes_frozen=True,
            sizes_auto_warmup_minutes=self.config.sizes_auto_warmup_minutes,
        )

    def _collect_auto_sample(self) -> None:
        for market in self.market_states.values():
            token_a = self.token_states.get(market.token_a)
            token_b = self.token_states.get(market.token_b)
            if not token_a or not token_b:
                continue
            if not token_a.book.asks or not token_b.book.asks:
                continue
            max_a_units = token_a.book.max_fillable_units()
            max_b_units = token_b.book.max_fillable_units()
            m_units = min(max_a_units, max_b_units)
            if m_units <= 0:
                continue
            shares = units_to_shares(m_units)
            if shares > 0:
                self._auto_samples.append(shares)

    def _window_end(self, market: MarketState, size: int, now_ns: int, reason: str) -> None:
        window = market.windows.get(size)
        if not window or not window.active:
            return
        duration_ms = int((now_ns - window.start_ns) / 1_000_000)
        token_a_state = self.token_states[market.token_a]
        token_b_state = self.token_states[market.token_b]
        churn_a = token_a_state.updates_count - window.start_updates_a
        churn_b = token_b_state.updates_count - window.start_updates_b
        self._record(
            "window_end",
            now_ns,
            market_id=market.market_id,
            token_a=market.token_a,
            token_b=market.token_b,
            size=size,
            duration_ms=duration_ms,
            best_edge_per_share_micro=window.best_edge_micro,
            churn_a=churn_a,
            churn_b=churn_b,
            end_reason=reason,
        )
        window.active = False

    def _end_market_windows(self, market: MarketState, now_ns: int, reason: str) -> None:
        for size in list(market.windows.keys()):
            self._window_end(market, size, now_ns, reason)

    def _end_all_windows(self, now_ns: int, reason: str) -> None:
        for market in self.market_states.values():
            self._end_market_windows(market, now_ns, reason)

    def _maybe_start_or_update_window(
        self, market: MarketState, size: int, now_ns: int, edge_ok: bool, edge_micro: int
    ) -> None:
        window = market.windows.setdefault(size, WindowState())
        if not window.active:
            if edge_ok and not self.tainted:
                token_a_state = self.token_states[market.token_a]
                token_b_state = self.token_states[market.token_b]
                window.active = True
                window.start_ns = now_ns
                window.best_edge_micro = edge_micro
                window.start_updates_a = token_a_state.updates_count
                window.start_updates_b = token_b_state.updates_count
                self._record(
                    "window_start",
                    now_ns,
                    market_id=market.market_id,
                    token_a=market.token_a,
                    token_b=market.token_b,
                    size=size,
                    edge_per_share_micro=edge_micro,
                )
            return
        if edge_ok:
            if edge_micro > window.best_edge_micro:
                window.best_edge_micro = edge_micro
        else:
            self._window_end(market, size, now_ns, "edge_flipped")

    def _check_integrity(self, now_ns: int) -> None:
        stale_tokens = set()
        for token in self.token_states.values():
            if token.last_update_ns is None:
                continue
            if now_ns - token.last_update_ns > int(self.config.stale_seconds * 1_000_000_000):
                stale_tokens.add(token.token_id)
        if not stale_tokens:
            return
        for market in self.market_states.values():
            if market.token_a in stale_tokens or market.token_b in stale_tokens:
                self._end_market_windows(market, now_ns, "token_stale")

    def _apply_status_endings(self, now_ns: int) -> None:
        for market in self.market_states.values():
            token_a = self.token_states[market.token_a]
            token_b = self.token_states[market.token_b]
            if token_a.desynced or token_b.desynced:
                self._end_market_windows(market, now_ns, "token_desynced")
            if token_a.no_feed or token_b.no_feed:
                self._end_market_windows(market, now_ns, "token_no_feed")
            if token_a.rest_unavailable or token_b.rest_unavailable:
                self._end_market_windows(market, now_ns, "reconcile_failed")
        if self.ws_disconnected:
            self._end_all_windows(now_ns, "ws_disconnected")

    def _check_no_feed(self, now_ns: int) -> None:
        for token in self.token_states.values():
            if token.no_feed:
                continue
            if token.subscription_ns is None:
                continue
            threshold_ns = 30 * 1_000_000_000
            if token.last_book_ns is None:
                if now_ns - token.subscription_ns > threshold_ns:
                    token.no_feed = True
                    self._alarm(now_ns, "no_feed", "no decodable book in 30s", token_id=token.token_id)
            else:
                if now_ns - token.last_book_ns > threshold_ns:
                    token.no_feed = True
                    self._alarm(now_ns, "no_feed", "no decodable book in 30s", token_id=token.token_id)

    def _maybe_heartbeat(self, now_ns: int, next_heartbeat_ns: int) -> int:
        if now_ns < next_heartbeat_ns:
            return next_heartbeat_ns
        self._record(
            "heartbeat",
            now_ns,
            ws_disconnected=self.ws_disconnected,
            tokens=len(self.token_states),
            markets=len(self.market_states),
            ws_total_messages=self.ws_total_messages,
            ws_pongs=self.ws_pongs,
            ws_empty_arrays=self.ws_empty_arrays,
            ws_book_like=self.ws_book_like,
            ws_json_decode_errors=self.ws_json_errors,
        )
        return now_ns + int(self.config.heartbeat_interval * 1_000_000_000)

    def _maybe_reconcile(self, now_ns: int, next_reconcile_ns: int) -> int:
        if now_ns < next_reconcile_ns:
            return next_reconcile_ns
        if not self.token_states:
            return now_ns + int(self.config.resync_interval * 1_000_000_000)
        token_ids = list(self.token_states.keys())
        sample_count = max(
            self.config.rest_snapshot_sample_min,
            int(len(token_ids) * self.config.rest_snapshot_sample_pct + 0.999),
        )
        sample_count = min(sample_count, len(token_ids))
        if self.config.offline:
            sample_ids = token_ids[:sample_count]
        else:
            sample_ids = self._rng.sample(token_ids, sample_count)
        rest_client = RestClient(
            base_url=self.config.clob_rest_base_url,
            timeout=self.config.rest_timeout,
            rate_per_sec=self.config.rest_rate_per_sec,
            burst=self.config.rest_burst,
        )
        rest_fixture = None
        if self.config.offline:
            rest_fixture = load_rest_book_fixture(self.fixtures_dir / "rest_book.json")
        sizes = self._sizes()
        if not sizes:
            return now_ns + int(self.config.resync_interval * 1_000_000_000)
        for token_id in sample_ids:
            token_state = self.token_states[token_id]
            try:
                if rest_fixture is not None:
                    book_data = rest_fixture[token_id]
                else:
                    book_data = fetch_book_with_retries(rest_client, token_id, self.config.rest_retry_max)
                rest_book = OrderBook(token_id=token_id)
                rest_book.update_from_rest(book_data, self.config.top_k, price_scale=self.config.price_scale)
                token_state.rest_unavailable = False
                result = self.reconciler.compare(token_id, token_state.book, rest_book, sizes)
                token_state.desynced = result.desynced
                if result.immediate:
                    self._alarm(now_ns, "reconcile_huge_mismatch", "immediate desync", token_id=token_id)
                self._record(
                    "reconcile_result",
                    now_ns,
                    token_id=token_id,
                    mismatch=result.mismatch,
                    mismatch_count=result.mismatch_count,
                    desynced=result.desynced,
                    immediate=result.immediate,
                )
            except (RestError, KeyError) as exc:
                token_state.rest_unavailable = True
                self._alarm(now_ns, "reconcile_failed", str(exc), token_id=token_id)
        return now_ns + int(self.config.resync_interval * 1_000_000_000)

    def _handle_ws_message(self, message: dict[str, Any], now_ns: int) -> None:
        if isinstance(message, list):
            for item in message:
                self._handle_ws_message(item, now_ns)
            return
        self.parse_total += 1
        try:
            token_id, asks, ts = parse_ws_message(message, price_scale=self.config.price_scale)
        except BookParseError as exc:
            self.parse_failures += 1
            fail_pct = self.parse_failures / max(1, self.parse_total)
            if self.parse_total <= 100 and fail_pct > self.config.ws_initial_parse_fail_threshold_pct:
                self._alarm(now_ns, "parse_taint", "initial ws parse failures high")
                self._end_all_windows(now_ns, "parse_taint")
            return
        token_state = self.token_states.get(token_id)
        if not token_state:
            return
        if ts is None:
            token_state.missing_ts += 1
        if ts is not None and token_state.last_msg_ts is not None and ts < token_state.last_msg_ts:
            token_state.ooo_drops += 1
            return
        token_state.last_msg_ts = ts if ts is not None else token_state.last_msg_ts
        token_state.book.update_from_asks(asks, self.config.top_k)
        token_state.last_update_ns = now_ns
        token_state.last_book_ns = now_ns
        token_state.updates_count += 1

    def _evaluate_edges(self, now_ns: int) -> None:
        sizes = self._sizes()
        if not sizes:
            return
        buffer_micro = self.config.buffer_per_share_micro()
        for market in self.market_states.values():
            token_a = self.token_states[market.token_a]
            token_b = self.token_states[market.token_b]
            if self.ws_disconnected or self.tainted:
                continue
            if token_a.no_feed or token_b.no_feed:
                continue
            if token_a.desynced or token_b.desynced:
                continue
            if token_a.rest_unavailable or token_b.rest_unavailable:
                continue
            if token_a.last_update_ns is None or token_b.last_update_ns is None:
                continue
            if now_ns - token_a.last_update_ns > int(self.config.stale_seconds * 1_000_000_000):
                continue
            if now_ns - token_b.last_update_ns > int(self.config.stale_seconds * 1_000_000_000):
                continue
            if not token_a.book.asks or not token_b.book.asks:
                continue
            for size_shares in sizes:
                size_units = size_shares * SIZE_SCALE
                cost_a, _, ok_a = sweep_cost(token_a.book.asks, size_units)
                cost_b, _, ok_b = sweep_cost(token_b.book.asks, size_units)
                if not ok_a or not ok_b:
                    continue
                allowed = size_shares * (self.config.price_scale - buffer_micro)
                total_cost = cost_a + cost_b
                edge_ok = total_cost <= allowed
                edge_micro = 0
                if edge_ok and size_shares > 0:
                    edge_micro = max(0, (allowed - total_cost) // size_shares)
                self._maybe_start_or_update_window(market, size_shares, now_ns, edge_ok, edge_micro)

    def _capture_ws_sample(self, raw: dict[str, Any], capture_path: Path, count: int) -> None:
        capture_path.parent.mkdir(parents=True, exist_ok=True)
        with capture_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(raw, separators=(",", ":"), ensure_ascii=True) + "\n")

    def scan_offline(self) -> int:
        markets = self._load_markets()
        selected = self._filter_markets(markets)
        now_ns = 0
        self.start_ns = now_ns
        self._init_states(selected, now_ns)
        self._record(
            "run_header",
            now_ns,
            config={
                "sizes": self.config.sizes,
                "market_regex": self.config.market_regex,
            },
        )
        if len(selected) < 10:
            self.coverage_low_polls = 1
        self._record(
            "market_poll_summary",
            now_ns,
            selected=len(selected),
            coverage_low_polls=self.coverage_low_polls,
        )
        ws_files = [
            "ws_book_clean.json",
            "ws_book_extra_fields.json",
            "ws_book_malformed.json",
            "ws_book_out_of_order.json",
        ]
        messages: list[dict[str, Any]] = []
        for name in ws_files:
            path = self.fixtures_dir / name
            with open(path, "r", encoding="utf-8") as handle:
                messages.extend(json.load(handle))
        next_heartbeat = 0
        next_reconcile = 0
        capture_path = Path(self.config.data_dir) / "debug" / "ws_samples.jsonl"
        capture_count = 0
        for msg in messages:
            msg_ts = msg.get("ts") or msg.get("timestamp") or 0
            now_ns = int(msg_ts) * 1_000_000
            if capture_count < self.config.ws_sample_capture_n:
                self._capture_ws_sample(msg, capture_path, capture_count)
                capture_count += 1
            self._handle_ws_message(msg, now_ns)
            if self.config.sizes.strip().lower() == "auto":
                self._collect_auto_sample()
                self._maybe_freeze_sizes(now_ns, self.start_ns)
            self._evaluate_edges(now_ns)
            self._check_integrity(now_ns)
            self._apply_status_endings(now_ns)
            next_heartbeat = self._maybe_heartbeat(now_ns, next_heartbeat)
            next_reconcile = self._maybe_reconcile(now_ns, next_reconcile)
        now_ns += int(self.config.stale_seconds * 1_000_000_000) + 1
        self._check_integrity(now_ns)
        self._end_all_windows(now_ns, "token_stale")
        self._record("heartbeat", now_ns, final=True)
        return 0

    def scan_online(self) -> int:
        asyncio.run(self._scan_online_async())
        return 0 if not self.tainted else 2

    def _geoblock_check(self, now_ns: int) -> None:
        try:
            import requests

            resp = requests.get(self.config.geoblock_url, timeout=self.config.rest_timeout)
            if resp.status_code >= 400:
                self._alarm(now_ns, "geoblock_check_failed", f"status {resp.status_code}")
        except Exception as exc:
            self._alarm(now_ns, "geoblock_check_failed", str(exc))

    async def _ws_reader(self, url: str, payload: dict[str, Any], queue: asyncio.Queue) -> None:
        while True:
            try:
                import websockets

                async with websockets.connect(url) as ws:
                    await ws.send(json.dumps(payload))
                    while True:
                        raw = await ws.recv()
                        await queue.put(raw)
            except Exception:
                await asyncio.sleep(1.0)

    async def _scan_online_async(self) -> None:
        markets = self._load_markets()
        selected = self._filter_markets(markets)
        if not self.config.offline:
            selected = self._filter_markets_by_book(selected)
        now_ns = time.monotonic_ns()
        self.last_ws_nonpong_ns = now_ns
        self.start_ns = now_ns
        self._init_states(selected, now_ns)
        self._geoblock_check(now_ns)
        self._record(
            "run_header",
            now_ns,
            config={
                "sizes": self.config.sizes,
                "market_regex": self.config.market_regex,
            },
        )
        asset_ids = sorted({t.token_id for t in self.token_states.values()})
        payload = build_handshake_payload(asset_ids)
        queue: asyncio.Queue = asyncio.Queue()
        ws_task = asyncio.create_task(self._ws_reader(self.config.clob_ws_url, payload, queue))
        next_heartbeat = now_ns
        next_reconcile = now_ns
        next_poll = now_ns
        capture_path = Path(self.config.data_dir) / "debug" / "ws_samples.jsonl"
        capture_count = 0
        try:
            while True:
                now_ns = time.monotonic_ns()
                try:
                    raw = await asyncio.wait_for(queue.get(), timeout=0.25)
                    # WS payloads are often list-of-events, plus "PONG" and empty [] keepalives.
                    self.ws_total_messages += 1
                    decoded = decode_ws_raw(raw)
                    if decoded.is_pong:
                        self.ws_pongs += 1
                    else:
                        self.last_ws_nonpong_ns = now_ns
                        if decoded.json_error:
                            self.ws_json_errors += 1
                        else:
                            if decoded.empty_array:
                                self.ws_empty_arrays += 1
                            if decoded.book_items:
                                self.ws_book_like += len(decoded.book_items)
                                self.last_ws_book_ns = now_ns
                                for item in decoded.book_items:
                                    if capture_count < self.config.ws_sample_capture_n:
                                        self._capture_ws_sample(item, capture_path, capture_count)
                                        capture_count += 1
                                    self._handle_ws_message(item, now_ns)
                    if self.config.sizes.strip().lower() == "auto":
                        self._collect_auto_sample()
                        self._maybe_freeze_sizes(now_ns, self.start_ns)
                    self._evaluate_edges(now_ns)
                except asyncio.TimeoutError:
                    pass
                if now_ns >= next_poll:
                    selected = self._filter_markets(self._load_markets())
                    if not self.config.offline:
                        selected = self._filter_markets_by_book(selected)
                    if len(selected) < 10:
                        self.coverage_low_polls += 1
                    else:
                        self.coverage_low_polls = 0
                    if self.coverage_low_polls >= 2:
                        dump_path = Path(self.config.data_dir) / "debug" / "coverage_dump.jsonl"
                        dump_path.parent.mkdir(parents=True, exist_ok=True)
                        candidates = self._discover_candidates(markets)[:20]
                        with dump_path.open("a", encoding="utf-8") as handle:
                            handle.write(
                                json.dumps(candidates, separators=(",", ":"), ensure_ascii=True) + "\n"
                            )
                        self._record("coverage_dump", now_ns, count=len(candidates))
                        self._alarm(now_ns, "coverage_low", "selected markets below threshold")
                        self._end_all_windows(now_ns, "coverage_low_taint")
                    self._record(
                        "market_poll_summary",
                        now_ns,
                        selected=len(selected),
                        coverage_low_polls=self.coverage_low_polls,
                    )
                    next_poll = now_ns + int(self.config.markets_poll_interval * 1_000_000_000)
                if self.last_ws_nonpong_ns is not None:
                    if now_ns - self.last_ws_nonpong_ns > int(
                        self.config.ws_disconnect_alarm_seconds * 1_000_000_000
                    ):
                        if not self.ws_disconnect_alarm_sent:
                            self.ws_disconnected = True
                            self._alarm(now_ns, "ws_disconnected", "no ws messages")
                            self._end_all_windows(now_ns, "ws_disconnected")
                            self.ws_disconnect_alarm_sent = True
                    elif self.ws_disconnected:
                        self.ws_disconnected = False
                        self.ws_disconnect_alarm_sent = False
                self._check_integrity(now_ns)
                self._apply_status_endings(now_ns)
                self._check_no_feed(now_ns)
                next_heartbeat = self._maybe_heartbeat(now_ns, next_heartbeat)
                next_reconcile = self._maybe_reconcile(now_ns, next_reconcile)
        finally:
            ws_task.cancel()

    def discover(self) -> list[dict[str, Any]]:
        markets = self._load_markets()
        candidates = self._discover_candidates(markets)
        return candidates[:50]
