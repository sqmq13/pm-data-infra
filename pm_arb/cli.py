from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import fields
from pathlib import Path
from typing import Any

from .book import BookParseError, OrderBook, parse_ws_message
from .clob_rest import RestClient, fetch_book_with_retries
from .clob_ws import wait_for_decodable_book
from .config import Config
from .engine import Engine
from .gamma import fetch_markets, parse_clob_token_ids
from .reconcile import Reconciler
from .report import generate_report
from .sweep import sweep_cost


def _is_field_type(field_type, expected: type, expected_name: str) -> bool:
    if field_type is expected:
        return True
    if isinstance(field_type, str) and field_type == expected_name:
        return True
    return False


def _str2bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"invalid bool: {value}")


def _add_config_args(parser: argparse.ArgumentParser) -> None:
    for field in fields(Config):
        name = field.name.replace("_", "-")
        if _is_field_type(field.type, bool, "bool"):
            if field.name == "offline":
                group = parser.add_mutually_exclusive_group()
                group.add_argument(
                    f"--{name}",
                    dest=field.name,
                    nargs="?",
                    const=True,
                    default=None,
                    type=_str2bool,
                )
                group.add_argument(f"--no-{name}", dest=field.name, action="store_false")
                continue
            group = parser.add_mutually_exclusive_group()
            group.add_argument(f"--{name}", dest=field.name, action="store_true")
            group.add_argument(f"--no-{name}", dest=field.name, action="store_false")
            parser.set_defaults(**{field.name: None})
        else:
            parser.add_argument(f"--{name}", dest=field.name, default=None)


def _cli_overrides(ns: argparse.Namespace) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    for field in fields(Config):
        value = getattr(ns, field.name, None)
        if value is None:
            continue
        if _is_field_type(field.type, bool, "bool"):
            overrides[field.name] = _str2bool(value)
        elif _is_field_type(field.type, int, "int"):
            overrides[field.name] = int(value)
        elif _is_field_type(field.type, float, "float"):
            overrides[field.name] = float(value)
        else:
            overrides[field.name] = value
    return overrides


def contract_test_offline(config: Config, fixtures_dir: Path) -> int:
    ws_clean = json.loads((fixtures_dir / "ws_book_clean.json").read_text(encoding="utf-8"))
    ws_extra = json.loads((fixtures_dir / "ws_book_extra_fields.json").read_text(encoding="utf-8"))
    ws_bad = json.loads((fixtures_dir / "ws_book_malformed.json").read_text(encoding="utf-8"))
    rest_fixture = json.loads((fixtures_dir / "rest_book.json").read_text(encoding="utf-8"))
    if not ws_clean or not ws_extra or not ws_bad:
        raise RuntimeError("fixtures missing")
    # decode clean + extra
    for msg in ws_clean + ws_extra:
        parse_ws_message(msg)
    # malformed must raise
    malformed_ok = False
    for msg in ws_bad:
        try:
            parse_ws_message(msg)
        except BookParseError:
            malformed_ok = True
            break
    if not malformed_ok:
        raise RuntimeError("malformed fixture did not trigger parse error")
    # sweep sanity
    token_id, asks, _ = parse_ws_message(ws_clean[0])
    book = OrderBook(token_id=token_id)
    book.update_from_asks(asks, config.top_k)
    cost, _, ok = sweep_cost(book.asks, 10 * 1_000_000)
    if not ok or cost <= 0:
        raise RuntimeError("sweep cost invalid")
    rest_book = OrderBook(token_id=token_id)
    rest_book.update_from_rest(rest_fixture[token_id], config.top_k, price_scale=config.price_scale)
    # reconcile persistence
    recon = Reconciler(
        persist_n=config.reconcile_mismatch_persist_n,
        tick_tolerance=config.reconcile_tick_tolerance,
        rel_tol=config.reconcile_rel_tol,
    )
    rest_book.update_from_asks([(800_000, 10 * 1_000_000)], config.top_k)
    sizes = [10]
    for idx in range(config.reconcile_mismatch_persist_n - 1):
        result = recon.compare(token_id, book, rest_book, sizes)
        if result.desynced:
            raise RuntimeError("desynced too early")
    result = recon.compare(token_id, book, rest_book, sizes)
    if not result.desynced:
        raise RuntimeError("desync persistence failed")
    print("contract-test --offline PASS")
    return 0


def contract_test_online(config: Config) -> int:
    markets = fetch_markets(
        config.gamma_base_url,
        config.rest_timeout,
        limit=config.gamma_limit,
        max_markets=config.max_markets,
    )
    regex = config.market_regex
    engine = Engine(config)
    filtered = engine._filter_markets(markets)
    if filtered:
        candidates = filtered
    else:
        candidates = [
            market
            for market in engine._discover_candidates(markets)
            if market.get("enableOrderBook") is not False
        ]
    attempted_payloads: list[dict[str, Any]] = []
    for market in candidates[:200]:
        token_ids = parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids")
        )
        if len(token_ids) != 2:
            continue
        token_a, token_b = str(token_ids[0]), str(token_ids[1])
        rest = RestClient(
            base_url=config.clob_rest_base_url,
            timeout=config.rest_timeout,
            rate_per_sec=config.rest_rate_per_sec,
            burst=config.rest_burst,
        )
        book_a = fetch_book_with_retries(rest, token_a, config.rest_retry_max)
        book_b = fetch_book_with_retries(rest, token_b, config.rest_retry_max)
        if not book_a.get("asks") and not book_b.get("asks"):
            continue
        payload_used = None
        ws_msg = None
        for variant in ("A", "B", "C"):
            try:
                timeout = min(5.0, float(config.contract_timeout))
                payload_used, ws_msg = asyncio.run(
                    wait_for_decodable_book(
                        config.clob_ws_url,
                        [token_a, token_b],
                        variant,
                        timeout,
                        lambda msg: parse_ws_message(msg, price_scale=config.price_scale),
                    )
                )
                attempted_payloads.append(payload_used)
                break
            except Exception as exc:
                attempted_payloads.append({"variant": variant, "error": str(exc)})
        if payload_used is None:
            raise RuntimeError(f"WS contract failed; payloads attempted: {attempted_payloads}")
        if ws_msg is not None:
            _, asks, _ = parse_ws_message(ws_msg, price_scale=config.price_scale)
            if not asks:
                raise RuntimeError("WS contract failed: empty asks")
        recon = Reconciler(
            persist_n=config.reconcile_mismatch_persist_n,
            tick_tolerance=config.reconcile_tick_tolerance,
            rel_tol=config.reconcile_rel_tol,
        )
        ws_book = OrderBook(token_id=token_a)
        ws_book.update_from_rest(book_a, config.top_k, price_scale=config.price_scale)
        rest_book = OrderBook(token_id=token_a)
        rest_book.update_from_rest(book_a, config.top_k, price_scale=config.price_scale)
        result = recon.compare(token_a, ws_book, rest_book, [10])
        print(
            "contract-test PASS",
            json.dumps(
                {
                    "market_id": market.get("id"),
                    "token_a": token_a,
                    "token_b": token_b,
                    "payload": payload_used,
                    "reconcile_desynced": result.desynced,
                }
            ),
        )
        return 0
    raise RuntimeError(f"no suitable market found; regex={regex}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pm_arb")
    subparsers = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    _add_config_args(common)

    scan = subparsers.add_parser("scan", parents=[common])
    scan.add_argument("--fixtures-dir", default="testdata/fixtures")

    report = subparsers.add_parser("report", parents=[common])

    contract = subparsers.add_parser("contract-test", parents=[common])
    contract.add_argument("--fixtures-dir", default="testdata/fixtures")

    discover = subparsers.add_parser("discover", parents=[common])

    args = parser.parse_args(argv)
    overrides = _cli_overrides(args)
    config = Config.from_env_and_cli(overrides, os.environ)

    if args.command == "scan":
        engine = Engine(config, fixtures_dir=args.fixtures_dir)
        if config.offline:
            return engine.scan_offline()
        return engine.scan_online()
    if args.command == "report":
        generate_report(config.data_dir)
        print(f"report written to {Path(config.data_dir) / 'reports'}")
        return 0
    if args.command == "contract-test":
        if config.offline:
            return contract_test_offline(config, Path(args.fixtures_dir))
        return contract_test_online(config)
    if args.command == "discover":
        engine = Engine(config)
        candidates = engine.discover()
        for market in candidates:
            reasons = ",".join(market.get("_match_reasons", [])) or "no_match"
            print(f"{market.get('id','unknown')}\t{reasons}\t{market.get('question','')}")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
