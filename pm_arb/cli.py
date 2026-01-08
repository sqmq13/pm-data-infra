from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import fields
from pathlib import Path
from typing import Any

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

from .config import Config
from .capture_offline import quantile, run_capture_offline
from .capture_inspect import audit_heartbeat_gaps, inspect_run, write_latency_report
from .capture_online import run_capture_online
from .capture_format import verify_frames
from .capture_slice import slice_run
from .gamma import fetch_markets, select_active_binary_markets


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


def _resolve_verify_targets(args: argparse.Namespace) -> list[tuple[Path, Path | None]]:
    if args.frames:
        if args.run_dir:
            raise ValueError("use --frames or --run-dir, not both")
        if args.idx and not args.frames:
            raise ValueError("--idx requires --frames")
        frames_path = Path(args.frames)
        idx_path = Path(args.idx) if args.idx else None
        return [(frames_path, idx_path)]
    if args.run_dir:
        run_dir = Path(args.run_dir)
        capture_dir = run_dir / "capture"
        frames_paths = sorted(capture_dir.glob("*.frames"))
        if not frames_paths:
            raise ValueError(f"no frames found in {capture_dir}")
        targets: list[tuple[Path, Path | None]] = []
        for frames_path in frames_paths:
            idx_path = frames_path.with_suffix(".idx")
            targets.append((frames_path, idx_path if idx_path.exists() else None))
        return targets
    raise ValueError("must provide --frames or --run-dir")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pm_arb")
    subparsers = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    _add_config_args(common)

    discover = subparsers.add_parser("discover", parents=[common])

    capture = subparsers.add_parser("capture", parents=[common])
    capture.add_argument("--run-id", default=None)
    capture.add_argument("--fixtures-dir", default="testdata/fixtures")

    capture_verify = subparsers.add_parser("capture-verify", parents=[common])
    capture_verify.add_argument("--frames", default=None)
    capture_verify.add_argument("--idx", default=None)
    capture_verify.add_argument("--run-dir", default=None)
    capture_verify.add_argument("--quiet", action="store_true", default=False)
    capture_verify.add_argument("--summary-only", action="store_true", default=False)

    capture_bench = subparsers.add_parser("capture-bench", parents=[common])
    capture_bench.add_argument("--run-id", default=None)
    capture_bench.add_argument("--fixtures-dir", default="testdata/fixtures")
    capture_bench.add_argument("--multiplier", type=int, default=1)

    capture_inspect = subparsers.add_parser("capture-inspect", parents=[common])
    capture_inspect.add_argument("--run-dir", required=True)

    capture_audit = subparsers.add_parser("capture-audit", parents=[common])
    capture_audit.add_argument("--run-dir", required=True)
    capture_audit.add_argument("--threshold-seconds", type=float, default=2.0)

    capture_latency_report = subparsers.add_parser(
        "capture-latency-report", parents=[common]
    )
    capture_latency_report.add_argument("--run-dir", required=True)

    capture_slice = subparsers.add_parser("capture-slice", parents=[common])
    capture_slice.add_argument("--run-dir", required=True)
    capture_slice.add_argument("--out-dir", default=None)
    capture_slice.add_argument("--slice-id", default=None)
    capture_slice.add_argument("--shard", type=int, default=None)
    capture_slice.add_argument("--start-mono-ns", type=int, default=None)
    capture_slice.add_argument("--end-mono-ns", type=int, default=None)
    capture_slice.add_argument("--start-offset", type=int, default=None)
    capture_slice.add_argument("--end-offset", type=int, default=None)

    args = parser.parse_args(argv)
    overrides = _cli_overrides(args)
    config = Config.from_env_and_cli(overrides, os.environ)

    if args.command == "discover":
        markets = fetch_markets(
            config.gamma_base_url,
            config.rest_timeout,
            limit=config.gamma_limit,
            max_markets=config.capture_max_markets,
        )
        candidates = select_active_binary_markets(
            markets, max_markets=config.capture_max_markets
        )
        for market in candidates:
            slug = market.get("slug", "")
            question = market.get("question", "")
            print(f"{market.get('id','unknown')}\tuniverse\t{slug}\t{question}")
        return 0
    if args.command == "capture":
        if config.offline:
            result = run_capture_offline(config, Path(args.fixtures_dir), run_id=args.run_id)
            print(f"capture run dir: {result.run.run_dir}")
            return 0
        return run_capture_online(config, run_id=args.run_id)
    if args.command == "capture-verify":
        try:
            targets = _resolve_verify_targets(args)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2
        summaries: list[dict[str, Any]] = []
        all_ok = True
        for frames_path, idx_path in targets:
            try:
                summary = verify_frames(frames_path, idx_path=idx_path)
            except FileNotFoundError as exc:
                print(f"file not found: {exc}", file=sys.stderr)
                return 2
            summary["frames_path"] = str(frames_path)
            summary["idx_path"] = str(idx_path) if idx_path is not None else None
            summaries.append(summary)
            all_ok = all_ok and summary.get("ok", False)
        summaries.sort(key=lambda entry: entry.get("frames_path") or "")
        totals = {
            "records": sum(entry.get("records", 0) for entry in summaries),
            "errors": sum(entry.get("errors", 0) for entry in summaries),
            "crc_mismatch": sum(entry.get("crc_mismatch", 0) for entry in summaries),
            "truncated_shards": [
                entry.get("frames_path") for entry in summaries if entry.get("truncated")
            ],
            "idx_bad_shards": [
                entry.get("frames_path")
                for entry in summaries
                if entry.get("idx_ok") is False
            ],
        }
        if not args.quiet:
            payload = {"ok": all_ok, "totals": totals}
            if not args.summary_only:
                payload["summaries"] = summaries
            print(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))
        return 0 if all_ok else 1
    if args.command == "capture-bench":
        if not config.offline:
            print("capture-bench requires --offline", file=sys.stderr)
            return 2
        result = run_capture_offline(
            config,
            Path(args.fixtures_dir),
            run_id=args.run_id,
            multiplier=args.multiplier,
        )
        elapsed_sec = max(result.elapsed_ns / 1_000_000_000.0, 1e-9)
        summary = {
            "run_id": result.run.run_id,
            "frames": result.stats.frames,
            "bytes_written": result.stats.bytes_written,
            "elapsed_ns": result.elapsed_ns,
            "msgs_per_sec": result.stats.frames / elapsed_sec,
            "bytes_per_sec": result.stats.bytes_written / elapsed_sec,
            "write_ns_p99": quantile(result.stats.write_durations_ns, 99),
            "ingest_ns_p99": quantile(result.stats.ingest_latencies_ns, 99),
        }
        print(json.dumps(summary, ensure_ascii=True, separators=(",", ":")))
        return 0
    if args.command == "capture-inspect":
        try:
            summary = inspect_run(Path(args.run_dir))
        except Exception as exc:
            print(str(exc), file=sys.stderr)
            return 2
        print(json.dumps(summary.payload, ensure_ascii=True, separators=(",", ":")))
        return 0
    if args.command == "capture-audit":
        try:
            summary = audit_heartbeat_gaps(
                Path(args.run_dir),
                threshold_seconds=args.threshold_seconds,
            )
        except Exception as exc:
            print(str(exc), file=sys.stderr)
            return 2
        print(json.dumps(summary, ensure_ascii=True, separators=(",", ":")))
        return 0
    if args.command == "capture-latency-report":
        try:
            report = write_latency_report(Path(args.run_dir))
        except Exception as exc:
            print(str(exc), file=sys.stderr)
            return 2
        print(json.dumps(report, ensure_ascii=True, separators=(",", ":")))
        return 0
    if args.command == "capture-slice":
        try:
            result = slice_run(
                Path(args.run_dir),
                out_dir=Path(args.out_dir) if args.out_dir else None,
                slice_id=args.slice_id,
                shard=args.shard,
                start_mono_ns=args.start_mono_ns,
                end_mono_ns=args.end_mono_ns,
                start_offset=args.start_offset,
                end_offset=args.end_offset,
            )
        except Exception as exc:
            print(str(exc), file=sys.stderr)
            return 2
        print(f"slice dir: {result.slice_dir}")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
