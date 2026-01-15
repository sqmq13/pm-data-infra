from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable

import hashlib
import requests

# Polymarket Maker Rebates docs (fee-rate endpoint + fee table examples):
# https://docs.polymarket.com/
FEE_RATE_ENDPOINT = "/fee-rate"
FEE_RATE_USER_AGENT = "pm-data-infra/phase1"

FEE_TABLE_PRECISION = Decimal("0.0001")
PRICE_TABLE_PRECISION = Decimal("0.01")


class UnknownFeeRate(Exception):
    pass


@dataclass(frozen=True)
class FeeRateResult:
    token_id: str
    fee_rate_bps: int | None
    fee_rate_known: bool
    fee_enabled: bool | None
    wall_ns_utc: int
    mono_ns: int
    error: str | None = None


@dataclass(frozen=True)
class FeeRateCacheEntry:
    result: FeeRateResult
    expires_at_mono_ns: int


@dataclass(frozen=True)
class FeeRegimeState:
    state: str
    reason: str | None
    sampled_tokens_count: int
    unknown_fee_count: int
    observed_fee_rates: list[int]


class FeeRateClient:
    def __init__(
        self,
        *,
        base_url: str,
        timeout_seconds: float,
        cache_ttl_seconds: float,
        max_in_flight: int,
        fetcher=None,
        monotonic_ns=None,
        wall_ns_utc=None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._cache_ttl_ns = int(cache_ttl_seconds * 1_000_000_000)
        self._semaphore = asyncio.Semaphore(max_in_flight)
        self._cache: dict[str, FeeRateCacheEntry] = {}
        self._inflight: dict[str, asyncio.Task[FeeRateResult]] = {}
        self._fetcher = fetcher or self._fetch_fee_rate_sync
        self._monotonic_ns = monotonic_ns or time.perf_counter_ns
        self._wall_ns_utc = wall_ns_utc or time.time_ns
        self._cache_hits = 0
        self._cache_misses = 0
        self._requests = 0
        self._errors = 0
        self._timeouts = 0

    def stats_snapshot(self) -> dict[str, int]:
        return {
            "cache_hits": self._cache_hits,
            "cache_misses": self._cache_misses,
            "requests": self._requests,
            "errors": self._errors,
            "timeouts": self._timeouts,
            "in_flight": len(self._inflight),
            "cache_entries": len(self._cache),
        }

    def cached_fee_rates(self, token_ids: Iterable[str]) -> dict[str, FeeRateResult]:
        now_ns = self._monotonic_ns()
        results: dict[str, FeeRateResult] = {}
        for token_id in token_ids:
            entry = self._cache.get(str(token_id))
            if entry is None or entry.expires_at_mono_ns <= now_ns:
                continue
            results[str(token_id)] = entry.result
        return results

    async def get_fee_rate(
        self,
        token_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> FeeRateResult:
        token_id = str(token_id)
        now_ns = self._monotonic_ns()
        entry = self._cache.get(token_id)
        if entry and entry.expires_at_mono_ns > now_ns:
            self._cache_hits += 1
            return entry.result
        self._cache_misses += 1
        inflight = self._inflight.get(token_id)
        if inflight is not None:
            return await inflight
        task = asyncio.create_task(
            self._fetch_and_cache(token_id, timeout_seconds=timeout_seconds)
        )
        self._inflight[token_id] = task
        try:
            return await task
        finally:
            self._inflight.pop(token_id, None)

    async def prefetch_fee_rates(
        self,
        token_ids: Iterable[str],
        *,
        timeout_seconds: float,
        max_tokens: int,
    ) -> dict[str, FeeRateResult]:
        tokens = []
        seen: set[str] = set()
        for token_id in token_ids:
            token_key = str(token_id)
            if token_key in seen:
                continue
            seen.add(token_key)
            tokens.append(token_key)
            if len(tokens) >= max_tokens:
                break
        tasks = [
            asyncio.create_task(self.get_fee_rate(token, timeout_seconds=timeout_seconds))
            for token in tokens
        ]
        results: dict[str, FeeRateResult] = {}
        if not tasks:
            return results
        done, pending = await asyncio.wait(tasks, timeout=timeout_seconds)
        for task in done:
            try:
                result = task.result()
            except Exception:
                continue
            results[result.token_id] = result
        for task in pending:
            task.cancel()
        return results

    def _fetch_fee_rate_sync(self, token_id: str) -> int:
        url = f"{self._base_url}{FEE_RATE_ENDPOINT}"
        resp = requests.get(
            url,
            params={"token_id": token_id},
            timeout=self._timeout_seconds,
            headers={"User-Agent": FEE_RATE_USER_AGENT},
        )
        resp.raise_for_status()
        payload = resp.json()
        fee_rate_bps = payload.get("fee_rate_bps") if isinstance(payload, dict) else None
        if not isinstance(fee_rate_bps, int):
            raise ValueError("fee_rate_bps missing or invalid")
        return fee_rate_bps

    async def _fetch_and_cache(
        self,
        token_id: str,
        *,
        timeout_seconds: float | None,
    ) -> FeeRateResult:
        timeout = timeout_seconds if timeout_seconds is not None else self._timeout_seconds
        async with self._semaphore:
            self._requests += 1
            try:
                fee_rate_bps = await asyncio.wait_for(
                    asyncio.to_thread(self._fetcher, token_id),
                    timeout=timeout,
                )
                fee_rate_known = True
                fee_enabled = fee_rate_bps > 0
                error = None
            except asyncio.TimeoutError:
                self._timeouts += 1
                fee_rate_bps = None
                fee_rate_known = False
                fee_enabled = None
                error = "timeout"
            except Exception as exc:
                self._errors += 1
                fee_rate_bps = None
                fee_rate_known = False
                fee_enabled = None
                error = type(exc).__name__
        now_mono_ns = self._monotonic_ns()
        now_wall_ns = self._wall_ns_utc()
        result = FeeRateResult(
            token_id=token_id,
            fee_rate_bps=fee_rate_bps,
            fee_rate_known=fee_rate_known,
            fee_enabled=fee_enabled,
            wall_ns_utc=now_wall_ns,
            mono_ns=now_mono_ns,
            error=error,
        )
        expires_at = now_mono_ns + self._cache_ttl_ns
        self._cache[token_id] = FeeRateCacheEntry(result=result, expires_at_mono_ns=expires_at)
        return result


def _as_decimal(value: Any) -> Decimal | None:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return Decimal(text)
        except Exception:
            return None
    return None


# Doc-table-based estimator from Maker Rebates fee examples (1/100 share points only).
# Non-table points intentionally return None (no closed-form curve asserted).
# https://docs.polymarket.com/
FEE_TABLE_BY_SHARES: dict[int, dict[Decimal, Decimal]] = {
    1: {
        Decimal("0.01"): Decimal("0"),
        Decimal("0.05"): Decimal("0.0006"),
        Decimal("0.10"): Decimal("0.002"),
        Decimal("0.20"): Decimal("0.0064"),
        Decimal("0.30"): Decimal("0.011"),
        Decimal("0.40"): Decimal("0.0144"),
        Decimal("0.50"): Decimal("0.0156"),
        Decimal("0.60"): Decimal("0.0144"),
        Decimal("0.70"): Decimal("0.011"),
        Decimal("0.80"): Decimal("0.0064"),
        Decimal("0.90"): Decimal("0.002"),
        Decimal("0.99"): Decimal("0"),
    },
    100: {
        Decimal("1.00"): Decimal("0.0025"),
        Decimal("5.00"): Decimal("0.0564"),
        Decimal("10.00"): Decimal("0.2025"),
        Decimal("20.00"): Decimal("0.64"),
        Decimal("30.00"): Decimal("1.1025"),
        Decimal("40.00"): Decimal("1.44"),
        Decimal("50.00"): Decimal("1.5625"),
        Decimal("60.00"): Decimal("1.44"),
        Decimal("70.00"): Decimal("1.1025"),
        Decimal("80.00"): Decimal("0.64"),
        Decimal("90.00"): Decimal("0.2025"),
        Decimal("99.00"): Decimal("0.0025"),
    },
}


# Create Order payload includes feeRateBps when known (CLOB Create Order docs).
# https://docs.polymarket.com/
def required_fee_rate_bps(token_id: str, metadata: Any) -> int:
    fee_rate_bps = None
    fee_rate_known = False
    if isinstance(metadata, dict):
        fee_rate_bps = metadata.get("fee_rate_bps")
        fee_rate_known = bool(metadata.get("fee_rate_known"))
    else:
        fee_rate_bps = getattr(metadata, "fee_rate_bps", None)
        fee_rate_known = bool(getattr(metadata, "fee_rate_known", False))
    if fee_rate_known and fee_rate_bps is not None:
        return int(fee_rate_bps)
    raise UnknownFeeRate(f"fee rate unknown for token_id={token_id}")


def fee_denomination_for_side(side: str) -> str:
    # Maker Rebates docs: buy fees in tokens, sell fees in USDC.
    # https://docs.polymarket.com/
    side_norm = side.strip().lower()
    if side_norm == "buy":
        return "token"
    if side_norm == "sell":
        return "usdc"
    raise ValueError(f"unknown side: {side}")


def estimate_taker_fee(
    shares: Any,
    price: Any,
    side: str,
    token_id: str,
    metadata: Any,
) -> Decimal | None:
    fee_rate_bps = None
    fee_rate_known = False
    if isinstance(metadata, dict):
        fee_rate_bps = metadata.get("fee_rate_bps")
        fee_rate_known = bool(metadata.get("fee_rate_known"))
    else:
        fee_rate_bps = getattr(metadata, "fee_rate_bps", None)
        fee_rate_known = bool(getattr(metadata, "fee_rate_known", False))
    if not fee_rate_known or fee_rate_bps is None:
        return None
    if int(fee_rate_bps) == 0:
        return Decimal("0")

    shares_dec = _as_decimal(shares)
    price_dec = _as_decimal(price)
    if shares_dec is None or price_dec is None:
        return None
    try:
        shares_int = int(shares_dec)
    except Exception:
        return None
    if Decimal(shares_int) != shares_dec:
        return None
    table = FEE_TABLE_BY_SHARES.get(shares_int)
    if table is None:
        return None
    total_price = (price_dec * Decimal(shares_int)).quantize(
        PRICE_TABLE_PRECISION,
        rounding=ROUND_HALF_UP,
    )
    fee = table.get(total_price)
    if fee is None:
        return None
    _ = fee_denomination_for_side(side)
    return fee.quantize(FEE_TABLE_PRECISION, rounding=ROUND_HALF_UP)


def parse_expected_fee_rate_bps_values(raw: str) -> set[int]:
    if not raw:
        return set()
    values: set[int] = set()
    for item in raw.split(","):
        text = item.strip()
        if not text:
            continue
        try:
            values.add(int(text))
        except ValueError:
            continue
    return values


class FeeRegimeMonitor:
    def __init__(
        self,
        *,
        expect_15m_crypto_fee_enabled: bool,
        expect_other_fee_free: bool,
        expect_unknown_fee_free: bool,
        expected_fee_rate_bps_values: set[int],
    ) -> None:
        self._expect_15m_crypto_fee_enabled = expect_15m_crypto_fee_enabled
        self._expect_other_fee_free = expect_other_fee_free
        self._expect_unknown_fee_free = expect_unknown_fee_free
        self._expected_fee_rate_bps_values = expected_fee_rate_bps_values
        self._tripped = False
        self._trip_reason: str | None = None

    def expected_fee_enabled(self, segment: Any) -> bool | None:
        cadence_bucket = getattr(segment, "cadence_bucket", None)
        is_crypto = getattr(segment, "is_crypto", None)
        if cadence_bucket == "15m" and is_crypto is True:
            return True if self._expect_15m_crypto_fee_enabled else None
        if cadence_bucket == "unknown" or is_crypto is None:
            if self._expect_unknown_fee_free:
                return False
            return None
        if self._expect_other_fee_free:
            return False
        return None

    def sample_tokens(
        self,
        token_ids: Iterable[str],
        *,
        run_id: str,
        universe_version: int,
        sample_size: int,
        canary_token_ids: Iterable[str],
    ) -> list[str]:
        seed = f"{run_id}:{universe_version}"
        scored: list[tuple[int, str]] = []
        seen: set[str] = set()
        for token_id in token_ids:
            token_key = str(token_id)
            if token_key in seen:
                continue
            seen.add(token_key)
            digest = hashlib.sha256(f"{seed}:{token_key}".encode("utf-8")).digest()
            score = int.from_bytes(digest[:8], "little", signed=False)
            scored.append((score, token_key))
        scored.sort(key=lambda item: (item[0], item[1]))
        sample = [token_id for _, token_id in scored[:sample_size]]
        for token_id in canary_token_ids:
            token_key = str(token_id)
            if token_key in seen and token_key not in sample:
                sample.append(token_key)
        return sample

    def evaluate(
        self,
        segments_by_token_id: dict[str, Any],
        *,
        token_ids_sampled: Iterable[str],
    ) -> FeeRegimeState:
        token_ids_sampled = list(token_ids_sampled)
        if self._tripped:
            return FeeRegimeState(
                state="TRIPPED",
                reason=self._trip_reason,
                sampled_tokens_count=len(token_ids_sampled),
                unknown_fee_count=0,
                observed_fee_rates=[],
            )
        unknown_fee_count = 0
        observed_fee_rates: list[int] = []
        for token_id in token_ids_sampled:
            segment = segments_by_token_id.get(str(token_id))
            if segment is None:
                continue
            fee_rate_bps = getattr(segment, "fee_rate_bps", None)
            fee_rate_known = bool(getattr(segment, "fee_rate_known", False))
            fee_enabled = getattr(segment, "fee_enabled", None)
            if not fee_rate_known:
                unknown_fee_count += 1
                continue
            if fee_rate_bps is not None and fee_rate_bps > 0:
                observed_fee_rates.append(int(fee_rate_bps))
            expected_enabled = self.expected_fee_enabled(segment)
            if expected_enabled is None:
                continue
            if expected_enabled and fee_rate_bps == 0:
                self._tripped = True
                self._trip_reason = "FEE_EXPECTED_BUT_ZERO"
                break
            if not expected_enabled and fee_enabled:
                self._tripped = True
                self._trip_reason = "FEE_UNEXPECTED_ON_FREE_SEGMENT"
                break
        if not self._tripped and self._expected_fee_rate_bps_values:
            for value in observed_fee_rates:
                if value not in self._expected_fee_rate_bps_values:
                    self._tripped = True
                    self._trip_reason = "FEE_RATE_BPS_UNEXPECTED_VALUE"
                    break
        return FeeRegimeState(
            state="TRIPPED" if self._tripped else "OK",
            reason=self._trip_reason,
            sampled_tokens_count=len(token_ids_sampled),
            unknown_fee_count=unknown_fee_count,
            observed_fee_rates=observed_fee_rates,
        )
