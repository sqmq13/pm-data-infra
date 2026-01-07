import asyncio
import threading
import time

import pytest

from pm_arb.fees import FeeRateClient


class FakeClock:
    def __init__(self) -> None:
        self._ns = 0

    def monotonic_ns(self) -> int:
        return self._ns

    def wall_ns_utc(self) -> int:
        return self._ns

    def advance(self, ns: int) -> None:
        self._ns += ns


@pytest.mark.asyncio
async def test_fee_rate_client_fee_enabled_and_fee_free():
    calls = []

    def fetcher(token_id: str) -> int:
        calls.append(token_id)
        return 25 if token_id == "t1" else 0

    clock = FakeClock()
    client = FeeRateClient(
        base_url="https://clob.polymarket.com",
        timeout_seconds=1.0,
        cache_ttl_seconds=10.0,
        max_in_flight=2,
        fetcher=fetcher,
        monotonic_ns=clock.monotonic_ns,
        wall_ns_utc=clock.wall_ns_utc,
    )
    enabled = await client.get_fee_rate("t1")
    free = await client.get_fee_rate("t0")
    assert enabled.fee_rate_bps == 25
    assert enabled.fee_enabled is True
    assert free.fee_rate_bps == 0
    assert free.fee_enabled is False
    assert calls == ["t1", "t0"]


@pytest.mark.asyncio
async def test_fee_rate_client_cache_hit_avoids_refetch():
    call_count = {"count": 0}

    def fetcher(_token_id: str) -> int:
        call_count["count"] += 1
        return 5

    clock = FakeClock()
    client = FeeRateClient(
        base_url="https://clob.polymarket.com",
        timeout_seconds=1.0,
        cache_ttl_seconds=10.0,
        max_in_flight=2,
        fetcher=fetcher,
        monotonic_ns=clock.monotonic_ns,
        wall_ns_utc=clock.wall_ns_utc,
    )
    await client.get_fee_rate("t1")
    await client.get_fee_rate("t1")
    assert call_count["count"] == 1
    stats = client.stats_snapshot()
    assert stats["cache_hits"] >= 1


@pytest.mark.asyncio
async def test_fee_rate_client_ttl_expiry_refetch():
    call_count = {"count": 0}

    def fetcher(_token_id: str) -> int:
        call_count["count"] += 1
        return 5

    clock = FakeClock()
    client = FeeRateClient(
        base_url="https://clob.polymarket.com",
        timeout_seconds=1.0,
        cache_ttl_seconds=1.0,
        max_in_flight=2,
        fetcher=fetcher,
        monotonic_ns=clock.monotonic_ns,
        wall_ns_utc=clock.wall_ns_utc,
    )
    await client.get_fee_rate("t1")
    clock.advance(int(2 * 1_000_000_000))
    await client.get_fee_rate("t1")
    assert call_count["count"] == 2


@pytest.mark.asyncio
async def test_fee_rate_client_failure_marks_unknown():
    def fetcher(_token_id: str) -> int:
        raise RuntimeError("boom")

    clock = FakeClock()
    client = FeeRateClient(
        base_url="https://clob.polymarket.com",
        timeout_seconds=0.1,
        cache_ttl_seconds=1.0,
        max_in_flight=1,
        fetcher=fetcher,
        monotonic_ns=clock.monotonic_ns,
        wall_ns_utc=clock.wall_ns_utc,
    )
    result = await client.get_fee_rate("t1")
    assert result.fee_rate_known is False
    assert result.fee_rate_bps is None
    assert result.fee_enabled is None


@pytest.mark.asyncio
async def test_fee_rate_client_bounded_concurrency():
    lock = threading.Lock()
    stats = {"in_flight": 0, "max_seen": 0}

    def fetcher(_token_id: str) -> int:
        with lock:
            stats["in_flight"] += 1
            stats["max_seen"] = max(stats["max_seen"], stats["in_flight"])
        time.sleep(0.01)
        with lock:
            stats["in_flight"] -= 1
        return 0

    client = FeeRateClient(
        base_url="https://clob.polymarket.com",
        timeout_seconds=1.0,
        cache_ttl_seconds=1.0,
        max_in_flight=2,
        fetcher=fetcher,
    )
    tokens = [f"t{i}" for i in range(10)]
    await client.prefetch_fee_rates(tokens, timeout_seconds=1.0, max_tokens=10)
    assert stats["max_seen"] <= 2


@pytest.mark.asyncio
async def test_fee_rate_client_inflight_dedup_on_error():
    gate = threading.Event()
    calls = {"count": 0}

    def fetcher(_token_id: str) -> int:
        calls["count"] += 1
        gate.wait(0.05)
        raise RuntimeError("boom")

    client = FeeRateClient(
        base_url="https://clob.polymarket.com",
        timeout_seconds=1.0,
        cache_ttl_seconds=1.0,
        max_in_flight=1,
        fetcher=fetcher,
    )
    task1 = asyncio.create_task(client.get_fee_rate("t1"))
    await asyncio.sleep(0)
    task2 = asyncio.create_task(client.get_fee_rate("t1"))
    gate.set()
    result1, result2 = await asyncio.gather(task1, task2)
    assert calls["count"] == 1
    assert result1.fee_rate_known is False
    assert result2.fee_rate_known is False
