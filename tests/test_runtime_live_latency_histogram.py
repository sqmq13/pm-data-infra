from pm_data.runtime.latency import LatencyHistogram


def test_latency_histogram_quantiles_bucketed() -> None:
    hist = LatencyHistogram()
    for _ in range(90):
        hist.observe_us(100)
    for _ in range(9):
        hist.observe_us(9_900)
    hist.observe_us(150_000)

    summary = hist.summary()
    assert summary.count == 100
    assert summary.p50_us >= 100
    assert summary.p90_us >= 100
    assert summary.p99_us >= 9_900
    assert summary.max_us >= 150_000
