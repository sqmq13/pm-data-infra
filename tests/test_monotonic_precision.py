from math import gcd

from pm_data.capture import monotonic_ns
from pm_data.capture_format import append_record


def test_monotonic_ns_not_ms_quantized(tmp_path):
    frames_path = tmp_path / "shard_00.frames"
    idx_path = tmp_path / "shard_00.idx"
    stamps: list[int] = []
    payload = b"{}"
    with frames_path.open("ab") as frames_fh, idx_path.open("ab") as idx_fh:
        for _ in range(1000):
            stamp = monotonic_ns()
            stamps.append(stamp)
            append_record(
                frames_fh,
                idx_fh,
                payload,
                stamp,
                stamp + 1,
                schema_version=2,
            )
    assert any(stamp % 1_000_000 != 0 for stamp in stamps)
    deltas = [later - earlier for earlier, later in zip(stamps, stamps[1:]) if later > earlier]
    assert deltas
    delta_gcd = deltas[0]
    for delta in deltas[1:]:
        delta_gcd = gcd(delta_gcd, delta)
    assert delta_gcd < 1_000_000
