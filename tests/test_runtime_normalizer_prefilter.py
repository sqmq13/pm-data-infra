import json

from pm_data.runtime.normalize import Normalizer
from pm_data.runtime.replay import RawFrame


def test_normalizer_prefilter_skips_non_book_payloads_without_decoding() -> None:
    payload = {"event_type": "trade", "foo": "bar"}
    frame = RawFrame(
        payload=json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8"),
        rx_mono_ns=123,
        rx_wall_ns_utc=0,
        shard_id=0,
        idx_i=0,
    )
    normalizer = Normalizer()
    events = normalizer.normalize(frame)
    assert events == []
    assert normalizer.decode_errors == 0
    assert normalizer.schema_errors == 0
    assert normalizer.prefilter_skipped_frames == 1

