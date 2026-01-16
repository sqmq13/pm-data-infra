from pathlib import Path


def test_single_lockfile_source_of_truth() -> None:
    assert Path("uv.lock").exists(), "expected uv.lock to be the source of truth"
    assert not Path("requirements.lock").exists(), "requirements.lock is deprecated; use uv.lock"

