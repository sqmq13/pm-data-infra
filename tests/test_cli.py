import pm_data.cli as cli


def _run_capture(monkeypatch, args):
    calls = {"offline": None, "capture_offline": 0, "capture_online": 0}
    monkeypatch.delenv("PM_DATA_OFFLINE", raising=False)

    def _capture_offline(config, fixtures_dir, run_id=None, **kwargs):
        calls["offline"] = config.offline
        calls["capture_offline"] += 1

        class Dummy:
            run = type("Run", (), {"run_dir": "run"})()

        return Dummy()

    def _capture_online(config, run_id=None):
        calls["offline"] = config.offline
        calls["capture_online"] += 1
        return 0

    monkeypatch.setattr(cli, "run_capture_offline", _capture_offline)
    monkeypatch.setattr(cli, "run_capture_online", _capture_online)
    exit_code = cli.main(["capture", *args])
    return calls, exit_code


def test_offline_flag_const(monkeypatch):
    calls, exit_code = _run_capture(monkeypatch, ["--offline"])
    assert exit_code == 0
    assert calls["offline"] is True
    assert calls["capture_offline"] == 1
    assert calls["capture_online"] == 0


def test_offline_flag_true(monkeypatch):
    calls, exit_code = _run_capture(monkeypatch, ["--offline", "true"])
    assert exit_code == 0
    assert calls["offline"] is True
    assert calls["capture_offline"] == 1
    assert calls["capture_online"] == 0


def test_offline_flag_false(monkeypatch):
    calls, exit_code = _run_capture(monkeypatch, ["--offline", "false"])
    assert exit_code == 0
    assert calls["offline"] is False
    assert calls["capture_online"] == 1
    assert calls["capture_offline"] == 0
