import pm_data.cli as cli


def test_discover_active_binary(monkeypatch, capsys):
    markets = [
        {
            "id": "m1",
            "slug": "s1",
            "question": "q1",
            "active": True,
            "enableOrderBook": True,
            "clobTokenIds": ["t1", "t2"],
        },
        {
            "id": "m2",
            "slug": "s2",
            "question": "q2",
            "active": True,
            "clobTokenIds": ["t3"],
        },
        {
            "id": "m3",
            "slug": "s3",
            "question": "q3",
            "active": False,
            "clobTokenIds": ["t4", "t5"],
        },
        {
            "id": "m4",
            "slug": "s4",
            "question": "q4",
            "active": True,
            "enableOrderBook": False,
            "clobTokenIds": ["t6", "t7"],
        },
    ]
    monkeypatch.setattr(cli, "fetch_markets", lambda *args, **kwargs: markets)
    exit_code = cli.main(["discover"])
    assert exit_code == 0
    output = capsys.readouterr().out.strip().splitlines()
    assert output == ["m1\tuniverse\ts1\tq1"]
