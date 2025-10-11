from __future__ import annotations

import json
from datetime import datetime


from yf_parqed.config_service import ConfigService


def test_default_base_path_uses_cwd(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    service = ConfigService()
    assert service.base_path == tmp_path
    assert service.tickers_path == tmp_path / "tickers.json"
    assert service.intervals_path == tmp_path / "intervals.json"


def test_set_working_path_updates_paths(tmp_path):
    service = ConfigService()
    new_path = service.set_working_path(tmp_path)
    assert new_path == tmp_path
    assert service.base_path == tmp_path
    assert service.tickers_path == tmp_path / "tickers.json"
    assert service.intervals_path == tmp_path / "intervals.json"


def test_load_intervals_handles_missing_file(tmp_path):
    service = ConfigService(tmp_path)
    assert service.load_intervals() == []


def test_load_intervals_handles_invalid_json(tmp_path, caplog):
    intervals_path = tmp_path / "intervals.json"
    intervals_path.write_text("{not-json]")
    service = ConfigService(tmp_path)
    with caplog.at_level("WARNING"):
        result = service.load_intervals()
    assert result == []


def test_save_intervals_persists_content(tmp_path):
    service = ConfigService(tmp_path)
    saved = service.save_intervals(["1d", "1h"])
    assert saved == ["1d", "1h"]
    assert json.loads(service.intervals_path.read_text()) == ["1d", "1h"]


def test_load_tickers_handles_missing_file(tmp_path):
    service = ConfigService(tmp_path)
    assert service.load_tickers() == {}


def test_load_tickers_handles_invalid_json(tmp_path, caplog):
    tickers_path = tmp_path / "tickers.json"
    tickers_path.write_text("[{]")
    service = ConfigService(tmp_path)
    with caplog.at_level("WARNING"):
        result = service.load_tickers()
    assert result == {}


def test_save_tickers_persists_content(tmp_path):
    service = ConfigService(tmp_path)
    payload = {"AAPL": {"ticker": "AAPL"}}
    service.save_tickers(payload)
    assert json.loads(service.tickers_path.read_text()) == payload


def test_configure_limits_updates_state():
    service = ConfigService()
    limits = service.configure_limits(5, 10)
    assert limits == (5, 10)
    assert service.get_limits() == (5, 10)


def test_configure_limits_defaults_exposed():
    service = ConfigService()
    assert service.get_limits() == (3, 2)


def test_format_date_defaults_to_get_now(monkeypatch):
    service = ConfigService()
    fake_now = datetime(2024, 1, 15)
    monkeypatch.setattr(service, "get_now", lambda: fake_now)
    assert service.format_date() == "2024-01-15"


def test_format_date_uses_explicit_value():
    service = ConfigService()
    explicit = datetime(2023, 12, 31)
    assert service.format_date(explicit) == "2023-12-31"
