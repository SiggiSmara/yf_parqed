from datetime import datetime
from pathlib import Path

from yf_parqed.common.config_service import ConfigService
from yf_parqed.yahoo.interval_scheduler import IntervalScheduler
from yf_parqed.yahoo.ticker_registry import TickerRegistry


def make_registry(tmp_path: Path, tickers: dict) -> TickerRegistry:
    config = ConfigService(tmp_path)
    registry = TickerRegistry(config, initial_tickers=tickers)
    return registry


def test_run_filters_active_tickers_and_invokes_processor(tmp_path, monkeypatch):
    tickers = {
        "AAA": {
            "status": "active",
            "intervals": {
                "1h": {
                    "status": "not_found",
                    "last_not_found_date": "2025-01-01",
                    "last_checked": "2025-01-01",
                }
            },
        },
        "BBB": {"status": "not_found", "intervals": {}},
    }
    registry = make_registry(tmp_path, tickers)
    monkeypatch.setattr(registry._config, "get_now", lambda: datetime(2025, 1, 15))

    loader_calls = []
    limiter_calls = []
    processed = []
    progress_events = []

    def loader():
        loader_calls.append(True)

    def limiter():
        limiter_calls.append(True)

    def processor(stock, start_date, end_date, interval):
        processed.append(
            {
                "stock": stock,
                "start": start_date,
                "end": end_date,
                "interval": interval,
            }
        )

    def progress_factory(stocks, description, disable):
        materialized = list(stocks)
        progress_events.append((materialized, description, disable))
        return materialized

    sentinel_today = datetime(2025, 1, 20)

    scheduler = IntervalScheduler(
        registry=registry,
        intervals=lambda: ["1d", "1h"],
        loader=loader,
        limiter=limiter,
        processor=processor,
        today_provider=lambda: sentinel_today,
        progress_factory=progress_factory,
    )

    start = datetime(2024, 1, 1)
    scheduler.run(start_date=start)

    assert len(loader_calls) == 1
    assert len(limiter_calls) == 1
    assert processed == [
        {
            "stock": "AAA",
            "start": start,
            "end": sentinel_today,
            "interval": "1d",
        }
    ]
    assert progress_events == [
        (["AAA"], "Processing stocks for interval:1d", False),
        ([], "Processing stocks for interval:1h", False),
    ]


def test_run_honors_progress_disable_environment(tmp_path, monkeypatch):
    tickers = {"AAA": {"status": "active", "intervals": {}}}
    registry = make_registry(tmp_path, tickers)

    monkeypatch.setenv("YF_PARQED_LOG_LEVEL", "DEBUG")

    recorded_disable = []

    def progress_factory(stocks, description, disable):
        recorded_disable.append(disable)
        return list(stocks)

    scheduler = IntervalScheduler(
        registry=registry,
        intervals=lambda: ["1d"],
        loader=lambda: None,
        limiter=lambda: None,
        processor=lambda stock, start_date, end_date, interval: None,
        today_provider=lambda: datetime(2025, 1, 1),
        progress_factory=progress_factory,
    )

    scheduler.run()

    assert recorded_disable == [True]
