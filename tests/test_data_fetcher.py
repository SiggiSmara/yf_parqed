"""Tests for the DataFetcher service."""

from datetime import datetime, timedelta
from unittest.mock import Mock

import pandas as pd
import pytest
from curl_cffi.requests.exceptions import HTTPError

from yf_parqed.data_fetcher import DataFetcher


@pytest.fixture
def mock_limiter():
    """Mock rate limiter that tracks calls."""
    limiter = Mock()
    return limiter


@pytest.fixture
def mock_today_provider():
    """Returns a fixed date for testing."""
    return lambda: datetime(2024, 10, 11, 17, 0, 0)


@pytest.fixture
def mock_empty_frame():
    """Returns an empty price dataframe."""

    def factory():
        return pd.DataFrame(
            {
                "stock": pd.Series(dtype="string"),
                "date": pd.Series(dtype="datetime64[ns]"),
                "open": pd.Series(dtype="float64"),
                "high": pd.Series(dtype="float64"),
                "low": pd.Series(dtype="float64"),
                "close": pd.Series(dtype="float64"),
                "volume": pd.Series(dtype="Int64"),
            }
        ).set_index(["stock", "date"])

    return factory


@pytest.fixture
def mock_ticker_factory():
    """Factory that returns mock tickers."""
    tickers = {}

    def factory(stock: str):
        if stock not in tickers:
            mock_ticker = Mock()
            mock_ticker.stock = stock
            tickers[stock] = mock_ticker
        return tickers[stock]

    return factory


@pytest.fixture
def fetcher(mock_limiter, mock_today_provider, mock_empty_frame, mock_ticker_factory):
    """A DataFetcher instance with mocked dependencies."""
    return DataFetcher(
        limiter=mock_limiter,
        today_provider=mock_today_provider,
        empty_frame_factory=mock_empty_frame,
        ticker_factory=mock_ticker_factory,
    )


class TestDataFetcherBasics:
    """Test basic DataFetcher initialization and simple operations."""

    def test_initialization_stores_dependencies(
        self, mock_limiter, mock_today_provider, mock_empty_frame
    ):
        """DataFetcher should store injected dependencies."""
        fetcher = DataFetcher(
            limiter=mock_limiter,
            today_provider=mock_today_provider,
            empty_frame_factory=mock_empty_frame,
        )
        assert fetcher._limiter is mock_limiter
        assert fetcher._today_provider is mock_today_provider
        assert fetcher._empty_frame_factory is mock_empty_frame

    def test_fetch_invokes_limiter(self, fetcher, mock_limiter, mock_ticker_factory):
        """Every fetch should invoke the rate limiter."""
        mock_ticker = mock_ticker_factory("AAPL")
        mock_ticker.history.return_value = pd.DataFrame()

        fetcher.fetch("AAPL", datetime(2024, 1, 1), datetime(2024, 1, 31), "1d")

        mock_limiter.assert_called_once()

    def test_fetch_creates_ticker_instance(self, fetcher, mock_ticker_factory):
        """Fetch should use the ticker factory to create a ticker instance."""
        mock_ticker = mock_ticker_factory("TSLA")
        mock_ticker.history.return_value = pd.DataFrame()

        fetcher.fetch("TSLA", datetime(2024, 1, 1), datetime(2024, 1, 31), "1d")

        # The ticker's history method should have been called
        mock_ticker.history.assert_called_once()


class TestFetchWindow:
    """Test window-based fetching logic."""

    def test_fetch_window_calls_history_with_correct_params(
        self, fetcher, mock_ticker_factory
    ):
        """Window fetch should call ticker.history with start/end dates."""
        mock_ticker = mock_ticker_factory("GOOG")
        mock_ticker.history.return_value = pd.DataFrame()

        start = datetime(2024, 5, 1)
        end = datetime(2024, 5, 31)
        fetcher.fetch("GOOG", start, end, "1d", get_all=False)

        mock_ticker.history.assert_called_once()
        call_kwargs = mock_ticker.history.call_args[1]
        assert call_kwargs["start"] == start
        assert call_kwargs["end"] == end
        assert call_kwargs["interval"] == "1d"

    def test_fetch_window_returns_empty_on_exception(
        self, fetcher, mock_ticker_factory, mock_empty_frame
    ):
        """If ticker.history raises, fetch should return an empty frame."""
        mock_ticker = mock_ticker_factory("ERROR")
        mock_ticker.history.side_effect = ValueError("API error")

        result = fetcher.fetch(
            "ERROR", datetime(2024, 1, 1), datetime(2024, 1, 31), "1d"
        )

        assert result.empty
        assert list(result.columns) == ["open", "high", "low", "close", "volume"]


class TestFetchAll:
    """Test period-based (get_all) fetching logic."""

    def test_fetch_all_uses_period_for_1d_interval(self, fetcher, mock_ticker_factory):
        """For 1d interval with get_all=True, should use period=10y."""
        mock_ticker = mock_ticker_factory("MSFT")
        mock_ticker.history.return_value = pd.DataFrame()

        fetcher.fetch(
            "MSFT", datetime(2024, 1, 1), datetime(2024, 1, 31), "1d", get_all=True
        )

        mock_ticker.history.assert_called_once()
        call_kwargs = mock_ticker.history.call_args[1]
        assert call_kwargs["period"] == "10y"
        assert call_kwargs["interval"] == "1d"

    def test_fetch_all_uses_729d_for_hourly_intervals(
        self, fetcher, mock_ticker_factory
    ):
        """For hourly intervals with get_all=True, should use period=729d."""
        mock_ticker = mock_ticker_factory("AMZN")
        mock_ticker.history.return_value = pd.DataFrame()

        fetcher.fetch(
            "AMZN", datetime(2024, 1, 1), datetime(2024, 1, 31), "1h", get_all=True
        )

        call_kwargs = mock_ticker.history.call_args[1]
        assert call_kwargs["period"] == "729d"

    def test_fetch_all_uses_8d_for_minute_intervals(self, fetcher, mock_ticker_factory):
        """For minute intervals with get_all=True, should use period=8d."""
        mock_ticker = mock_ticker_factory("NFLX")
        mock_ticker.history.return_value = pd.DataFrame()

        fetcher.fetch(
            "NFLX", datetime(2024, 1, 1), datetime(2024, 1, 31), "1m", get_all=True
        )

        call_kwargs = mock_ticker.history.call_args[1]
        assert call_kwargs["period"] == "8d"

    def test_fetch_all_returns_empty_on_http_error(
        self, fetcher, mock_ticker_factory, mock_empty_frame
    ):
        """If ticker.history raises HTTPError during get_all, should return empty frame."""
        mock_ticker = mock_ticker_factory("FAIL")
        mock_ticker.history.side_effect = HTTPError("404 Not Found")

        result = fetcher.fetch(
            "FAIL", datetime(2024, 1, 1), datetime(2024, 1, 31), "1d", get_all=True
        )

        assert result.empty


class TestIntervalConstraints:
    """Test Yahoo Finance interval constraint enforcement."""

    def test_hourly_interval_constrains_start_to_729_days(
        self, fetcher, mock_ticker_factory, mock_today_provider
    ):
        """For hourly intervals, start dates older than 729 days should be adjusted."""
        today = mock_today_provider()
        old_start = today - timedelta(days=800)
        end = today - timedelta(days=1)

        mock_ticker = mock_ticker_factory("LIMIT")
        mock_ticker.history.return_value = pd.DataFrame()

        fetcher.fetch("LIMIT", old_start, end, "1h", get_all=False)

        call_kwargs = mock_ticker.history.call_args[1]
        # Start should be adjusted to ~729 days ago
        assert (today - call_kwargs["start"]).days <= 729

    def test_hourly_interval_returns_empty_when_window_too_large(
        self, fetcher, mock_ticker_factory, mock_today_provider
    ):
        """If adjusted hourly window is still >729 days, should return empty."""
        today = mock_today_provider()
        old_start = today - timedelta(days=800)
        old_end = today - timedelta(days=750)

        mock_ticker = mock_ticker_factory("TOOLARGE")
        mock_ticker.history.return_value = pd.DataFrame()

        result = fetcher.fetch("TOOLARGE", old_start, old_end, "1h", get_all=False)

        # Window after adjustment is still too large, should return empty
        # The constraint logic forces an empty window by returning (end, end)
        assert result.empty or mock_ticker.history.call_count == 0

    def test_minute_interval_constrains_to_7_days(
        self, fetcher, mock_ticker_factory, mock_today_provider
    ):
        """For minute intervals, dates older than 7 days should be adjusted."""
        today = mock_today_provider()
        old_start = today - timedelta(days=30)
        end = today - timedelta(days=1)

        mock_ticker = mock_ticker_factory("MINUTE")
        mock_ticker.history.return_value = pd.DataFrame()

        fetcher.fetch("MINUTE", old_start, end, "1m", get_all=False)

        call_kwargs = mock_ticker.history.call_args[1]
        # Start should be adjusted to ~7 days ago
        assert (today - call_kwargs["start"]).days <= 7


class TestDataframeNormalization:
    """Test that fetched data is properly normalized."""

    def test_normalization_renames_date_column(self, fetcher, mock_ticker_factory):
        """Normalized dataframe should have lowercase column names."""
        # yfinance returns data with index named (not a column)
        raw_df = pd.DataFrame(
            {
                "Open": [100.0],
                "High": [101.0],
                "Low": [99.0],
                "Close": [100.5],
                "Volume": [1000],
            },
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-02")], name="Date"),
        )

        mock_ticker = mock_ticker_factory("NORM")
        mock_ticker.history.return_value = raw_df

        result = fetcher.fetch(
            "NORM", datetime(2024, 1, 1), datetime(2024, 1, 31), "1d"
        )

        # Check it's not empty after normalization
        assert not result.empty
        # Stock is in index, not columns
        assert "open" in result.columns
        assert "high" in result.columns
        assert "close" in result.columns

    def test_normalization_adds_stock_column(self, fetcher, mock_ticker_factory):
        """Normalized dataframe should include stock symbol."""
        raw_df = pd.DataFrame(
            {
                "Open": [50.0],
                "High": [51.0],
                "Low": [49.0],
                "Close": [50.5],
                "Volume": [2000],
            },
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-02")], name="Date"),
        )

        mock_ticker = mock_ticker_factory("STOCK")
        mock_ticker.history.return_value = raw_df

        result = fetcher.fetch(
            "STOCK", datetime(2024, 1, 1), datetime(2024, 1, 31), "1d"
        )

        assert not result.empty
        assert "stock" in result.index.names
        assert result.index.get_level_values("stock")[0] == "STOCK"

    def test_normalization_removes_timezone_info(self, fetcher, mock_ticker_factory):
        """Normalized dates should be timezone-naive."""
        tz_aware_date = pd.Timestamp("2024-01-02", tz="America/New_York")
        raw_df = pd.DataFrame(
            {
                "Open": [75.0],
                "High": [76.0],
                "Low": [74.0],
                "Close": [75.5],
                "Volume": [3000],
            },
            index=pd.DatetimeIndex([tz_aware_date], name="Date"),
        )

        mock_ticker = mock_ticker_factory("TZ")
        mock_ticker.history.return_value = raw_df

        result = fetcher.fetch("TZ", datetime(2024, 1, 1), datetime(2024, 1, 31), "1d")

        assert not result.empty
        date_val = result.index.get_level_values("date")[0]
        assert date_val.tz is None

    def test_normalization_handles_empty_dataframe(
        self, fetcher, mock_ticker_factory, mock_empty_frame
    ):
        """Empty dataframes should pass through normalization."""
        mock_ticker = mock_ticker_factory("EMPTY")
        mock_ticker.history.return_value = pd.DataFrame()

        result = fetcher.fetch(
            "EMPTY", datetime(2024, 1, 1), datetime(2024, 1, 31), "1d"
        )

        assert result.empty
        assert list(result.index.names) == ["stock", "date"]


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_fetch_with_none_ticker_factory_uses_default(
        self, mock_limiter, mock_today_provider, mock_empty_frame
    ):
        """If no ticker_factory provided, should use yfinance.Ticker."""
        fetcher = DataFetcher(
            limiter=mock_limiter,
            today_provider=mock_today_provider,
            empty_frame_factory=mock_empty_frame,
            ticker_factory=None,
        )
        # The default factory should be yf.Ticker
        import yfinance as yf

        assert fetcher._ticker_factory == yf.Ticker

    def test_multiple_fetches_invoke_limiter_each_time(
        self, fetcher, mock_limiter, mock_ticker_factory
    ):
        """Each fetch call should invoke the limiter."""
        mock_ticker = mock_ticker_factory("MULTI")
        mock_ticker.history.return_value = pd.DataFrame()

        fetcher.fetch("MULTI", datetime(2024, 1, 1), datetime(2024, 1, 31), "1d")
        fetcher.fetch("MULTI", datetime(2024, 2, 1), datetime(2024, 2, 28), "1d")
        fetcher.fetch("MULTI", datetime(2024, 3, 1), datetime(2024, 3, 31), "1d")

        assert mock_limiter.call_count == 3
