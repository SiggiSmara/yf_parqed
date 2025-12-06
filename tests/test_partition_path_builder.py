from datetime import date, datetime
from pathlib import Path

import pytest

from yf_parqed.common.partition_path_builder import PartitionPathBuilder


@pytest.fixture(scope="module")
def builder() -> PartitionPathBuilder:
    return PartitionPathBuilder(root=Path("data"))


def test_builds_partition_path_with_all_metadata(builder: PartitionPathBuilder) -> None:
    ts = datetime(2025, 3, 9, 15, 30, 0)
    result = builder.build(
        market="us",
        source="yahoo",
        dataset="stocks",
        interval="1d",
        ticker="AAPL",
        timestamp=ts,
    )
    expected = Path(
        "data/us/yahoo/stocks_1d/ticker=AAPL/year=2025/month=03/data.parquet"
    )
    assert result == expected


def test_zero_pads_month_and_day(builder: PartitionPathBuilder) -> None:
    ts = datetime(2025, 1, 5, 8, 0, 0)
    result = builder.build(
        market="us",
        source="yahoo",
        dataset="stocks",
        interval="1h",
        ticker="MSFT",
        timestamp=ts,
    )
    assert "month=01" in str(result)
    # assert "day=05" in str(result)


def test_accepts_date_instances(builder: PartitionPathBuilder) -> None:
    ts = date(2024, 12, 31)
    result = builder.build(
        market="de",
        source="xetra",
        dataset="stocks",
        interval="1d",
        ticker="DBK",
        timestamp=ts,
    )
    expected_suffix = Path(
        "de/xetra/stocks_1d/ticker=DBK/year=2024/month=12/data.parquet"
    )
    assert str(result).endswith(str(expected_suffix))


def test_falls_back_to_legacy_path_when_missing_metadata(
    builder: PartitionPathBuilder,
) -> None:
    ts = datetime(2025, 7, 14, 0, 0, 0)
    result = builder.build(
        market=None,
        source=None,
        dataset="stocks",
        interval="1d",
        ticker="AAPL",
        timestamp=ts,
    )
    assert result == Path("data/stocks_1d/AAPL.parquet")


def test_requires_ticker_and_interval(builder: PartitionPathBuilder) -> None:
    ts = datetime(2025, 7, 14)
    with pytest.raises(ValueError):
        builder.build(
            market="us",
            source="yahoo",
            dataset="stocks",
            interval="",
            ticker="AAPL",
            timestamp=ts,
        )
    with pytest.raises(ValueError):
        builder.build(
            market="us",
            source="yahoo",
            dataset="stocks",
            interval="1d",
            ticker="",
            timestamp=ts,
        )


def test_ticker_root_returns_expected_path(builder: PartitionPathBuilder) -> None:
    result = builder.ticker_root(
        market="us",
        source="yahoo",
        dataset="stocks",
        interval="1d",
        ticker="AAPL",
    )
    assert result == Path("data/us/yahoo/stocks_1d/ticker=AAPL")


def test_invalid_timestamp_type_raises_typeerror(builder: PartitionPathBuilder) -> None:
    """Test that passing invalid timestamp type raises TypeError."""
    with pytest.raises(
        TypeError, match="timestamp must be a date or datetime instance"
    ):
        builder.build(
            market="us",
            source="yahoo",
            dataset="stocks",
            interval="1d",
            ticker="AAPL",
            timestamp="2025-01-01",  # String instead of date/datetime
        )


def test_ticker_root_requires_interval(builder: PartitionPathBuilder) -> None:
    """Test that ticker_root raises ValueError when interval is missing."""
    with pytest.raises(ValueError, match="interval is required"):
        builder.ticker_root(
            market="us",
            source="yahoo",
            dataset="stocks",
            interval="",
            ticker="AAPL",
        )


def test_ticker_root_requires_ticker(builder: PartitionPathBuilder) -> None:
    """Test that ticker_root raises ValueError when ticker is missing."""
    with pytest.raises(ValueError, match="ticker is required"):
        builder.ticker_root(
            market="us",
            source="yahoo",
            dataset="stocks",
            interval="1d",
            ticker="",
        )


def test_ticker_root_requires_market_and_source(builder: PartitionPathBuilder) -> None:
    """Test that ticker_root raises ValueError when market or source is missing."""
    with pytest.raises(
        ValueError, match="market and source are required for partitioned paths"
    ):
        builder.ticker_root(
            market=None,
            source="yahoo",
            dataset="stocks",
            interval="1d",
            ticker="AAPL",
        )

    with pytest.raises(
        ValueError, match="market and source are required for partitioned paths"
    ):
        builder.ticker_root(
            market="us",
            source=None,
            dataset="stocks",
            interval="1d",
            ticker="AAPL",
        )
