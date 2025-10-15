from datetime import date, datetime
from pathlib import Path

import pytest

from yf_parqed.partition_path_builder import PartitionPathBuilder


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
        "data/us/yahoo/stocks_1d/ticker=AAPL/year=2025/month=03/day=09/data.parquet"
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
    assert "day=05" in str(result)


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
        "de/xetra/stocks_1d/ticker=DBK/year=2024/month=12/day=31/data.parquet"
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
