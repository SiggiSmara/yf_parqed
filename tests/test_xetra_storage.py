import pandas as pd
import pytest
import warnings
from src.yf_parqed.partitioned_storage_backend import PartitionedStorageBackend
from src.yf_parqed.partition_path_builder import PartitionPathBuilder
from src.yf_parqed.xetra_parser import XetraParser


def make_trades_df(rows=5):
    return pd.DataFrame(
        {
            "trade_id": range(rows),
            "price": [100.0 + i for i in range(rows)],
            "volume": [10 + i for i in range(rows)],
            "timestamp": pd.date_range("2025-11-01", periods=rows, freq="min"),
        }
    )


def test_save_xetra_trades(tmp_path):
    backend = PartitionedStorageBackend(
        empty_frame_factory=lambda: pd.DataFrame(),
        normalizer=lambda df: df,
        column_provider=lambda: ["trade_id", "price", "volume", "timestamp"],
        path_builder=PartitionPathBuilder(root=tmp_path),
    )
    trades_df = make_trades_df(3)
    venue = "FRANKFURT"
    trade_date = "2025-11-01"
    market = "xetra"
    source = "delayed"
    backend.save_xetra_trades(trades_df, venue, trade_date, market, source)
    out_path = (
        tmp_path
        / market
        / source
        / "trades"
        / f"venue={venue}"
        / "year=2025"
        / "month=11"
        / "day=01"
        / "trades.parquet"
    )
    assert out_path.exists()
    df = pd.read_parquet(out_path)
    assert len(df) == 3
    assert set(df.columns) >= {"trade_id", "price", "volume", "timestamp"}


def test_atomic_write_and_corruption(tmp_path):
    backend = PartitionedStorageBackend(
        empty_frame_factory=lambda: pd.DataFrame(),
        normalizer=lambda df: df,
        column_provider=lambda: ["trade_id", "price", "volume", "timestamp"],
        path_builder=PartitionPathBuilder(root=tmp_path),
    )
    trades_df = make_trades_df(2)
    venue = "XETRA"
    trade_date = "2025-11-02"
    market = "xetra"
    source = "delayed"
    out_path = (
        tmp_path
        / market
        / source
        / "trades"
        / f"venue={venue}"
        / "year=2025"
        / "month=11"
        / "day=02"
        / "trades.parquet"
    )
    # Simulate corrupt file
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        f.write(b"corrupt")
    # Should overwrite
    backend.save_xetra_trades(trades_df, venue, trade_date, market, source)
    df = pd.read_parquet(out_path)
    assert len(df) == 2


def test_multiple_venues_and_dates(tmp_path):
    backend = PartitionedStorageBackend(
        empty_frame_factory=lambda: pd.DataFrame(),
        normalizer=lambda df: df,
        column_provider=lambda: ["trade_id", "price", "volume", "timestamp"],
        path_builder=PartitionPathBuilder(root=tmp_path),
    )
    venues = ["FRANKFURT", "XETRA"]
    dates = ["2025-11-01", "2025-11-02"]
    for venue in venues:
        for d in dates:
            df = make_trades_df(1)
            backend.save_xetra_trades(df, venue, d)
            out_path = (
                tmp_path
                / "xetra"
                / "delayed"
                / "trades"
                / f"venue={venue}"
                / "year=2025"
                / "month=11"
                / f"day={d[-2:]}"
                / "trades.parquet"
            )
            assert out_path.exists()
            read_df = pd.read_parquet(out_path)
            assert len(read_df) == 1


def test_schema_completeness_before_storage():
    """
    Test that parsed Xetra data always has all 22 expected columns before storage.

    This ensures Parquet schema stability by verifying that _ensure_complete_schema()
    guarantees all FIELD_MAPPING columns are present, even when API omits optional fields.
    """
    parser = XetraParser()

    # Test with minimal required fields (simulating API response with omitted optional fields)
    minimal_json = """
{"messageId":"MSG001","sourceName":"DETR","isin":"DE0007100000","instrumentId":"BMW","transIdCode":"O","tickId":1001,"lastTrade":56.20,"lastQty":100,"currency":"EUR","lastTradeTime":"2025-11-14T11:30:15.123456789Z","executionVenueId":"XETR"}
{"messageId":"MSG002","sourceName":"DETR","isin":"DE0005140008","instrumentId":"DB","transIdCode":"O","tickId":1002,"lastTrade":9.85,"lastQty":200,"currency":"EUR","lastTradeTime":"2025-11-14T11:30:16.987654321Z","executionVenueId":"XETR"}
"""

    df = parser.parse(minimal_json)

    # Verify all 22 columns from FIELD_MAPPING are present
    expected_columns = set(parser.FIELD_MAPPING.values())
    actual_columns = set(df.columns)

    assert (
        len(expected_columns) == 22
    ), f"Expected 22 columns in FIELD_MAPPING, got {len(expected_columns)}"
    assert actual_columns == expected_columns, (
        f"Column mismatch:\n"
        f"  Missing: {expected_columns - actual_columns}\n"
        f"  Extra: {actual_columns - expected_columns}"
    )

    # Verify specific optional columns are present (even if None/NaN)
    optional_columns = [
        "distribution_time",
        "instrument_code",
        "market_mechanism",
        "trading_mode",
        "negotiated_flag",
        "modification_flag",
        "benchmark_flag",
        "pub_deferral",
        "quote_type",
        "algo_indicator",
    ]

    for col in optional_columns:
        assert col in df.columns, f"Optional column '{col}' missing from DataFrame"

    # Verify required columns have actual data
    required_columns = [
        "isin",
        "price",
        "volume",
        "currency",
        "trade_time",
        "venue",
        "trans_id",
        "tick_id",
    ]
    for col in required_columns:
        assert col in df.columns, f"Required column '{col}' missing"
        assert not df[col].isna().all(), f"Required column '{col}' has all NaN values"

    # Verify DataFrame is ready for storage (no schema inconsistencies)
    assert len(df) == 2, "Should have parsed 2 trades"
    assert df.shape[1] == 22, f"Should have exactly 22 columns, got {df.shape[1]}"


def test_schema_stability_across_multiple_parses():
    """
    Test that schema remains stable across multiple parse operations with varying data.

    Ensures that DataFrames can be safely merged/concatenated without schema conflicts.
    """
    parser = XetraParser()

    # Parse 1: Full data with all optional fields
    full_json = """
{"messageId":"MSG001","sourceName":"DETR","isin":"DE0007100000","instrumentId":"BMW","transIdCode":"O","tickId":1001,"lastTrade":56.20,"lastQty":100,"currency":"EUR","quotationType":"P","lastTradeTime":"2025-11-14T11:30:15.123456789Z","distributionDateTime":"2025-11-14T11:30:15.500000000Z","executionVenueId":"XETR","tickActionIndicator":"N","instrumentIdCode":"I","mmtMarketMechanism":"1","mmtTradingMode":"2","mmtNegotTransPretrdWaivInd":"N","mmtModificationInd":"CANC","mmtBenchmarkRefprcInd":"N","mmtPubModeDefReason":"NONE","mmtAlgoInd":"H"}
"""

    # Parse 2: Minimal data with omitted optional fields
    minimal_json = """
{"messageId":"MSG002","sourceName":"DETR","isin":"DE0005140008","instrumentId":"DB","transIdCode":"O","tickId":1002,"lastTrade":9.85,"lastQty":200,"currency":"EUR","lastTradeTime":"2025-11-14T11:30:16.987654321Z","executionVenueId":"XETR"}
"""

    df1 = parser.parse(full_json)
    df2 = parser.parse(minimal_json)

    # Verify both DataFrames have identical column sets
    assert set(df1.columns) == set(df2.columns), (
        f"Schema mismatch between parses:\n"
        f"  df1 only: {set(df1.columns) - set(df2.columns)}\n"
        f"  df2 only: {set(df2.columns) - set(df1.columns)}"
    )

    # Verify both have 22 columns
    assert len(df1.columns) == 22, f"df1 should have 22 columns, got {len(df1.columns)}"
    assert len(df2.columns) == 22, f"df2 should have 22 columns, got {len(df2.columns)}"

    # Verify DataFrames can be safely concatenated (Parquet merge simulation)
    try:
        # Suppress FutureWarning about all-NA columns during concat
        # This is expected behavior when merging DataFrames with optional fields
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", message=".*empty or all-NA entries.*", category=FutureWarning
            )
            merged = pd.concat([df1, df2], ignore_index=True)

        assert len(merged) == 2, "Concatenation should preserve both rows"
        assert len(merged.columns) == 22, "Concatenation should preserve all 22 columns"
    except Exception as e:
        pytest.fail(f"DataFrame concatenation failed (schema instability): {e}")

    # Verify column order is consistent (important for Parquet schema)
    assert list(df1.columns) == list(df2.columns), "Column order should be consistent"
