"""Tests for XetraParser class."""

import json
import pytest
import pandas as pd
from yf_parqed.xetra_parser import XetraParser


class TestXetraParser:
    """Test suite for XetraParser JSON→DataFrame conversion."""

    @pytest.fixture
    def parser(self):
        """Create parser instance for tests."""
        return XetraParser()

    @pytest.fixture
    def sample_trade(self):
        """Single trade record matching Deutsche Börse schema."""
        return {
            "messageId": "posttrade",
            "sourceName": "ETR",
            "isin": "DE0007100000",
            "currency": "EUR",
            "tickActionIndicator": "I",
            "instrumentIdCode": "I",
            "mmtMarketMechanism": "8",
            "mmtTradingMode": "2",
            "mmtNegotTransPretrdWaivInd": "-",
            "mmtModificationInd": "-",
            "mmtBenchmarkRefprcInd": "-",
            "mmtPubModeDefReason": "-",
            "mmtAlgoInd": "H",
            "quotationType": 1,
            "lastQty": 159.00,
            "lastTrade": 56.20,
            "lastTradeTime": "2025-10-31T13:54:00.042457058Z",
            "distributionDateTime": "2025-10-31T13:54:00.052903000Z",
            "tickId": 33976320,
            "instrumentId": "DE0007100000",
            "transIdCode": "1000000000000025050760176191884004245705800000006636",
            "executionVenueId": "XETA",
        }

    @pytest.fixture
    def sample_json_single(self, sample_trade):
        """JSONL format with single trade."""
        return json.dumps(sample_trade)

    @pytest.fixture
    def sample_json_multiple(self, sample_trade):
        """JSONL format with multiple trades."""
        trade1 = sample_trade.copy()
        trade2 = sample_trade.copy()
        trade2["isin"] = "DE000A3H2200"
        trade2["lastTrade"] = 48.04
        trade2["lastQty"] = 3.00
        trade2["tickId"] = 49699840

        return json.dumps(trade1) + "\n" + json.dumps(trade2)

    def test_parse_single_trade(self, parser, sample_json_single):
        """Test parsing a single trade record."""
        df = parser.parse(sample_json_single)

        assert len(df) == 1
        assert df.loc[0, "isin"] == "DE0007100000"
        assert df.loc[0, "price"] == 56.20
        assert df.loc[0, "volume"] == 159.00
        assert df.loc[0, "currency"] == "EUR"

    def test_parse_multiple_trades(self, parser, sample_json_multiple):
        """Test parsing multiple trade records."""
        df = parser.parse(sample_json_multiple)

        assert len(df) == 2
        assert df.loc[0, "isin"] == "DE0007100000"
        assert df.loc[1, "isin"] == "DE000A3H2200"

    def test_column_renaming(self, parser, sample_json_single):
        """Test that JSON fields are renamed to DataFrame columns."""
        df = parser.parse(sample_json_single)

        # Check renamed columns exist
        assert "price" in df.columns  # was lastTrade
        assert "volume" in df.columns  # was lastQty
        assert "trade_time" in df.columns  # was lastTradeTime
        assert "venue" in df.columns  # was executionVenueId
        assert "trans_id" in df.columns  # was transIdCode

        # Check original names don't exist
        assert "lastTrade" not in df.columns
        assert "lastQty" not in df.columns

    def test_timestamp_conversion(self, parser, sample_json_single):
        """Test nanosecond ISO 8601 timestamp conversion."""
        df = parser.parse(sample_json_single)

        # Check trade_time is datetime
        assert pd.api.types.is_datetime64_any_dtype(df["trade_time"])

        # Check parsed value
        expected = pd.Timestamp("2025-10-31 13:54:00.042457058")
        assert df.loc[0, "trade_time"] == expected

        # Check timezone was removed
        assert df["trade_time"].dt.tz is None

    def test_distribution_time_conversion(self, parser, sample_json_single):
        """Test distribution timestamp conversion."""
        df = parser.parse(sample_json_single)

        assert "distribution_time" in df.columns
        assert pd.api.types.is_datetime64_any_dtype(df["distribution_time"])

        expected = pd.Timestamp("2025-10-31 13:54:00.052903000")
        assert df.loc[0, "distribution_time"] == expected

    def test_data_type_normalization(self, parser, sample_json_single):
        """Test data types are normalized correctly."""
        df = parser.parse(sample_json_single)

        # Float columns
        assert df["price"].dtype == "float64"
        assert df["volume"].dtype == "float64"

        # Integer column (nullable Int64 allowed)
        assert str(df["tick_id"].dtype) in ["int64", "Int64"]

        # String columns
        assert df["isin"].dtype == "object"
        assert df["currency"].dtype == "object"
        assert df["venue"].dtype == "object"

    def test_algo_indicator_conversion(self, parser, sample_json_single):
        """Test algo indicator converted to boolean."""
        df = parser.parse(sample_json_single)

        assert "algo_indicator" in df.columns
        assert df["algo_indicator"].dtype == "bool"
        assert df.loc[0, "algo_indicator"]  # "H" → True

    def test_algo_indicator_false(self, parser, sample_trade):
        """Test algo indicator 'False' case."""
        sample_trade["mmtAlgoInd"] = "-"
        json_str = json.dumps(sample_trade)

        df = parser.parse(json_str)

        assert not df.loc[0, "algo_indicator"]  # "-" → False

    def test_parse_empty_json(self, parser):
        """Test handling of empty JSON string."""
        df = parser.parse("")

        assert len(df) == 0
        assert isinstance(df, pd.DataFrame)
        # Check essential columns exist in empty DataFrame
        assert "isin" in df.columns
        assert "price" in df.columns

    def test_parse_whitespace_only(self, parser):
        """Test handling of whitespace-only input."""
        df = parser.parse("   \n\n   ")

        assert len(df) == 0
        assert isinstance(df, pd.DataFrame)

    def test_invalid_json_syntax(self, parser):
        """Test error handling for malformed JSON."""
        invalid_json = '{"isin": "DE0007100000", "price":'  # Missing closing

        with pytest.raises(json.JSONDecodeError):
            parser.parse(invalid_json)

    def test_missing_required_field_isin(self, parser, sample_trade):
        """Test validation fails when required field missing."""
        del sample_trade["isin"]
        json_str = json.dumps(sample_trade)

        with pytest.raises(ValueError, match="Missing required fields"):
            parser.parse(json_str)

    def test_missing_required_field_price(self, parser, sample_trade):
        """Test validation fails without price (lastTrade)."""
        del sample_trade["lastTrade"]
        json_str = json.dumps(sample_trade)

        with pytest.raises(ValueError, match="Missing required fields"):
            parser.parse(json_str)

    def test_missing_required_field_volume(self, parser, sample_trade):
        """Test validation fails without volume (lastQty)."""
        del sample_trade["lastQty"]
        json_str = json.dumps(sample_trade)

        with pytest.raises(ValueError, match="Missing required fields"):
            parser.parse(json_str)

    def test_validate_schema_success(self, parser, sample_json_single):
        """Test schema validation passes for valid DataFrame."""
        df = parser.parse(sample_json_single)

        # Should not raise
        assert parser.validate_schema(df) is True

    def test_validate_schema_missing_column(self, parser, sample_json_single):
        """Test schema validation fails with missing required column."""
        df = parser.parse(sample_json_single)
        df = df.drop(columns=["isin"])

        with pytest.raises(ValueError, match="Schema validation failed"):
            parser.validate_schema(df)

    def test_validate_schema_wrong_dtype(self, parser, sample_json_single):
        """Test schema validation fails with incorrect data type."""
        df = parser.parse(sample_json_single)
        # Change price to string
        df["price"] = df["price"].astype(str)

        with pytest.raises(ValueError, match="Schema validation failed"):
            parser.validate_schema(df)

    def test_all_23_fields_present(self, parser, sample_json_single):
        """Test that all 23 fields from sample data are parsed."""
        df = parser.parse(sample_json_single)

        # Verify we have all expected columns from FIELD_MAPPING
        expected_columns = set(XetraParser.FIELD_MAPPING.values())
        actual_columns = set(df.columns)

        assert actual_columns == expected_columns

    def test_parse_real_sample_file(self, parser):
        """Test parsing actual Deutsche Börse sample data."""
        # First 3 lines from DETR-posttrade-2025-10-31T13_54.json
        real_sample = """{"messageId":"posttrade","sourceName":"ETR","isin":"DE0007100000","currency":"EUR","tickActionIndicator":"I","instrumentIdCode":"I","mmtMarketMechanism":"8","mmtTradingMode":"2","mmtNegotTransPretrdWaivInd":"-","mmtModificationInd":"-","mmtBenchmarkRefprcInd":"-","mmtPubModeDefReason":"-","mmtAlgoInd":"H","quotationType":1,"lastQty":159.00,"lastTrade":56.20,"lastTradeTime":"2025-10-31T13:54:00.042457058Z","distributionDateTime":"2025-10-31T13:54:00.052903000Z","tickId":33976320,"instrumentId":"DE0007100000","transIdCode":"1000000000000025050760176191884004245705800000006636","executionVenueId":"XETA"}
{"messageId":"posttrade","sourceName":"ETR","isin":"DE000A3H2200","currency":"EUR","tickActionIndicator":"I","instrumentIdCode":"I","mmtMarketMechanism":"8","mmtTradingMode":"2","mmtNegotTransPretrdWaivInd":"-","mmtModificationInd":"-","mmtBenchmarkRefprcInd":"-","mmtPubModeDefReason":"-","mmtAlgoInd":"H","quotationType":1,"lastQty":3.00,"lastTrade":48.04,"lastTradeTime":"2025-10-31T13:54:00.052133524Z","distributionDateTime":"2025-10-31T13:54:00.053188000Z","tickId":49699840,"instrumentId":"DE000A3H2200","transIdCode":"1000000000000059069030176191884005213352400000009707","executionVenueId":"XETA"}
{"messageId":"posttrade","sourceName":"ETR","isin":"DE000SHA0100","currency":"EUR","tickActionIndicator":"I","instrumentIdCode":"I","mmtMarketMechanism":"8","mmtTradingMode":"2","mmtNegotTransPretrdWaivInd":"-","mmtModificationInd":"-","mmtBenchmarkRefprcInd":"-","mmtPubModeDefReason":"-","mmtAlgoInd":"H","quotationType":1,"lastQty":40.00,"lastTrade":7.06,"lastTradeTime":"2025-10-31T13:54:00.284365566Z","distributionDateTime":"2025-10-31T13:54:00.292958000Z","tickId":49704960,"instrumentId":"DE000SHA0100","transIdCode":"1000000000000137135710176191884028436556600000009708","executionVenueId":"XETA"}"""

        df = parser.parse(real_sample)

        assert len(df) == 3
        assert df["isin"].tolist() == [
            "DE0007100000",
            "DE000A3H2200",
            "DE000SHA0100",
        ]
        assert df["price"].tolist() == [56.20, 48.04, 7.06]
        assert df["volume"].tolist() == [159.00, 3.00, 40.00]

    def test_parse_with_newlines_between_records(self, parser, sample_trade):
        """Test parsing handles extra newlines gracefully."""
        trade1 = json.dumps(sample_trade)
        trade2 = json.dumps(sample_trade)

        json_with_newlines = f"{trade1}\n\n\n{trade2}\n\n"

        df = parser.parse(json_with_newlines)

        assert len(df) == 2  # Should skip empty lines

    def test_field_mapping_completeness(self, parser):
        """Test FIELD_MAPPING covers all known Deutsche Börse fields."""
        expected_fields = [
            "messageId",
            "sourceName",
            "isin",
            "instrumentId",
            "transIdCode",
            "tickId",
            "lastTrade",
            "lastQty",
            "currency",
            "quotationType",
            "lastTradeTime",
            "distributionDateTime",
            "executionVenueId",
            "tickActionIndicator",
            "instrumentIdCode",
            "mmtMarketMechanism",
            "mmtTradingMode",
            "mmtNegotTransPretrdWaivInd",
            "mmtModificationInd",
            "mmtBenchmarkRefprcInd",
            "mmtPubModeDefReason",
            "mmtAlgoInd",
        ]

        for field in expected_fields:
            assert field in XetraParser.FIELD_MAPPING, f"Missing mapping for {field}"

    def test_required_fields_definition(self, parser):
        """Test REQUIRED_FIELDS contains critical trade identifiers."""
        required = XetraParser.REQUIRED_FIELDS

        assert "isin" in required
        assert "lastTrade" in required  # price
        assert "lastQty" in required  # volume
        assert "currency" in required
        assert "lastTradeTime" in required
        assert "transIdCode" in required  # unique ID
        assert "tickId" in required  # sequencing

    def test_empty_dataframe_schema(self, parser):
        """Test empty DataFrame has correct schema."""
        df = parser._create_empty_dataframe()

        assert len(df) == 0
        # Check essential columns exist
        assert "isin" in df.columns
        assert "price" in df.columns
        assert "volume" in df.columns
        assert "trade_time" in df.columns

        # Check data types are set even when empty
        assert pd.api.types.is_datetime64_any_dtype(df["trade_time"])
