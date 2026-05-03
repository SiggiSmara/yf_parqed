"""Tests for XetraParser class."""

import json
import pytest
import pandas as pd
from yf_parqed.xetra.xetra_parser import XetraParser
from yf_parqed.xetra.exceptions import XetraSchemaUnknownError


class TestXetraParser:
    """Test suite for XetraParser JSON→DataFrame conversion."""

    @pytest.fixture
    def parser(self):
        return XetraParser()

    # ------------------------------------------------------------------
    # Legacy schema (2025-legacy) fixtures
    # ------------------------------------------------------------------

    @pytest.fixture
    def legacy_trade(self):
        """Single trade record in 2025-legacy Deutsche Börse schema."""
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
    def legacy_json_single(self, legacy_trade):
        return json.dumps(legacy_trade)

    @pytest.fixture
    def legacy_json_multiple(self, legacy_trade):
        trade1 = legacy_trade.copy()
        trade2 = legacy_trade.copy()
        trade2["isin"] = "DE000A3H2200"
        trade2["lastTrade"] = 48.04
        trade2["lastQty"] = 3.00
        trade2["tickId"] = 49699840
        return json.dumps(trade1) + "\n" + json.dumps(trade2)

    # ------------------------------------------------------------------
    # 2026-mifid schema fixtures
    # ------------------------------------------------------------------

    @pytest.fixture
    def mifid_trade(self):
        """Single trade record in 2026-mifid Deutsche Börse schema."""
        return {
            "instrumentIdentificationCode": "DE0007100000",
            "price": 56.20,
            "quantity": 159.00,
            "priceCurrency": "EUR",
            "tradingDateAndTime": "2026-03-15T10:30:00.123456789Z",
            "publicationDateAndTime": "2026-03-15T10:30:00.134000000Z",
            "venueOfExecution": "XETA",
            "transactionIdentificationCode": "2000000000000025050760176191884004245705800000006636",
            "tradingSystem": "XETRA",
            "priceNotation": "MONE",
            "venueOfPublication": "XETA",
            "mmtTradingMode": "2",
            "mmtModificationInd": "-",
            "mmtBenchmarkRefprcInd": "-",
            "mmtPubModeDefReason": "-",
            "mmtAlgoInd": "H",
        }

    @pytest.fixture
    def mifid_json_single(self, mifid_trade):
        return json.dumps(mifid_trade)

    @pytest.fixture
    def mifid_json_multiple(self, mifid_trade):
        trade1 = mifid_trade.copy()
        trade2 = mifid_trade.copy()
        trade2["instrumentIdentificationCode"] = "DE000A3H2200"
        trade2["price"] = 48.04
        trade2["quantity"] = 3.00
        return json.dumps(trade1) + "\n" + json.dumps(trade2)

    # ------------------------------------------------------------------
    # Legacy schema: basic parsing
    # ------------------------------------------------------------------

    def test_legacy_parse_single_trade(self, parser, legacy_json_single):
        df = parser.parse(legacy_json_single)

        assert len(df) == 1
        assert df.loc[0, "isin"] == "DE0007100000"
        assert df.loc[0, "price"] == 56.20
        assert df.loc[0, "quantity"] == 159.00
        assert df.loc[0, "price_currency"] == "EUR"
        assert df.loc[0, "schema_version"] == "2025-legacy"

    def test_legacy_parse_multiple_trades(self, parser, legacy_json_multiple):
        df = parser.parse(legacy_json_multiple)

        assert len(df) == 2
        assert df.loc[0, "isin"] == "DE0007100000"
        assert df.loc[1, "isin"] == "DE000A3H2200"

    def test_legacy_column_renaming(self, parser, legacy_json_single):
        df = parser.parse(legacy_json_single)

        assert "price" in df.columns
        assert "quantity" in df.columns
        assert "trading_date_time" in df.columns
        assert "execution_venue" in df.columns
        assert "transaction_id" in df.columns
        assert "price_currency" in df.columns

        assert "lastTrade" not in df.columns
        assert "lastQty" not in df.columns
        assert "lastTradeTime" not in df.columns
        assert "currency" not in df.columns

    def test_legacy_timestamp_conversion(self, parser, legacy_json_single):
        df = parser.parse(legacy_json_single)

        assert pd.api.types.is_datetime64_any_dtype(df["trading_date_time"])
        expected = pd.Timestamp("2025-10-31 13:54:00.042457058")
        assert df.loc[0, "trading_date_time"] == expected
        assert df["trading_date_time"].dt.tz is None

    def test_legacy_distribution_time_conversion(self, parser, legacy_json_single):
        df = parser.parse(legacy_json_single)

        assert "distribution_time" in df.columns
        assert pd.api.types.is_datetime64_any_dtype(df["distribution_time"])
        expected = pd.Timestamp("2025-10-31 13:54:00.052903000")
        assert df.loc[0, "distribution_time"] == expected

    def test_legacy_data_type_normalization(self, parser, legacy_json_single):
        df = parser.parse(legacy_json_single)

        assert df["price"].dtype == "float64"
        assert df["quantity"].dtype == "float64"
        assert df["isin"].dtype == "object"
        assert df["price_currency"].dtype == "object"
        assert df["execution_venue"].dtype == "object"

    def test_legacy_algo_indicator_true(self, parser, legacy_json_single):
        df = parser.parse(legacy_json_single)

        assert "algo_indicator" in df.columns
        assert df["algo_indicator"].dtype == "bool"
        assert df.loc[0, "algo_indicator"]  # "H" → True

    def test_legacy_algo_indicator_false(self, parser, legacy_trade):
        legacy_trade["mmtAlgoInd"] = "-"
        df = parser.parse(json.dumps(legacy_trade))

        assert not df.loc[0, "algo_indicator"]

    # ------------------------------------------------------------------
    # 2026-mifid schema: basic parsing
    # ------------------------------------------------------------------

    def test_mifid_parse_single_trade(self, parser, mifid_json_single):
        df = parser.parse(mifid_json_single)

        assert len(df) == 1
        assert df.loc[0, "isin"] == "DE0007100000"
        assert df.loc[0, "price"] == 56.20
        assert df.loc[0, "quantity"] == 159.00
        assert df.loc[0, "price_currency"] == "EUR"
        assert df.loc[0, "schema_version"] == "2026-mifid"

    def test_mifid_parse_multiple_trades(self, parser, mifid_json_multiple):
        df = parser.parse(mifid_json_multiple)

        assert len(df) == 2
        assert df.loc[0, "isin"] == "DE0007100000"
        assert df.loc[1, "isin"] == "DE000A3H2200"

    def test_mifid_column_renaming(self, parser, mifid_json_single):
        df = parser.parse(mifid_json_single)

        assert "isin" in df.columns
        assert "price" in df.columns
        assert "quantity" in df.columns
        assert "trading_date_time" in df.columns
        assert "execution_venue" in df.columns
        assert "transaction_id" in df.columns
        assert "trading_system" in df.columns
        assert "price_notation" in df.columns
        assert "venue_publication" in df.columns

        assert "instrumentIdentificationCode" not in df.columns
        assert "tradingDateAndTime" not in df.columns

    def test_mifid_timestamp_conversion(self, parser, mifid_json_single):
        df = parser.parse(mifid_json_single)

        assert pd.api.types.is_datetime64_any_dtype(df["trading_date_time"])
        expected = pd.Timestamp("2026-03-15 10:30:00.123456789")
        assert df.loc[0, "trading_date_time"] == expected
        assert df["trading_date_time"].dt.tz is None

    def test_mifid_no_legacy_columns(self, parser, mifid_json_single):
        """2026-mifid files must not produce legacy-only columns."""
        df = parser.parse(mifid_json_single)

        assert "tick_id" not in df.columns
        assert "source_name" not in df.columns
        assert "instrument_id" not in df.columns

    # ------------------------------------------------------------------
    # Schema detection
    # ------------------------------------------------------------------

    def test_schema_version_legacy(self, parser, legacy_json_single):
        df = parser.parse(legacy_json_single)
        assert df.loc[0, "schema_version"] == "2025-legacy"

    def test_schema_version_mifid(self, parser, mifid_json_single):
        df = parser.parse(mifid_json_single)
        assert df.loc[0, "schema_version"] == "2026-mifid"

    def test_unknown_schema_raises(self, parser):
        unknown = json.dumps({"unknownField": "value", "anotherField": 42})

        with pytest.raises(XetraSchemaUnknownError) as exc_info:
            parser.parse(unknown)

        assert "unknownField" in str(exc_info.value) or "anotherField" in str(
            exc_info.value
        )

    def test_unknown_schema_error_contains_actual_fields(self, parser):
        unknown = json.dumps({"futureField": "x", "anotherFuture": 1})
        with pytest.raises(XetraSchemaUnknownError) as exc_info:
            parser.parse(unknown)

        assert exc_info.value.actual_fields is not None
        assert "futureField" in exc_info.value.actual_fields

    # ------------------------------------------------------------------
    # Tiered required fields
    # ------------------------------------------------------------------

    def test_hard_required_isin_missing_raises(self, parser, legacy_trade):
        # isin is also the schema sentinel — removing it causes XetraSchemaUnknownError
        # (schema detection fails before the hard-required check runs)
        del legacy_trade["isin"]
        with pytest.raises(XetraSchemaUnknownError):
            parser.parse(json.dumps(legacy_trade))

    def test_hard_required_price_missing_raises(self, parser, legacy_trade):
        del legacy_trade["lastTrade"]
        with pytest.raises(ValueError, match="Missing hard-required fields"):
            parser.parse(json.dumps(legacy_trade))

    def test_hard_required_quantity_missing_raises(self, parser, legacy_trade):
        del legacy_trade["lastQty"]
        with pytest.raises(ValueError, match="Missing hard-required fields"):
            parser.parse(json.dumps(legacy_trade))

    def test_soft_required_missing_stores_null(self, parser, legacy_trade):
        """Removing a soft-required field (execution_venue) stores null, doesn't raise."""
        del legacy_trade["executionVenueId"]
        df = parser.parse(json.dumps(legacy_trade))

        assert len(df) == 1
        assert "execution_venue" in df.columns
        assert pd.isna(df.loc[0, "execution_venue"])

    def test_soft_required_transaction_id_missing_stores_null(
        self, parser, legacy_trade
    ):
        del legacy_trade["transIdCode"]
        df = parser.parse(json.dumps(legacy_trade))

        assert "transaction_id" in df.columns
        assert pd.isna(df.loc[0, "transaction_id"])

    # ------------------------------------------------------------------
    # Empty / malformed input
    # ------------------------------------------------------------------

    def test_parse_empty_json(self, parser):
        df = parser.parse("")

        assert len(df) == 0
        assert isinstance(df, pd.DataFrame)
        assert "isin" in df.columns
        assert "price" in df.columns

    def test_parse_whitespace_only(self, parser):
        df = parser.parse("   \n\n   ")

        assert len(df) == 0
        assert isinstance(df, pd.DataFrame)

    def test_invalid_json_syntax(self, parser):
        invalid_json = '{"isin": "DE0007100000", "price":'

        with pytest.raises(json.JSONDecodeError):
            parser.parse(invalid_json)

    def test_parse_with_extra_newlines(self, parser, legacy_trade):
        trade1 = json.dumps(legacy_trade)
        trade2 = json.dumps(legacy_trade)
        df = parser.parse(f"{trade1}\n\n\n{trade2}\n\n")

        assert len(df) == 2

    # ------------------------------------------------------------------
    # Schema validation
    # ------------------------------------------------------------------

    def test_validate_schema_success(self, parser, legacy_json_single):
        df = parser.parse(legacy_json_single)
        assert parser.validate_schema(df) is True

    def test_validate_schema_missing_column(self, parser, legacy_json_single):
        df = parser.parse(legacy_json_single)
        df = df.drop(columns=["isin"])

        with pytest.raises(ValueError, match="Schema validation failed"):
            parser.validate_schema(df)

    def test_validate_schema_wrong_dtype(self, parser, legacy_json_single):
        df = parser.parse(legacy_json_single)
        df["price"] = df["price"].astype(str)

        with pytest.raises(ValueError, match="Schema validation failed"):
            parser.validate_schema(df)

    # ------------------------------------------------------------------
    # Real sample data (2025-legacy format from DETR-posttrade-2025-10-31T13_54)
    # ------------------------------------------------------------------

    def test_parse_real_legacy_sample(self, parser):
        real_sample = (
            '{"messageId":"posttrade","sourceName":"ETR","isin":"DE0007100000","currency":"EUR",'
            '"tickActionIndicator":"I","instrumentIdCode":"I","mmtMarketMechanism":"8",'
            '"mmtTradingMode":"2","mmtNegotTransPretrdWaivInd":"-","mmtModificationInd":"-",'
            '"mmtBenchmarkRefprcInd":"-","mmtPubModeDefReason":"-","mmtAlgoInd":"H",'
            '"quotationType":1,"lastQty":159.00,"lastTrade":56.20,'
            '"lastTradeTime":"2025-10-31T13:54:00.042457058Z",'
            '"distributionDateTime":"2025-10-31T13:54:00.052903000Z","tickId":33976320,'
            '"instrumentId":"DE0007100000",'
            '"transIdCode":"1000000000000025050760176191884004245705800000006636",'
            '"executionVenueId":"XETA"}\n'
            '{"messageId":"posttrade","sourceName":"ETR","isin":"DE000A3H2200","currency":"EUR",'
            '"tickActionIndicator":"I","instrumentIdCode":"I","mmtMarketMechanism":"8",'
            '"mmtTradingMode":"2","mmtNegotTransPretrdWaivInd":"-","mmtModificationInd":"-",'
            '"mmtBenchmarkRefprcInd":"-","mmtPubModeDefReason":"-","mmtAlgoInd":"H",'
            '"quotationType":1,"lastQty":3.00,"lastTrade":48.04,'
            '"lastTradeTime":"2025-10-31T13:54:00.052133524Z",'
            '"distributionDateTime":"2025-10-31T13:54:00.053188000Z","tickId":49699840,'
            '"instrumentId":"DE000A3H2200",'
            '"transIdCode":"1000000000000059069030176191884005213352400000009707",'
            '"executionVenueId":"XETA"}'
        )

        df = parser.parse(real_sample)

        assert len(df) == 2
        assert df["isin"].tolist() == ["DE0007100000", "DE000A3H2200"]
        assert df["price"].tolist() == [56.20, 48.04]
        assert df["quantity"].tolist() == [159.00, 3.00]
        assert df.loc[0, "schema_version"] == "2025-legacy"

    # ------------------------------------------------------------------
    # SCHEMAS registry integrity
    # ------------------------------------------------------------------

    def test_schemas_registry_has_both_versions(self, parser):
        assert "2025-legacy" in XetraParser.SCHEMAS
        assert "2026-mifid" in XetraParser.SCHEMAS

    def test_schemas_registry_hard_required_fields_present(self):
        """Every schema must map to all hard-required DF column names."""
        hard = XetraParser.HARD_REQUIRED_FIELDS
        for version, mapping in XetraParser.SCHEMAS.items():
            mapped_cols = set(mapping.values())
            missing = hard - mapped_cols
            assert not missing, (
                f"Schema '{version}' missing hard-required columns: {missing}"
            )

    def test_legacy_schema_contains_expected_fields(self):
        mapping = XetraParser.SCHEMAS["2025-legacy"]
        assert "isin" in mapping
        assert "lastTrade" in mapping
        assert "lastQty" in mapping
        assert "transIdCode" in mapping
        assert "executionVenueId" in mapping

    def test_mifid_schema_contains_expected_fields(self):
        mapping = XetraParser.SCHEMAS["2026-mifid"]
        assert "instrumentIdentificationCode" in mapping
        assert "price" in mapping
        assert "quantity" in mapping
        assert "transactionIdentificationCode" in mapping
        assert "venueOfExecution" in mapping

    def test_empty_dataframe_schema(self, parser):
        df = parser._create_empty_dataframe()

        assert len(df) == 0
        assert "isin" in df.columns
        assert "price" in df.columns
        assert "quantity" in df.columns
        assert "trading_date_time" in df.columns
        assert pd.api.types.is_datetime64_any_dtype(df["trading_date_time"])
