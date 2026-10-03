import json
import pandas as pd
from loguru import logger

from .exceptions import XetraSchemaUnknownError


class XetraParser:
    """Parse Xetra trade JSON into validated DataFrames."""

    # Multi-schema registry: version → {json_field: df_column}
    SCHEMAS = {
        "2025-legacy": {
            "messageId": "message_id",
            "sourceName": "source_name",
            "isin": "isin",  # sentinel
            "instrumentId": "instrument_id",
            "transIdCode": "transaction_id",
            "tickId": "tick_id",
            "lastTrade": "price",
            "lastQty": "quantity",
            "currency": "price_currency",
            "quotationType": "quote_type",
            "lastTradeTime": "trading_date_time",
            "distributionDateTime": "distribution_time",
            "executionVenueId": "execution_venue",
            "tickActionIndicator": "tick_action",
            "instrumentIdCode": "instrument_code",
            "mmtMarketMechanism": "market_mechanism",
            "mmtTradingMode": "trading_mode",
            "mmtNegotTransPretrdWaivInd": "negotiated_flag",
            "mmtModificationInd": "modification_flag",
            "mmtBenchmarkRefprcInd": "benchmark_flag",
            "mmtPubModeDefReason": "pub_deferral",
            "mmtAlgoInd": "algo_indicator",
        },
        "2026-mifid": {
            "instrumentIdentificationCode": "isin",  # sentinel
            "transactionIdentificationCode": "transaction_id",
            "price": "price",
            "quantity": "quantity",
            "priceCurrency": "price_currency",
            "tradingDateAndTime": "trading_date_time",
            "venueOfExecution": "execution_venue",
            "publicationDateAndTime": "distribution_time",
            "tradingSystem": "trading_system",
            "priceNotation": "price_notation",
            "venueOfPublication": "venue_publication",
            "mmtTradingMode": "trading_mode",
            "mmtModificationInd": "modification_flag",
            "mmtBenchmarkRefprcInd": "benchmark_flag",
            "mmtPubModeDefReason": "pub_deferral",
            "mmtAlgoInd": "algo_indicator",
        },
    }

    # Presence of this JSON field in the first record → schema version. First match wins.
    SCHEMA_SENTINELS = {
        "isin": "2025-legacy",
        "instrumentIdentificationCode": "2026-mifid",
    }

    # Records missing any hard-required DF column are dropped.
    HARD_REQUIRED_FIELDS = {"isin", "price", "quantity", "trading_date_time"}

    # Missing soft-required DF columns are added as null with a WARNING (record kept).
    SOFT_REQUIRED_FIELDS = {"transaction_id", "execution_venue", "price_currency"}

    # dtypes enforced after rename; used by validate_schema too.
    EXPECTED_DTYPES = {
        "isin": "object",
        "price": "float64",
        "quantity": "float64",
        "price_currency": "object",
        "trading_date_time": "datetime64[ns]",
        "distribution_time": "datetime64[ns]",
        "execution_venue": "object",
        "transaction_id": "object",
        "schema_version": "object",
    }

    def parse(self, json_str: str) -> pd.DataFrame:
        """
        Parse Xetra JSONL string into a validated DataFrame.

        Raises:
            XetraSchemaUnknownError: Field names match no registered schema.
            ValueError: Hard-required fields absent after schema detection.
            json.JSONDecodeError: JSON syntax is invalid.
        """
        try:
            trades = []
            for line in json_str.strip().split("\n"):
                if line.strip():
                    trades.append(json.loads(line))

            if not trades:
                logger.warning("Parsed empty trade array from JSON")
                return self._create_empty_dataframe()

            logger.debug(f"Parsed {len(trades)} trade records from JSON")

            raw_fields = list(trades[0].keys())
            schema_version = self._detect_schema(raw_fields)
            field_mapping = self.SCHEMAS[schema_version]
            logger.debug(f"Detected schema: {schema_version}")

            df = pd.DataFrame(trades)
            df = df.rename(columns=field_mapping)

            self._validate_required_fields(df)

            df = self._convert_timestamps(df)
            df = self._normalize_types(df)

            if "algo_indicator" in df.columns:
                df["algo_indicator"] = df["algo_indicator"] == "H"

            df["schema_version"] = schema_version
            df = self._ensure_complete_schema(df, field_mapping)

            logger.info(
                f"Successfully parsed {len(df)} trades [{schema_version}] with {len(df.columns)} columns"
            )

            return df

        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {e}")
            raise
        except (XetraSchemaUnknownError, ValueError):
            raise
        except Exception as e:
            logger.error(f"Unexpected parsing error: {e}")
            raise

    def _detect_schema(self, raw_fields: list[str]) -> str:
        """Return the schema version for the given JSON field list, or raise XetraSchemaUnknownError."""
        for sentinel, version in self.SCHEMA_SENTINELS.items():
            if sentinel in raw_fields:
                return version
        raise XetraSchemaUnknownError(raw_fields)

    def _validate_required_fields(self, df: pd.DataFrame) -> None:
        """
        Apply tiered field validation.

        Hard-required: raises ValueError if any are absent.
        Soft-required: adds null column with a WARNING if absent.
        """
        missing_hard = [
            col for col in self.HARD_REQUIRED_FIELDS if col not in df.columns
        ]
        if missing_hard:
            raise ValueError(
                f"Missing hard-required fields in trade data: {', '.join(sorted(missing_hard))}"
            )

        missing_soft = [
            col for col in self.SOFT_REQUIRED_FIELDS if col not in df.columns
        ]
        if missing_soft:
            logger.warning(
                f"Soft-required fields missing, storing as null: {sorted(missing_soft)}"
            )
            for col in missing_soft:
                df[col] = None

    def _convert_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert ISO 8601 timestamp strings to timezone-naive datetime64[ns]."""
        for col in ("trading_date_time", "distribution_time"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True).dt.tz_localize(None)
        return df

    def _normalize_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enforce expected dtypes on key columns."""
        for col, dtype in self.EXPECTED_DTYPES.items():
            if col in df.columns:
                if dtype == "float64":
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif dtype == "int64":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        return df

    def _ensure_complete_schema(
        self, df: pd.DataFrame, field_mapping: dict
    ) -> pd.DataFrame:
        """
        Add any missing mapped columns as None; drop unmapped columns; reorder.

        Columns are ordered by field_mapping sequence with schema_version appended.
        """
        expected = list(
            dict.fromkeys(list(field_mapping.values()) + ["schema_version"])
        )

        missing = [col for col in expected if col not in df.columns]
        if missing:
            logger.debug(f"Adding {len(missing)} missing optional columns: {missing}")
            for col in missing:
                df[col] = None

        return df[[col for col in expected if col in df.columns]]

    def _create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with minimum schema for no-data files."""
        columns = sorted(self.HARD_REQUIRED_FIELDS | self.SOFT_REQUIRED_FIELDS) + [
            "distribution_time",
            "algo_indicator",
            "schema_version",
        ]
        df = pd.DataFrame(columns=columns)
        for col, dtype in self.EXPECTED_DTYPES.items():
            if col in df.columns:
                if dtype == "datetime64[ns]":
                    df[col] = pd.to_datetime(df[col])
                elif dtype == "float64":
                    df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    def validate_schema(self, df: pd.DataFrame) -> bool:
        """
        Validate DataFrame schema matches expected structure.

        Raises:
            ValueError: If hard-required columns are missing or dtypes are wrong.
        """
        missing = [col for col in self.HARD_REQUIRED_FIELDS if col not in df.columns]
        if missing:
            raise ValueError(f"Schema validation failed: missing columns {missing}")

        for col, expected_dtype in self.EXPECTED_DTYPES.items():
            if col in df.columns:
                actual_dtype = str(df[col].dtype)
                if expected_dtype == "int64" and actual_dtype == "Int64":
                    continue
                if not actual_dtype.startswith(expected_dtype.split("[")[0]):
                    raise ValueError(
                        f"Schema validation failed: column '{col}' has dtype '{actual_dtype}', "
                        f"expected '{expected_dtype}'"
                    )

        logger.debug(f"Schema validation passed for {len(df)} rows")
        return True
