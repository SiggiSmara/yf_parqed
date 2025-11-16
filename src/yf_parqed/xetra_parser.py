import json
import pandas as pd
from loguru import logger


class XetraParser:
    """Parse Xetra trade JSON into validated DataFrames."""

    # Required fields that must be present in every trade record
    REQUIRED_FIELDS = [
        "isin",
        "lastTrade",
        "lastQty",
        "currency",
        "lastTradeTime",
        "transIdCode",
        "tickId",
    ]

    # Complete field mapping: JSON field name → DataFrame column name
    FIELD_MAPPING = {
        # Core identifiers
        "messageId": "message_id",
        "sourceName": "source_name",
        "isin": "isin",
        "instrumentId": "instrument_id",
        "transIdCode": "trans_id",
        "tickId": "tick_id",
        # Trade details
        "lastTrade": "price",
        "lastQty": "volume",
        "currency": "currency",
        "quotationType": "quote_type",
        # Timestamps
        "lastTradeTime": "trade_time",
        "distributionDateTime": "distribution_time",
        # Execution venue
        "executionVenueId": "venue",
        # MiFID II transparency fields
        "tickActionIndicator": "tick_action",
        "instrumentIdCode": "instrument_code",
        "mmtMarketMechanism": "market_mechanism",
        "mmtTradingMode": "trading_mode",
        "mmtNegotTransPretrdWaivInd": "negotiated_flag",
        "mmtModificationInd": "modification_flag",
        "mmtBenchmarkRefprcInd": "benchmark_flag",
        "mmtPubModeDefReason": "pub_deferral",
        "mmtAlgoInd": "algo_indicator",
    }

    # Expected data types for validation
    EXPECTED_DTYPES = {
        "isin": "object",
        "price": "float64",
        "volume": "float64",
        "currency": "object",
        "trade_time": "datetime64[ns]",
        "venue": "object",
        "trans_id": "object",
        "tick_id": "int64",
    }

    def parse(self, json_str: str) -> pd.DataFrame:
        """
        Parse Xetra JSON string into DataFrame.

        Args:
            json_str: JSON string containing array of trade records (one JSON object per line)

        Returns:
            DataFrame with normalized column names and proper data types

        Raises:
            ValueError: If JSON is malformed or required fields missing
            json.JSONDecodeError: If JSON syntax is invalid

        Example:
            >>> parser = XetraParser()
            >>> json_data = '{"isin":"DE0007100000","lastTrade":56.20,...}\\n{"isin":"...",...}'
            >>> df = parser.parse(json_data)
            >>> print(df.columns)
            Index(['isin', 'price', 'volume', 'currency', 'trade_time', ...])
        """
        try:
            # Parse JSONL (one JSON object per line)
            trades = []
            for line in json_str.strip().split("\n"):
                if line.strip():  # Skip empty lines
                    trades.append(json.loads(line))

            if not trades:
                logger.warning("Parsed empty trade array from JSON")
                return self._create_empty_dataframe()

            logger.debug(f"Parsed {len(trades)} trade records from JSON")

            # Convert to DataFrame
            df = pd.DataFrame(trades)

            # Rename columns using mapping
            df = df.rename(columns=self.FIELD_MAPPING)

            # Validate required fields
            self._validate_required_fields(df)

            # Convert timestamps
            df = self._convert_timestamps(df)

            # Normalize data types
            df = self._normalize_types(df)

            # Convert algo indicator to boolean
            if "algo_indicator" in df.columns:
                df["algo_indicator"] = df["algo_indicator"] == "H"

            # Ensure all expected columns are present for Parquet schema stability
            df = self._ensure_complete_schema(df)

            logger.info(
                f"Successfully parsed {len(df)} trades with {len(df.columns)} columns"
            )

            return df

        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected parsing error: {e}")
            raise

    def _validate_required_fields(self, df: pd.DataFrame) -> None:
        """
        Validate that all required fields are present.

        Raises:
            ValueError: If any required field is missing
        """
        # Map required JSON fields to DataFrame column names
        required_columns = [self.FIELD_MAPPING[field] for field in self.REQUIRED_FIELDS]

        missing = [col for col in required_columns if col not in df.columns]

        if missing:
            raise ValueError(
                f"Missing required fields in trade data: {', '.join(missing)}"
            )

    def _convert_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert timestamp strings to pandas datetime.

        Handles nanosecond precision ISO 8601 timestamps.
        """
        timestamp_cols = ["trade_time", "distribution_time"]

        for col in timestamp_cols:
            if col in df.columns:
                # Parse ISO 8601 with nanosecond precision, remove timezone info
                df[col] = pd.to_datetime(df[col], utc=True).dt.tz_localize(None)

        return df

    def _normalize_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize data types to expected schema.

        Ensures price/volume are float64, tick_id is int64, etc.
        """
        for col, dtype in self.EXPECTED_DTYPES.items():
            if col in df.columns:
                if dtype == "float64":
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif dtype == "int64":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                # object types don't need conversion

        return df

    def _ensure_complete_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure all columns from FIELD_MAPPING are present in DataFrame.

        API responses may omit optional fields like mmtNegotTransPretrdWaivInd.
        Missing columns are added with None/NaN values to maintain stable Parquet schema.

        Args:
            df: Parsed DataFrame with potentially missing columns

        Returns:
            DataFrame with all expected columns present
        """
        expected_columns = list(self.FIELD_MAPPING.values())
        missing_columns = [col for col in expected_columns if col not in df.columns]

        if missing_columns:
            logger.debug(
                f"Adding {len(missing_columns)} missing columns to DataFrame: {missing_columns}"
            )
            for col in missing_columns:
                df[col] = (
                    None  # Will be properly typed by _normalize_types if applicable
                )

        # Reorder columns to match FIELD_MAPPING order for consistency
        df = df[expected_columns]

        return df

    def _create_empty_dataframe(self) -> pd.DataFrame:
        """
        Create empty DataFrame with correct schema.

        Used when no trades are found in JSON.
        """
        # Create DataFrame with all mapped columns
        columns = list(self.FIELD_MAPPING.values())
        df = pd.DataFrame(columns=columns)

        # Set data types for empty DataFrame
        for col, dtype in self.EXPECTED_DTYPES.items():
            if col in df.columns:
                if dtype == "datetime64[ns]":
                    df[col] = pd.to_datetime(df[col])
                elif dtype == "float64":
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif dtype == "int64":
                    df[col] = pd.array([], dtype="Int64")

        return df

    def validate_schema(self, df: pd.DataFrame) -> bool:
        """
        Validate DataFrame schema matches expected structure.

        Args:
            df: DataFrame to validate

        Returns:
            True if schema is valid

        Raises:
            ValueError: If schema validation fails
        """
        # Check required columns exist
        required_columns = [self.FIELD_MAPPING[field] for field in self.REQUIRED_FIELDS]
        missing = [col for col in required_columns if col not in df.columns]

        if missing:
            raise ValueError(f"Schema validation failed: missing columns {missing}")

        # Check data types for critical columns
        for col, expected_dtype in self.EXPECTED_DTYPES.items():
            if col in df.columns:
                actual_dtype = str(df[col].dtype)

                # Allow Int64 (nullable) for int64
                if expected_dtype == "int64" and actual_dtype == "Int64":
                    continue

                if not actual_dtype.startswith(expected_dtype.split("[")[0]):
                    raise ValueError(
                        f"Schema validation failed: column '{col}' has dtype '{actual_dtype}', "
                        f"expected '{expected_dtype}'"
                    )

        logger.debug(f"Schema validation passed for {len(df)} rows")
        return True
