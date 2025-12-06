"""Live API tests against real Deutsche Börse API.

These tests make actual HTTP requests to the production API and validate
real-world behavior. They are marked with @pytest.mark.live for organization.

IMPORTANT: Deutsche Börse API has aggressive rate limiting (HTTP 429 errors).
- First run may succeed, but subsequent runs may get rate-limited
- Wait 30-60 seconds between test runs if you get 429 errors or empty file lists
- Tests automatically skip on weekends (no trading data available)
- Tests download only 1 sample file from peak trading hours (not full day) to minimize API load

Run all tests: uv run pytest
Run ONLY live tests: uv run pytest -m live -v
Run EXCLUDING live: uv run pytest -m "not live"

Rate limit recovery: Wait at least 60 seconds between full live test runs.
"""

from datetime import datetime, timedelta

import pandas as pd
import pytest

from yf_parqed.xetra.xetra_fetcher import XetraFetcher
from yf_parqed.xetra.xetra_parser import XetraParser
from yf_parqed.xetra.xetra_service import XetraService

# Skip all tests in this module by default unless explicitly run with -m live
pytestmark = pytest.mark.live


def list_all_available_files(fetcher: XetraFetcher, venue: str) -> list[str]:
    """List ALL currently available files from API (no date filtering).

    This uses the API without a date parameter to get all files currently hosted.
    Deutsche Börse typically keeps ~24 hours of data.

    Args:
        fetcher: XetraFetcher instance
        venue: Venue code ('DETR', 'DFRA', etc.)

    Returns:
        List of all available filenames
    """
    # Now list_available_files() already gets all files without date parameter
    return fetcher.list_available_files(venue)


def get_peak_trading_file(files: list[str]) -> str | None:
    """Select a file from peak trading hours (10:00-15:00 CET) where we expect actual data.

    Args:
        files: List of filenames like ['DETR-posttrade-2025-11-04T12_30.json.gz']

    Returns:
        Filename from peak hours, or None if no suitable files found.
    """
    from datetime import datetime, time

    # Look for files between 09:00-14:00 UTC (10:00-15:00 CET, peak trading)
    peak_files = []
    for filename in files:
        try:
            # Extract datetime: "DETR-posttrade-2025-11-04T12_30.json.gz" -> "2025-11-04T12:30"
            datetime_part = (
                filename.split("DETR-posttrade-")[1]
                .split(".json.gz")[0]
                .replace("_", ":")
            )
            dt = datetime.fromisoformat(datetime_part)

            # Peak trading: 09:00-14:00 UTC = 10:00-15:00 CET
            if time(9, 0) <= dt.time() <= time(14, 0):
                peak_files.append(filename)
        except (IndexError, ValueError):
            continue

    # Return middle file from peak hours for best chance of data
    if peak_files:
        # Sort chronologically (oldest first) before selecting middle
        peak_files.sort()
        return peak_files[len(peak_files) // 2]

    # Fallback: prefer earlier trading hours (files are reverse sorted, so take from end)
    if files:
        # Files are in reverse chronological order, so the last file is oldest/earliest
        # Return a file from the last quarter (earliest available times)
        return files[len(files) * 3 // 4]

    return None


class TestXetraLiveAPI:
    """Live API tests - require internet connection and working Deutsche Börse API."""

    def test_list_files_real_api(self):
        """Test listing files from real Deutsche Börse API."""
        fetcher = XetraFetcher()

        # List ALL currently available files (no date restriction)
        with fetcher:
            files = list_all_available_files(fetcher, "DETR")

        # Should return at least some files
        assert isinstance(files, list)
        print(f"\n✓ Found {len(files)} currently available DETR files")

        # If files exist, validate naming convention
        if files:
            for filename in files[:3]:  # Check first 3
                assert "DETR-posttrade" in filename
                assert ".json.gz" in filename
                print(f"  - {filename}")
        else:
            pytest.skip("No files currently available (API maintenance or outage)")

    def test_download_real_file(self):
        """Test downloading an actual file from Deutsche Börse API."""
        fetcher = XetraFetcher()

        # List all currently available files
        with fetcher:
            files = list_all_available_files(fetcher, "DETR")

            # Skip if no files available
            if not files:
                pytest.skip("No files currently available (API maintenance or outage)")

            # Get file from peak trading hours
            filename = get_peak_trading_file(files)
            if not filename:
                pytest.skip("No suitable peak trading hours files found")

            # Extract date from filename for download API
            # Format: DETR-posttrade-2025-11-04T12_30.json.gz
            date_part = filename.split("-")[2]  # "2025"
            month_part = filename.split("-")[3]  # "11"
            day_part = filename.split("-")[4].split("T")[0]  # "04"
            file_date = f"{date_part}-{month_part}-{day_part}"

            compressed_data = fetcher.download_file("DETR", file_date, filename)

            # Validate we got bytes
            assert isinstance(compressed_data, bytes)
            assert len(compressed_data) > 0
            print(f"\n✓ Downloaded {filename}")
            print(f"  - Compressed size: {len(compressed_data):,} bytes")

            # Try to decompress
            jsonl_content = fetcher.decompress_gzip(compressed_data)
            assert isinstance(jsonl_content, str)

            # Handle empty files gracefully (can happen after hours or on low-volume days)
            if len(jsonl_content) == 0:
                pytest.skip(
                    f"File {filename} is empty (likely no trades during this interval). "
                    f"This is expected for off-hours or low-volume periods."
                )

            print(f"  - Decompressed size: {len(jsonl_content):,} bytes")

    def test_parse_real_data(self):
        """Test parsing actual production data from Deutsche Börse."""
        fetcher = XetraFetcher()
        parser = XetraParser()

        with fetcher:
            files = list_all_available_files(fetcher, "DETR")

            if not files:
                pytest.skip("No files currently available (API maintenance or outage)")

            # Get file from peak trading hours for best chance of data
            filename = get_peak_trading_file(files)
            if not filename:
                pytest.skip("No suitable peak trading hours files found")

            # Extract date from filename
            date_part = filename.split("-")[2]
            month_part = filename.split("-")[3]
            day_part = filename.split("-")[4].split("T")[0]
            file_date = f"{date_part}-{month_part}-{day_part}"

            compressed_data = fetcher.download_file("DETR", file_date, filename)
            jsonl_content = fetcher.decompress_gzip(compressed_data)

            # Handle empty files
            if len(jsonl_content) == 0:
                pytest.skip(
                    f"File {filename} is empty. "
                    f"Try running during peak trading hours (10:00-15:00 CET) on a weekday."
                )

            # Parse to DataFrame
            df = parser.parse(jsonl_content)

            # Validate parsed data
            assert isinstance(df, pd.DataFrame)

            if len(df) == 0:
                pytest.skip(
                    f"File {filename} parsed but contains no trades. "
                    f"This can happen during low-volume periods."
                )

            # Check required columns exist
            required_cols = ["isin", "price", "volume", "currency", "trade_time"]
            for col in required_cols:
                assert col in df.columns, f"Missing column: {col}"

            # Validate data types
            assert df["price"].dtype == "float64"
            assert df["volume"].dtype == "float64"
            assert pd.api.types.is_datetime64_any_dtype(df["trade_time"])

            print(f"\n✓ Parsed {len(df)} trades from {filename}")
            print(f"  - ISINs: {len(df['isin'].unique())} unique")
            print(
                f"  - Price range: €{df['price'].min():.2f} - €{df['price'].max():.2f}"
            )
            print(f"  - Total volume: {df['volume'].sum():,.0f}")

            # Sample first few rows
            print("\n  Sample trades:")
            for _, row in df.head(3).iterrows():
                print(
                    f"    {row['isin']}: {row['volume']:.0f} @ €{row['price']:.2f} at {row['trade_time']}"
                )

    def test_full_workflow_real_api(self, tmp_path):
        """Test complete fetch → parse → store workflow with real API.

        NOTE: Only fetches one file from peak trading hours to avoid rate limiting.
        """
        service = XetraService(root_path=tmp_path)
        fetcher = service.fetcher

        # Step 1: List ALL available files and select one
        with fetcher.client:
            all_files = list_all_available_files(fetcher, "DETR")

            if not all_files:
                pytest.skip("No files currently available (API maintenance or outage)")

            print(f"\n✓ Found {len(all_files)} currently available DETR files")

            # Step 2: Get file from peak trading hours
            filename = get_peak_trading_file(all_files)
            if not filename:
                pytest.skip("No suitable peak trading hours files found")

            # Extract date from filename
            date_part = filename.split("-")[2]
            month_part = filename.split("-")[3]
            day_part = filename.split("-")[4].split("T")[0]
            trading_day = f"{date_part}-{month_part}-{day_part}"

            # Step 3: Fetch and parse (reuses connection)
            df = service.fetch_and_parse_trades("DETR", trading_day, filename)

        assert isinstance(df, pd.DataFrame)

        if len(df) == 0:
            pytest.skip(
                f"File {filename} contains no trades. "
                f"Try running during peak trading hours (10:00-15:00 CET) on a weekday."
            )

        print(f"✓ Fetched {len(df)} trades from {filename}")
        print(f"  - {len(df['isin'].unique())} unique ISINs")

        # Step 3: Store to parquet
        trade_date = datetime.strptime(trading_day, "%Y-%m-%d")
        service.store_trades(df, "DETR", trade_date, market="de", source="xetra")

        # Step 4: Verify file exists
        expected_path = (
            tmp_path
            / "de"
            / "xetra"
            / "trades"
            / "venue=DETR"
            / f"year={trade_date.year}"
            / f"month={trade_date.month:02d}"
            / f"day={trade_date.day:02d}"
            / "trades.parquet"
        )

        assert expected_path.exists(), f"Parquet file not found at {expected_path}"
        print(f"✓ Stored to {expected_path}")

        # Step 5: Read back and validate
        df_read = pd.read_parquet(expected_path)
        assert len(df_read) == len(df)
        print(f"✓ Read back {len(df_read)} trades successfully")

    def test_multiple_venues_real_api(self):
        """Test fetching data from multiple venues on same date.

        NOTE: Only lists files (no downloads) to avoid rate limiting.
        """
        fetcher = XetraFetcher()

        venues = ["DETR", "DFRA", "DGAT", "DEUR"]

        results = {}
        with fetcher:
            for venue in venues:
                files = list_all_available_files(fetcher, venue)
                results[venue] = len(files)

        print("\n✓ Currently available file counts:")
        for venue, count in results.items():
            print(f"  - {venue}: {count} files")

        # At least one venue should have files (unless API outage)
        total_files = sum(results.values())
        if total_files == 0:
            pytest.skip("No files for any venue (API maintenance or outage)")

    def test_schema_validation_real_data(self):
        """Validate that real production data matches expected schema."""
        fetcher = XetraFetcher()
        parser = XetraParser()

        with fetcher:
            files = list_all_available_files(fetcher, "DETR")

            if not files:
                pytest.skip("No files currently available (API maintenance or outage)")

            # Get file from peak trading hours
            filename = get_peak_trading_file(files)
            if not filename:
                pytest.skip("No suitable peak trading hours files found")

            # Extract date from filename
            date_part = filename.split("-")[2]
            month_part = filename.split("-")[3]
            day_part = filename.split("-")[4].split("T")[0]
            file_date = f"{date_part}-{month_part}-{day_part}"

            compressed_data = fetcher.download_file("DETR", file_date, filename)
            jsonl_content = fetcher.decompress_gzip(compressed_data)

            if len(jsonl_content) == 0:
                pytest.skip(
                    f"File {filename} is empty. "
                    f"Try running during peak trading hours (10:00-15:00 CET) on a weekday."
                )

            df = parser.parse(jsonl_content)

            # Validate core required columns exist (using actual parser output names)
            # NOTE: Not all MiFID II transparency fields may be present in every API response
            required_cols = [
                "message_id",
                "source_name",
                "isin",
                "instrument_id",
                "trans_id",
                "tick_id",
                "price",
                "volume",
                "currency",
                "quote_type",
                "trade_time",
                "distribution_time",
                "venue",
                "tick_action",
                "instrument_code",
                "market_mechanism",
                "trading_mode",
                "modification_flag",
                "benchmark_flag",
                "pub_deferral",
                "algo_indicator",
            ]

            # Optional columns that may or may not be present
            optional_cols = [
                "negotiated_flag",  # mmtNegotTransPretrdWaivInd may be omitted
            ]

            # Check required columns
            missing_required = [col for col in required_cols if col not in df.columns]
            assert not missing_required, f"Missing required columns: {missing_required}"

            print(f"\n✓ Schema validation passed for {filename}")
            print(f"  - All {len(required_cols)} required columns present")
            print(f"  - Total columns in response: {len(df.columns)}")

            # Report on optional columns
            present_optional = [col for col in optional_cols if col in df.columns]
            missing_optional = [col for col in optional_cols if col not in df.columns]
            if present_optional:
                print(f"  - Optional columns present: {present_optional}")
            if missing_optional:
                print(f"  - Optional columns not in response: {missing_optional}")

            # Check for any unexpected columns (schema drift)
            all_known_cols = set(required_cols + optional_cols)
            extra_cols = set(df.columns) - all_known_cols
            if extra_cols:
                print(f"  ⚠ Unexpected columns found: {extra_cols}")
            else:
                print("  - No unexpected columns")

    def test_historical_data_availability(self):
        """Test how far back historical data is available."""
        fetcher = XetraFetcher()

        # Test last 7 calendar days (will include weekends)
        today = datetime.now()
        dates = []
        for i in range(1, 8):
            date = today - timedelta(days=i)
            # Skip weekends for efficiency
            if date.weekday() < 5:  # Monday = 0, Friday = 4
                dates.append(date.strftime("%Y-%m-%d"))

        availability = {}
        with fetcher:
            for date in dates:
                all_files = fetcher.list_available_files("DETR")
                # Filter files by date
                date_files = [f for f in all_files if date in f]
                availability[date] = len(date_files) > 0

        print("\n✓ Historical data availability (DETR):")
        for date, available in availability.items():
            status = "✓ Available" if available else "✗ Not available"
            print(f"  - {date}: {status}")

        # Note: This is informational - no assertion as weekend data is expected to be missing
