from datetime import datetime
from typing import List, Optional
from pathlib import Path

import pandas as pd
from loguru import logger

from .partitioned_storage_backend import PartitionedStorageBackend
from .partition_path_builder import PartitionPathBuilder
from .xetra_fetcher import XetraFetcher
from .xetra_parser import XetraParser
from .config_service import ConfigService


class XetraService:
    """
    Orchestration layer for Xetra trade data operations.

    Coordinates fetching, parsing, and storage of Deutsche Börse trade data.
    """

    def __init__(
        self,
        fetcher: Optional[XetraFetcher] = None,
        parser: Optional[XetraParser] = None,
        backend: Optional[PartitionedStorageBackend] = None,
        root_path: Optional[Path] = None,
        config: Optional[ConfigService] = None,
    ):
        """
        Initialize XetraService with injected dependencies.

        Args:
            fetcher: HTTP client for Deutsche Börse API (default: XetraFetcher())
            parser: JSON→DataFrame parser (default: XetraParser())
            backend: Parquet storage backend (default: PartitionedStorageBackend)
            root_path: Root directory for data storage (default: Path("data"))
            config: Configuration service for rate limiting (default: ConfigService())
        """
        # Initialize config first to get rate limits
        self.config = config or ConfigService()
        inter_request_delay, burst_size, burst_cooldown = self.config.get_xetra_limits()

        # Initialize fetcher with config-based rate limits
        self.fetcher = fetcher or XetraFetcher(
            inter_request_delay=inter_request_delay,
            burst_size=burst_size,
            burst_cooldown=burst_cooldown,
        )
        self.parser = parser or XetraParser()

        # Default backend configuration for Xetra trades
        if backend is None:
            path_builder = PartitionPathBuilder(root=root_path or Path("data"))
            self.backend = PartitionedStorageBackend(
                empty_frame_factory=lambda: pd.DataFrame(),
                normalizer=lambda df: df,
                column_provider=lambda: [],
                path_builder=path_builder,
            )
        else:
            self.backend = backend

    def get_missing_dates(
        self, venue: str, market: str = "de", source: str = "xetra"
    ) -> List[str]:
        """
        Determine which dates have available data from Xetra but are not yet stored locally.

        This method checks:
        1. What dates are available from the API (extracts all unique dates from available files)
        2. What dates are already stored locally
        3. Returns dates that need to be fetched

        Args:
            venue: Venue code ('DETR', 'DFRA', 'DGAT', 'DEUR')
            market: Market code (default: 'de')
            source: Source code (default: 'xetra')

        Returns:
            List of dates in 'YYYY-MM-DD' format that should be fetched

        Example:
            >>> service = XetraService()
            >>> dates = service.get_missing_dates('DETR')
            >>> print(dates)  # e.g., ['2025-11-04', '2025-11-03']
        """
        # Get ALL available files from API
        try:
            all_files = self.fetcher.list_available_files(venue)
        except Exception as e:
            logger.error(f"Could not list files for {venue}: {e}")
            return []

        if not all_files:
            logger.info(f"No files available from API for {venue}")
            return []

        # Extract unique dates from filenames
        # Filename format: DETR-posttrade-2025-10-31T13_54.json.gz
        available_dates_set = set()
        for filename in all_files:
            try:
                if "T" in filename:
                    file_date = filename.rsplit("T", 1)[0][
                        -10:
                    ]  # Last 10 chars before T
                    # Validate date format
                    datetime.strptime(file_date, "%Y-%m-%d")
                    available_dates_set.add(file_date)
            except (IndexError, ValueError):
                # Skip files we can't parse
                continue

        if not available_dates_set:
            logger.info(f"No dates available from API for {venue}")
            return []

        available_dates = sorted(list(available_dates_set))

        # Check which dates need to be fetched (either missing or incomplete)
        # Return all available dates - the incremental logic will check which files are already stored
        missing_dates = []
        for date_str in available_dates:
            trade_date = datetime.strptime(date_str, "%Y-%m-%d")
            year = trade_date.year
            month = f"{trade_date.month:02d}"
            day = f"{trade_date.day:02d}"

            # Check if parquet file exists for this date
            base_dir = (
                self.backend._path_builder._root
                / market
                / source
                / "trades"
                / f"venue={venue}"
                / f"year={year}"
                / f"month={month}"
                / f"day={day}"
            )
            parquet_path = base_dir / "trades.parquet"

            if not parquet_path.exists():
                missing_dates.append(date_str)
                logger.info(f"Missing: {venue} {date_str} (not stored locally)")
            else:
                # File exists but may be incomplete - include it so incremental logic can check
                missing_dates.append(date_str)
                logger.debug(
                    f"Checking {venue} {date_str} for missing files (incremental)"
                )

        return missing_dates

    def list_files(self, venue: str, date: str) -> List[str]:
        """
        List available trade files for a venue/date.

        Args:
            venue: Venue code ('DETR', 'DFRA', 'DGAT', 'DEUR')
            date: Trade date in 'YYYY-MM-DD' format

        Returns:
            List of filenames for the specified date
            (e.g., ['DETR-posttrade-2025-10-31T13_54.json.gz'])

        Example:
            >>> service = XetraService()
            >>> files = service.list_files('DETR', '2025-10-31')
            >>> print(len(files))
            12
        """
        # Get ALL available files from API (no date filter)
        all_files = self.fetcher.list_available_files(venue)

        # Filter to only files matching the requested date
        # Filename format: DETR-posttrade-2025-11-04T09_00.json.gz
        # Extract date part and compare
        date_files = []
        for filename in all_files:
            try:
                # Extract date from filename: split by 'T' and get the date part before it
                # "DETR-posttrade-2025-11-04T09_00.json.gz" -> get "2025-11-04"
                if "T" in filename:
                    file_date = filename.rsplit("T", 1)[0][
                        -10:
                    ]  # Last 10 chars before T
                    if file_date == date:
                        date_files.append(filename)
            except (IndexError, ValueError):
                # Skip files we can't parse
                continue

        return date_files

    def fetch_and_parse_trades(
        self, venue: str, date: str, filename: str
    ) -> pd.DataFrame:
        """
        Download, decompress, and parse a single trade file.

        Args:
            venue: Venue code ('DETR', 'DFRA', etc.)
            date: Trade date in 'YYYY-MM-DD' format
            filename: Full filename (e.g., 'DETR-posttrade-2025-10-31T13_54.json.gz')

        Returns:
            DataFrame with parsed trades (23 columns)

        Raises:
            httpx.HTTPStatusError: On HTTP errors (404, 500)
            httpx.RequestError: On network failures
            ValueError: If JSON parsing or validation fails

        Example:
            >>> service = XetraService()
            >>> df = service.fetch_and_parse_trades('DETR', '2025-10-31', 'DETR-posttrade-2025-10-31T13_54.json.gz')
            >>> print(len(df))
            150
        """
        logger.debug(f"Fetching {filename} for {venue} on {date}")

        # Download gzipped file
        compressed_data = self.fetcher.download_file(venue, date, filename)

        # Decompress
        json_str = self.fetcher.decompress_gzip(compressed_data)

        # Parse to DataFrame
        df = self.parser.parse(json_str)

        logger.debug(
            f"Parsed {len(df)} trades from {filename} ({len(df['isin'].unique())} unique ISINs)"
        )

        return df

    def fetch_all_trades_for_date(self, venue: str, date: str) -> pd.DataFrame:
        """
        Fetch and combine all trade files for a venue/date.

        Args:
            venue: Venue code ('DETR', 'DFRA', etc.)
            date: Trade date in 'YYYY-MM-DD' format

        Returns:
            Combined DataFrame with all trades for the day

        Example:
            >>> service = XetraService()
            >>> df = service.fetch_all_trades_for_date('DETR', '2025-10-31')
            >>> print(f"{len(df)} total trades")
            1500 total trades
        """
        files = self.list_files(venue, date)

        if not files:
            logger.warning(f"No trade files found for {venue} on {date}")
            return pd.DataFrame()

        logger.info(f"Found {len(files)} files for {venue} on {date}")

        all_trades = []
        for filename in files:
            try:
                df = self.fetch_and_parse_trades(venue, date, filename)
                all_trades.append(df)
            except Exception as e:
                logger.error(f"Failed to process {filename}: {e}")
                # Continue with other files
                continue

        if not all_trades:
            logger.warning(f"No trades successfully parsed for {venue} on {date}")
            return pd.DataFrame()

        # Combine all DataFrames
        combined = pd.concat(all_trades, ignore_index=True)
        logger.info(
            f"Combined {len(combined)} total trades from {len(all_trades)} files"
        )

        return combined

    def fetch_and_store_missing_trades_incremental(
        self,
        venue: str,
        market: str = "de",
        source: str = "xetra",
        consolidate: bool = True,
    ) -> dict:
        """
        Fetch and store missing trade data with incremental saves and optional consolidation.

        Storage strategy (inspired by yf-parqed pattern):
        1. **Download phase**: Store AFTER EACH FILE (interrupt-safe, can resume)
        2. **Consolidation phase**: After all files for a date complete, optionally
           consolidate into single optimized parquet file

        This provides:
        - Resilience: Interruptions lose only current file (1-2 seconds of work)
        - Resume capability: Re-running skips already-stored data
        - Efficiency: Consolidation reduces file count from 1142 → 1 per date
        - Storage optimization: Single file per date is easier to backup/query

        Args:
            venue: Venue code ('DETR', 'DFRA', 'DGAT', 'DEUR')
            market: Market code (default: 'de')
            source: Source code (default: 'xetra')
            consolidate: If True, consolidate per-file storage into single file per date (default: True)

        Returns:
            Dictionary with summary statistics:
            {
                'dates_checked': ['2025-11-04', '2025-11-03'],
                'dates_fetched': ['2025-11-04'],
                'dates_partial': ['2025-11-03'],  # Interrupted mid-date
                'total_trades': 1500,
                'total_files': 600,
                'consolidated': True
            }

        Example:
            >>> service = XetraService()
            >>> # Safe incremental storage
            >>> summary = service.fetch_and_store_missing_trades_incremental('DETR')
            >>> print(f"Stored {summary['total_files']} files, {summary['total_trades']} trades")

            >>> # Download only, no consolidation (useful for testing)
            >>> summary = service.fetch_and_store_missing_trades_incremental('DETR', consolidate=False)
        """
        logger.info(f"Checking for missing trades: {venue} (incremental mode)")

        missing_dates = self.get_missing_dates(venue, market, source)

        if not missing_dates:
            logger.info(f"All available data already stored for {venue}")
            return {
                "dates_checked": [],
                "dates_fetched": [],
                "dates_partial": [],
                "total_trades": 0,
                "total_files": 0,
                "consolidated": False,
            }

        logger.info(
            f"Found {len(missing_dates)} missing dates for {venue}: {missing_dates}"
        )

        total_trades = 0
        total_files = 0
        dates_fetched = []
        dates_partial = []

        for date_str in missing_dates:
            try:
                logger.info(f"Fetching {venue} trades for {date_str} (incremental)")

                # Get list of files for this date
                files = self.list_files(venue, date_str)
                if not files:
                    logger.warning(f"No trade files found for {venue} on {date_str}")
                    continue

                logger.info(
                    f"Found {len(files)} files available from API for {venue} on {date_str}"
                )

                # Check which timestamps are already stored
                trade_date = datetime.strptime(date_str, "%Y-%m-%d")
                year = trade_date.year
                month = f"{trade_date.month:02d}"
                day = f"{trade_date.day:02d}"

                base_dir = (
                    self.backend._path_builder._root
                    / market
                    / source
                    / "trades"
                    / f"venue={venue}"
                    / f"year={year}"
                    / f"month={month}"
                    / f"day={day}"
                )
                parquet_path = base_dir / "trades.parquet"

                # Use centralized download log instead of per-day metadata
                download_log_path = (
                    self.backend._path_builder._root
                    / market
                    / source
                    / ".download_log.parquet"
                )

                # Track which timestamps have been downloaded (including empty files)
                already_downloaded_timestamps = set()

                # Check centralized download log
                if download_log_path.exists():
                    try:
                        import pandas as pd

                        # Read only rows for this venue and date
                        df_log = pd.read_parquet(download_log_path)
                        df_filtered = df_log[
                            (df_log["venue"] == venue) & (df_log["date"] == date_str)
                        ]

                        if len(df_filtered) > 0:
                            already_downloaded_timestamps = set(
                                df_filtered["timestamp"].unique()
                            )
                            logger.debug(
                                f"Found {len(already_downloaded_timestamps)} completed downloads from log for {date_str}"
                            )
                    except Exception as e:
                        logger.warning(
                            f"Could not read download log for {date_str}: {e}"
                        )

                # Also check parquet file for timestamps with actual trade data
                # (timestamps not yet tracked in download log)
                if parquet_path.exists():
                    try:
                        import pandas as pd

                        # Use pandas to avoid schema issues (it handles type conversions automatically)
                        df_existing = pd.read_parquet(
                            parquet_path, columns=["trade_time"]
                        )

                        if len(df_existing) > 0:
                            parquet_timestamps_before = len(
                                already_downloaded_timestamps
                            )
                            # Extract minute-level timestamps (YYYY-MM-DDTHH_MM format from filenames)
                            # Timestamps in data are like "2025-11-04 09:00:00.123456"
                            # Convert to "2025-11-04T09_00" format to match filenames
                            for ts in df_existing["trade_time"]:
                                # Format: YYYY-MM-DDTHH_MM
                                timestamp_str = ts.strftime("%Y-%m-%dT%H_%M")
                                already_downloaded_timestamps.add(timestamp_str)

                            parquet_timestamps_added = (
                                len(already_downloaded_timestamps)
                                - parquet_timestamps_before
                            )
                            if parquet_timestamps_added > 0:
                                logger.debug(
                                    f"Found {parquet_timestamps_added} additional timestamps from parquet for {date_str}"
                                )

                            logger.debug(
                                f"Total tracked timestamps: {len(already_downloaded_timestamps)} (download log + data timestamps)"
                            )
                    except Exception as e:
                        # Can't read parquet file - continue with what we have from log
                        logger.warning(
                            f"Could not read parquet timestamps for {date_str}: {e}"
                        )

                # Filter files to only those not yet downloaded
                files_to_fetch = []
                for filename in files:
                    # Extract timestamp from filename: "DETR-posttrade-2025-11-04T09_00.json.gz"
                    try:
                        timestamp_part = filename.split("DETR-posttrade-")[1].split(
                            ".json.gz"
                        )[0]  # "2025-11-04T09_00"
                        if timestamp_part not in already_downloaded_timestamps:
                            files_to_fetch.append(filename)
                    except IndexError:
                        # Can't parse filename, include it to be safe
                        files_to_fetch.append(filename)

                if not files_to_fetch:
                    logger.info(
                        f"All {len(files)} files already stored for {date_str}, skipping"
                    )
                    continue

                logger.info(
                    f"Need to fetch {len(files_to_fetch)}/{len(files)} files for {date_str}"
                )

                date_trades = 0
                date_files = 0
                completed_timestamps_this_run = []

                # Process each file individually - store immediately after each file
                for i, filename in enumerate(files_to_fetch, 1):
                    try:
                        # Fetch and parse single file
                        df = self.fetch_and_parse_trades(venue, date_str, filename)

                        if not df.empty:
                            # Store immediately (merge with existing data)
                            # This makes interruptions safe - worst case is losing current file
                            self.store_trades(df, venue, trade_date, market, source)

                            date_trades += len(df)
                            date_files += 1
                            total_trades += len(df)
                            total_files += 1
                        else:
                            # Empty file (no trades) - still count as processed
                            # This prevents re-downloading empty files on subsequent runs
                            date_files += 1
                            total_files += 1
                            logger.debug(
                                f"Processed empty file {filename} for {date_str}"
                            )

                        # Track this timestamp as completed (whether it had data or not)
                        try:
                            timestamp_part = filename.split("DETR-posttrade-")[1].split(
                                ".json.gz"
                            )[0]
                            completed_timestamps_this_run.append(
                                {
                                    "venue": venue,
                                    "date": date_str,
                                    "timestamp": timestamp_part,
                                    "has_data": not df.empty,
                                    "trade_count": len(df) if not df.empty else 0,
                                    "downloaded_at": datetime.now(),
                                }
                            )
                            already_downloaded_timestamps.add(timestamp_part)
                        except IndexError:
                            pass  # Can't parse filename

                        # Append to centralized download log every 10 files to enable resume
                        if (
                            i % 10 == 0 or i == len(files_to_fetch)
                        ) and completed_timestamps_this_run:
                            try:
                                import pandas as pd

                                # Create DataFrame from new log entries
                                df_new_log = pd.DataFrame(completed_timestamps_this_run)

                                # Append to existing log or create new one
                                log_dir = download_log_path.parent
                                log_dir.mkdir(parents=True, exist_ok=True)

                                if download_log_path.exists():
                                    # Append to existing log
                                    df_existing_log = pd.read_parquet(download_log_path)
                                    df_combined = pd.concat(
                                        [df_existing_log, df_new_log], ignore_index=True
                                    )
                                    # Remove duplicates (in case of retry)
                                    df_combined = df_combined.drop_duplicates(
                                        subset=["venue", "date", "timestamp"],
                                        keep="last",
                                    )
                                    df_combined.to_parquet(
                                        download_log_path, index=False
                                    )
                                else:
                                    # Create new log
                                    df_new_log.to_parquet(
                                        download_log_path, index=False
                                    )

                                # Clear the buffer after saving
                                completed_timestamps_this_run = []

                            except Exception as e:
                                logger.warning(f"Could not save download log: {e}")

                        if i % 50 == 0 or i == len(
                            files_to_fetch
                        ):  # Log every 50 files and at end
                            logger.info(
                                f"✓ [{i}/{len(files_to_fetch)}] Processed {date_files} files, "
                                f"{date_trades:,} trades for {date_str}"
                            )

                    except Exception as e:
                        logger.error(f"Failed to process {filename}: {e}")
                        # Continue with next file - partial progress is saved
                        continue

                # Check if we completed all files for this date
                if date_files == len(files):
                    dates_fetched.append(date_str)
                    logger.info(
                        f"✓ Completed {venue} {date_str}: {date_trades:,} trades from {date_files} files"
                    )

                    # Monthly consolidation: after successful date, consolidate month-to-date
                    if consolidate:
                        try:
                            trade_date = datetime.strptime(date_str, "%Y-%m-%d")
                            logger.info(
                                f"Consolidating month {trade_date.year}-{trade_date.month:02d} "
                                f"(includes {date_str})..."
                            )
                            self._consolidate_to_monthly(
                                venue, trade_date.year, trade_date.month, market, source
                            )
                        except Exception as e:
                            logger.error(f"Failed to consolidate month: {e}")
                            # Don't fail the whole process - data is still stored daily

                elif date_files > 0:
                    dates_partial.append(date_str)
                    logger.warning(
                        f"⚠ Partial completion {venue} {date_str}: {date_trades:,} trades "
                        f"from {date_files}/{len(files)} files (can resume)"
                    )

            except Exception as e:
                logger.error(f"Failed to fetch {venue} on {date_str}: {e}")
                continue

        summary = {
            "dates_checked": missing_dates,
            "dates_fetched": dates_fetched,
            "dates_partial": dates_partial,
            "total_trades": total_trades,
            "total_files": total_files,
            "consolidated": consolidate and len(dates_fetched) > 0,
        }

        logger.info(
            f"Summary for {venue}: {len(dates_fetched)} dates complete, "
            f"{total_trades:,} total trades from {total_files} files"
        )
        if dates_partial:
            logger.info(
                f"⚠ {len(dates_partial)} partial date(s) - re-run to resume: {dates_partial}"
            )

        return summary

    def _consolidate_to_monthly(
        self,
        venue: str,
        year: int,
        month: int,
        market: str = "de",
        source: str = "xetra",
    ) -> None:
        """
        Consolidate all daily parquet files for a month into a single optimized monthly file.

        Reads all date-partitioned files for the month, combines them, and writes to
        a single monthly parquet file with optimal compression. Daily files are kept
        as a safety backup (can be manually deleted after verification).

        Path strategy:
        - Daily files: {root}/{market}/{source}/trades/venue=X/year=Y/month=M/day=D/trades.parquet
        - Monthly file: {root}/{market}/{source}/trades_monthly/venue=X/year=Y/month=M/trades.parquet

        Args:
            venue: Venue code
            year: Year (e.g., 2025)
            month: Month (1-12)
            market: Market code
            source: Source code
        """
        import pyarrow.parquet as pq

        # Find all daily files for this month
        month_str = f"{month:02d}"
        daily_root = (
            self.backend._path_builder._root
            / market
            / source
            / "trades"
            / f"venue={venue}"
            / f"year={year}"
            / f"month={month_str}"
        )

        if not daily_root.exists():
            logger.warning(f"No data found for {venue} {year}-{month_str}")
            return

        # Collect all daily parquet files
        daily_files = sorted(daily_root.rglob("trades.parquet"))
        if not daily_files:
            logger.warning(f"No daily files found for {venue} {year}-{month_str}")
            return

        logger.info(
            f"Consolidating {len(daily_files)} daily files for {venue} {year}-{month_str}"
        )

        # Read and combine all daily files
        daily_dfs = []
        total_trades = 0
        for daily_file in daily_files:
            try:
                df = pd.read_parquet(daily_file)
                daily_dfs.append(df)
                total_trades += len(df)
                logger.debug(f"Read {len(df):,} trades from {daily_file.name}")
            except Exception as e:
                logger.error(f"Failed to read {daily_file}: {e}")
                continue

        if not daily_dfs:
            logger.error(f"No data could be read for {venue} {year}-{month_str}")
            return

        # Combine all monthly data
        monthly_df = pd.concat(daily_dfs, ignore_index=True)

        # Sort by timestamp for optimal query performance
        if "time" in monthly_df.columns:
            monthly_df = monthly_df.sort_values("time")

        # Write to monthly consolidated file
        monthly_root = (
            self.backend._path_builder._root
            / market
            / source
            / "trades_monthly"
            / f"venue={venue}"
            / f"year={year}"
            / f"month={month_str}"
        )
        monthly_root.mkdir(parents=True, exist_ok=True)
        monthly_file = monthly_root / "trades.parquet"

        # Use same atomic write pattern as backend
        temp_file = monthly_file.with_suffix(".tmp")
        try:
            import pyarrow as pa
            import os
            import shutil

            table = pa.Table.from_pandas(monthly_df)
            pq.write_table(
                table,
                str(temp_file),
                compression="gzip",
                row_group_size=100000,  # Optimize for ~100K rows per group
            )
            with open(temp_file, "rb") as fd:
                os.fsync(fd.fileno())
            shutil.move(str(temp_file), str(monthly_file))

            logger.info(
                f"✓ Consolidated to monthly: {monthly_file.name} "
                f"({total_trades:,} trades, {len(monthly_df['isin'].unique())} unique ISINs)"
            )
        except Exception as e:
            logger.error(f"Failed to write monthly file: {e}")
            if temp_file.exists():
                temp_file.unlink()
            raise

    def check_partial_downloads(
        self, venue: str, market: str = "de", source: str = "xetra"
    ) -> dict:
        """
        Check for dates with partial/incomplete downloads by counting daily files.

        A complete date should have ~1142 files downloaded. If a date has fewer files
        stored, it's likely an interrupted download that can be resumed.

        Args:
            venue: Venue code
            market: Market code
            source: Source code

        Returns:
            Dictionary with analysis:
            {
                'complete_dates': ['2025-11-01', '2025-11-02'],
                'partial_dates': [
                    {'date': '2025-11-04', 'files_stored': 181, 'expected': ~1142}
                ],
                'months_ready': [(2025, 11)],  # Months with all dates complete
            }
        """

        daily_root = (
            self.backend._path_builder._root
            / market
            / source
            / "trades"
            / f"venue={venue}"
        )

        if not daily_root.exists():
            return {
                "complete_dates": [],
                "partial_dates": [],
                "months_ready": [],
            }

        # Scan all year/month/day directories
        complete_dates = []
        partial_dates = []
        dates_by_month = {}  # Track dates per month for consolidation readiness

        for year_dir in sorted(daily_root.glob("year=*")):
            year = int(year_dir.name.split("=")[1])

            for month_dir in sorted(year_dir.glob("month=*")):
                month = int(month_dir.name.split("=")[1])
                month_key = (year, month)
                dates_by_month[month_key] = []

                for day_dir in sorted(month_dir.glob("day=*")):
                    day = int(day_dir.name.split("=")[1])
                    date_str = f"{year}-{month:02d}-{day:02d}"

                    # Count parquet files (incremental storage creates many temp files)
                    parquet_files = list(day_dir.glob("trades.parquet"))

                    if parquet_files:
                        # Has data - check if it looks complete
                        # We can't know exact expected count without calling API,
                        # but we can mark dates with data
                        complete_dates.append(date_str)
                        dates_by_month[month_key].append(date_str)
                    else:
                        # Has directory but no parquet - likely interrupted
                        partial_dates.append(
                            {
                                "date": date_str,
                                "files_stored": 0,
                                "status": "empty_directory",
                            }
                        )

        # Determine which months are ready for consolidation
        # (have at least some dates - user can manually verify completeness)
        months_ready = [
            month_key for month_key, dates in dates_by_month.items() if len(dates) > 0
        ]

        return {
            "complete_dates": sorted(complete_dates),
            "partial_dates": partial_dates,
            "months_ready": sorted(months_ready),
        }

    def fetch_and_store_missing_trades(
        self, venue: str, market: str = "de", source: str = "xetra"
    ) -> dict:
        """
        Automatically fetch and store any missing trade data for a venue.

        This is the smart entry point that handles everything:
        1. Determines what dates are available from Xetra API
        2. Checks what's already stored locally
        3. Fetches and stores only the missing dates

        Args:
            venue: Venue code ('DETR', 'DFRA', 'DGAT', 'DEUR')
            market: Market code (default: 'de')
            source: Source code (default: 'xetra')

        Returns:
            Dictionary with summary statistics:
            {
                'dates_checked': ['2025-11-04', '2025-11-03'],
                'dates_fetched': ['2025-11-04'],
                'dates_skipped': ['2025-11-03'],
                'total_trades': 1500,
                'total_isins': 250
            }

        Example:
            >>> service = XetraService()
            >>> summary = service.fetch_and_store_missing_trades('DETR')
            >>> print(f"Fetched {summary['total_trades']} trades")
            Fetched 1500 trades
        """
        logger.info(f"Checking for missing trades: {venue}")

        missing_dates = self.get_missing_dates(venue, market, source)

        if not missing_dates:
            logger.info(f"All available data already stored for {venue}")
            return {
                "dates_checked": [],
                "dates_fetched": [],
                "dates_skipped": [],
                "total_trades": 0,
                "total_isins": 0,
            }

        logger.info(
            f"Found {len(missing_dates)} missing dates for {venue}: {missing_dates}"
        )

        total_trades = 0
        total_isins_set = set()
        dates_fetched = []

        for date_str in missing_dates:
            try:
                logger.info(f"Fetching {venue} trades for {date_str}")
                df = self.fetch_all_trades_for_date(venue, date_str)

                if df.empty:
                    logger.warning(f"No trades found for {venue} on {date_str}")
                    continue

                # Store the data
                trade_date = datetime.strptime(date_str, "%Y-%m-%d")
                self.store_trades(df, venue, trade_date, market, source)

                # Track statistics
                total_trades += len(df)
                total_isins_set.update(df["isin"].unique())
                dates_fetched.append(date_str)

                logger.info(
                    f"✓ Stored {len(df)} trades for {venue} on {date_str} "
                    f"({len(df['isin'].unique())} unique ISINs)"
                )

            except Exception as e:
                logger.error(f"Failed to fetch/store {venue} on {date_str}: {e}")
                continue

        summary = {
            "dates_checked": missing_dates,
            "dates_fetched": dates_fetched,
            "dates_skipped": [d for d in missing_dates if d not in dates_fetched],
            "total_trades": total_trades,
            "total_isins": len(total_isins_set),
        }

        logger.info(
            f"Summary for {venue}: {len(dates_fetched)}/{len(missing_dates)} dates fetched, "
            f"{total_trades} total trades, {len(total_isins_set)} unique ISINs"
        )

        return summary

    def store_trades(
        self,
        df: pd.DataFrame,
        venue: str,
        trade_date: datetime,
        market: str = "de",
        source: str = "xetra",
    ) -> None:
        """
        Store trades to partitioned parquet storage.

        Args:
            df: DataFrame with trade data
            venue: Venue code (DETR, DFRA, etc.)
            trade_date: Trade date
            market: Market code (default: 'de' for Germany)
            source: Source code (default: 'xetra')

        Example:
            >>> service = XetraService()
            >>> df = service.fetch_all_trades_for_date('DETR', '2025-10-31')
            >>> service.store_trades(df, 'DETR', datetime(2025, 10, 31))
        """
        if df.empty:
            logger.warning("No trades to store (empty DataFrame)")
            return

        self.backend.save_xetra_trades(df, venue, trade_date, market, source)
        logger.debug(
            f"Stored {len(df)} trades for {venue} on {trade_date.date()} "
            f"({len(df['isin'].unique())} unique ISINs)"
        )

    def close(self) -> None:
        """Close HTTP client resources."""
        self.fetcher.close()

    def __enter__(self):
        """Context manager support."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup."""
        self.close()
