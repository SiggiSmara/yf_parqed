from datetime import datetime
from typing import List, Optional
from pathlib import Path
import gc
import os
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from ..common.partitioned_storage_backend import PartitionedStorageBackend
from ..common.partition_path_builder import PartitionPathBuilder
from .xetra_fetcher import XetraFetcher
from .xetra_parser import XetraParser
from .exceptions import XetraSchemaUnknownError
from ..common.config_service import ConfigService


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
        self.root_path = root_path or Path("data")

        # Default backend configuration for Xetra trades
        if backend is None:
            path_builder = PartitionPathBuilder(root=self.root_path)
            self.backend = PartitionedStorageBackend(
                empty_frame_factory=lambda: pd.DataFrame(),
                normalizer=lambda df: df,
                column_provider=lambda: [],
                path_builder=path_builder,
            )
        else:
            self.backend = backend

    def has_any_data(self, venue: str, market: str = "de", source: str = "xetra") -> bool:
        """
        Check if any data exists for the specified venue.

        Args:
            venue: Venue code ('DETR', 'DFRA', 'DGAT', 'DEUR')
            market: Market code (default: 'de')
            source: Source code (default: 'xetra')

        Returns:
            True if any parquet files exist for this venue, False otherwise
        """
        # Check if venue directory exists and has any parquet files
        venue_dir = (
            self.backend._path_builder._root
            / market
            / source
            / "trades"
            / f"venue={venue}"
        )
        
        if not venue_dir.exists():
            return False
        
        # Check if any parquet files exist in the venue directory tree
        parquet_files = list(venue_dir.rglob("*.parquet"))
        return len(parquet_files) > 0

    # --- Raw cache helpers ---

    def _raw_cache_path(
        self,
        venue: str,
        date_str: str,
        filename: str,
        market: str = "de",
        source: str = "xetra",
    ) -> Path:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        return (
            self.root_path
            / market
            / source
            / "raw"
            / venue
            / f"year={d.year}"
            / f"month={d.month:02d}"
            / f"day={d.day:02d}"
            / filename
        )

    def _save_to_raw_cache(
        self,
        compressed_data: bytes,
        venue: str,
        date_str: str,
        filename: str,
        market: str = "de",
        source: str = "xetra",
    ) -> Path:
        cache_path = self._raw_cache_path(venue, date_str, filename, market, source)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = cache_path.with_name(cache_path.name + ".tmp")
        try:
            tmp_path.write_bytes(compressed_data)
            tmp_path.rename(cache_path)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise
        return cache_path

    def _is_cached(
        self,
        venue: str,
        date_str: str,
        filename: str,
        market: str = "de",
        source: str = "xetra",
    ) -> bool:
        return self._raw_cache_path(venue, date_str, filename, market, source).exists()

    def _is_parquet_readable(self, path: Path) -> bool:
        try:
            pq.read_metadata(str(path))
            pd.read_parquet(path, columns=[])
            return True
        except Exception:
            return False

    def cleanup_raw_cache(
        self,
        venue: str,
        max_age_days: int = 7,
        market: str = "de",
        source: str = "xetra",
        dry_run: bool = False,
    ) -> dict:
        """
        Delete raw cache files older than max_age_days when a readable Parquet confirms the data.

        Returns dict with keys: deleted, kept_recent, kept_no_parquet, errors.
        """
        raw_dir = self.root_path / market / source / "raw" / venue
        if not raw_dir.exists():
            return {"deleted": 0, "kept_recent": 0, "kept_no_parquet": 0, "errors": 0}

        now = time.time()
        ttl = max_age_days * 86400
        deleted = kept_recent = kept_no_parquet = errors = 0

        # Remove orphaned .tmp files unconditionally
        for tmp_path in raw_dir.rglob("*.json.gz.tmp"):
            try:
                if not dry_run:
                    tmp_path.unlink(missing_ok=True)
                deleted += 1
            except Exception as e:
                logger.warning(f"Failed to remove orphaned tmp {tmp_path}: {e}")
                errors += 1

        for cache_file in raw_dir.rglob("*.json.gz"):
            try:
                if now - cache_file.stat().st_mtime < ttl:
                    kept_recent += 1
                    continue

                parts = cache_file.parent.parts
                year_part = next((p for p in parts if p.startswith("year=")), None)
                month_part = next((p for p in parts if p.startswith("month=")), None)
                day_part = next((p for p in parts if p.startswith("day=")), None)

                if not (year_part and month_part and day_part):
                    logger.warning(f"Cannot parse date from raw cache path {cache_file}")
                    kept_no_parquet += 1
                    continue

                year = year_part.split("=")[1]
                month = month_part.split("=")[1]
                day = day_part.split("=")[1]

                daily_path = (
                    self.root_path / market / source / "trades"
                    / f"venue={venue}" / f"year={year}" / f"month={month}" / f"day={day}"
                    / "trades.parquet"
                )
                monthly_path = (
                    self.root_path / market / source / "trades_monthly"
                    / f"venue={venue}" / f"year={year}" / f"month={month}"
                    / "trades.parquet"
                )

                if self._is_parquet_readable(daily_path) or self._is_parquet_readable(monthly_path):
                    if not dry_run:
                        cache_file.unlink(missing_ok=True)
                    deleted += 1
                    logger.debug(f"Cleaned up aged raw cache file {cache_file.name}")
                else:
                    kept_no_parquet += 1
                    logger.warning(
                        f"Raw cache {cache_file.name} is >{max_age_days}d old "
                        f"but no readable Parquet found — keeping"
                    )
            except Exception as e:
                logger.warning(f"Error processing raw cache file {cache_file}: {e}")
                errors += 1

        label = " [DRY RUN]" if dry_run else ""
        logger.info(
            f"Raw cache cleanup {venue}{label}: "
            f"deleted={deleted}, kept_recent={kept_recent}, "
            f"kept_no_parquet={kept_no_parquet}, errors={errors}"
        )
        return {
            "deleted": deleted,
            "kept_recent": kept_recent,
            "kept_no_parquet": kept_no_parquet,
            "errors": errors,
        }

    def reprocess_from_raw_cache(
        self,
        venue: str,
        date_str: str,
        market: str = "de",
        source: str = "xetra",
        force: bool = False,
    ) -> dict:
        """
        Rebuild the daily Parquet for venue/date from raw .json.gz cache files.

        If a readable daily Parquet already exists and force=False, returns early.
        Unknown-schema files are logged as errors but do not abort the run.

        Returns dict with keys: processed, trades, skipped_unknown_schema, errors.
        """
        trade_date = datetime.strptime(date_str, "%Y-%m-%d")
        cache_dir = (
            self.root_path / market / source / "raw" / venue
            / f"year={trade_date.year}" / f"month={trade_date.month:02d}"
            / f"day={trade_date.day:02d}"
        )
        if not cache_dir.exists():
            raise FileNotFoundError(f"No raw cache for {venue} {date_str} at {cache_dir}")

        if not force:
            daily_path = (
                self.root_path / market / source / "trades"
                / f"venue={venue}" / f"year={trade_date.year}"
                / f"month={trade_date.month:02d}" / f"day={trade_date.day:02d}"
                / "trades.parquet"
            )
            if self._is_parquet_readable(daily_path):
                logger.info(
                    f"Readable Parquet already exists for {venue} {date_str}; "
                    f"use force=True to reprocess anyway"
                )
                return {"processed": 0, "trades": 0, "skipped_unknown_schema": 0, "errors": 0}

        raw_files = sorted(cache_dir.glob("*.json.gz"))
        if not raw_files:
            raise FileNotFoundError(f"Raw cache directory exists but is empty: {cache_dir}")

        processed = trades = skipped_unknown_schema = errors = 0

        for raw_file in raw_files:
            try:
                compressed_data = raw_file.read_bytes()
                json_str = self.fetcher.decompress_gzip(compressed_data)
                df = self.parser.parse(json_str)
                if not df.empty:
                    self.store_trades(df, venue, trade_date, market, source)
                    trades += len(df)
                processed += 1
            except XetraSchemaUnknownError as e:
                logger.error(
                    f"Unknown schema in {raw_file.name}: {sorted(e.actual_fields)}"
                )
                skipped_unknown_schema += 1
            except Exception as e:
                logger.error(f"Failed to reprocess {raw_file.name}: {e}")
                errors += 1

        logger.info(
            f"Reprocess {venue} {date_str}: {processed} files, {trades} trades, "
            f"{skipped_unknown_schema} unknown-schema, {errors} errors"
        )

        try:
            self._consolidate_daily_files(venue, date_str, market, source)
        except Exception as e:
            logger.error(f"Failed to consolidate daily files for {venue} {date_str}: {e}")

        return {
            "processed": processed,
            "trades": trades,
            "skipped_unknown_schema": skipped_unknown_schema,
            "errors": errors,
        }

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

        logger.info(
            f"Will check {len(available_dates)} dates for {venue}: {available_dates}"
        )
        return available_dates

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

        # Save raw bytes before parsing — file is preserved even on parse failure or SIGKILL
        try:
            self._save_to_raw_cache(compressed_data, venue, date, filename)
        except Exception as e:
            logger.warning(f"Failed to cache {filename}: {e}")

        # Decompress
        json_str = self.fetcher.decompress_gzip(compressed_data)

        # Parse to DataFrame
        try:
            df = self.parser.parse(json_str)
        except XetraSchemaUnknownError as e:
            logger.error(
                f"Unknown schema in {filename}: raw file is in raw cache. "
                f"Fields received: {sorted(e.actual_fields)}"
            )
            raise

        isin_count = df["isin"].nunique() if "isin" in df.columns else 0
        logger.debug(
            f"Parsed {len(df)} trades from {filename} ({isin_count} unique ISINs)"
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
            except XetraSchemaUnknownError:
                raise  # All files share the same schema — no point continuing
            except Exception as e:
                logger.error(f"Failed to process {filename}: {e}")
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
        last_processed_month: tuple | None = None

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

                trade_date = datetime.strptime(date_str, "%Y-%m-%d")
                this_month = (trade_date.year, trade_date.month)

                # H: consolidate previous month when month rolls over
                if consolidate and last_processed_month is not None and this_month != last_processed_month:
                    py, pm = last_processed_month
                    try:
                        logger.info(
                            f"Month rolled over {py}-{pm:02d} → "
                            f"{this_month[0]}-{this_month[1]:02d}, consolidating"
                        )
                        self._consolidate_to_monthly(venue, py, pm, market, source)
                    except Exception as e:
                        logger.error(f"Failed to consolidate {py}-{pm:02d}: {e}")

                files_to_fetch = [
                    f for f in files if not self._is_cached(venue, date_str, f, market, source)
                ]

                if not files_to_fetch:
                    self._consolidate_daily_files(venue, date_str, market, source)
                    logger.info(
                        f"All {len(files)} files already cached for {date_str}, skipping"
                    )
                    last_processed_month = this_month
                    continue

                logger.info(
                    f"Need to fetch {len(files_to_fetch)}/{len(files)} files for {date_str}"
                )

                date_trades = 0
                date_files = 0

                # Process each file individually - store immediately after each file
                for i, filename in enumerate(files_to_fetch, 1):
                    try:
                        # Fetch and parse single file
                        df = self.fetch_and_parse_trades(venue, date_str, filename)

                        if not df.empty:
                            self.store_trades(df, venue, trade_date, market, source)

                            date_trades += len(df)
                            date_files += 1
                            total_trades += len(df)
                            total_files += 1
                        else:
                            # Empty file (no trades) - still count as processed
                            date_files += 1
                            total_files += 1
                            logger.debug(
                                f"Processed empty file {filename} for {date_str}"
                            )

                        if i % 50 == 0 or i == len(files_to_fetch):
                            logger.info(
                                f"✓ [{i}/{len(files_to_fetch)}] Processed {date_files} files, "
                                f"{date_trades:,} trades for {date_str}"
                            )

                    except Exception as e:
                        logger.error(f"Failed to process {filename}: {e}")
                        continue

                # Check if we completed all files for this date
                if date_files == len(files):
                    dates_fetched.append(date_str)
                    logger.info(
                        f"✓ Completed {venue} {date_str}: {date_trades:,} trades from {date_files} files"
                    )
                    try:
                        self._consolidate_daily_files(venue, date_str, market, source)
                    except Exception as e:
                        logger.error(f"Failed to consolidate daily files for {date_str}: {e}")

                elif date_files > 0:
                    dates_partial.append(date_str)
                    logger.warning(
                        f"⚠ Partial completion {venue} {date_str}: {date_trades:,} trades "
                        f"from {date_files}/{len(files)} files (can resume)"
                    )

                last_processed_month = this_month

            except Exception as e:
                logger.error(f"Failed to fetch {venue} on {date_str}: {e}")
                continue

        # H: consolidate last processed month if it's a fully past month
        if consolidate and last_processed_month is not None:
            now = datetime.now()
            py, pm = last_processed_month
            if (py, pm) < (now.year, now.month):
                try:
                    logger.info(f"Consolidating last processed month {py}-{pm:02d}")
                    self._consolidate_to_monthly(venue, py, pm, market, source)
                except Exception as e:
                    logger.error(f"Failed to consolidate {py}-{pm:02d}: {e}")

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

    def _consolidate_daily_files(
        self,
        venue: str,
        date_str: str,
        market: str = "de",
        source: str = "xetra",
    ) -> None:
        """
        Merge per-call mini-Parquets into a single daily trades.parquet.

        If trades.parquet already exists (crash-during-cleanup scenario), the
        mini-files are stale and are deleted without re-reading them.
        """
        d = datetime.strptime(date_str, "%Y-%m-%d")
        daily_dir = (
            self.root_path / market / source / "trades"
            / f"venue={venue}"
            / f"year={d.year}"
            / f"month={d.month:02d}"
            / f"day={d.day:02d}"
        )
        if not daily_dir.exists():
            return

        final_path = daily_dir / "trades.parquet"
        mini_files = sorted(daily_dir.glob("trades-*.parquet"))

        if not mini_files:
            return

        if final_path.exists():
            for mini in mini_files:
                mini.unlink(missing_ok=True)
            logger.debug(f"Cleaned {len(mini_files)} stale mini-files for {venue} {date_str}")
            return

        tables = []
        for mini in mini_files:
            try:
                tables.append(pq.read_table(str(mini)))
            except Exception as e:
                logger.warning(f"Skipping unreadable mini-file {mini.name}: {e}")

        if not tables:
            return

        combined = pa.concat_tables(tables)
        tmp_path = final_path.with_name("trades.parquet.tmp")
        tmp_path.unlink(missing_ok=True)
        try:
            pq.write_table(combined, str(tmp_path), use_dictionary=False, compression="gzip")
            with open(tmp_path, "rb") as fd:
                os.fsync(fd.fileno())
            tmp_path.replace(final_path)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

        for mini in mini_files:
            mini.unlink(missing_ok=True)
        logger.info(
            f"Consolidated {len(mini_files)} mini-files → trades.parquet "
            f"for {venue} {date_str} ({len(combined)} rows)"
        )

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
            table = pa.Table.from_pandas(monthly_df)
            pq.write_table(
                table,
                str(temp_file),
                compression="gzip",
                row_group_size=100000,
            )
            with open(temp_file, "rb") as fd:
                os.fsync(fd.fileno())
            temp_file.replace(monthly_file)

            logger.info(
                f"✓ Consolidated to monthly: {monthly_file.name} "
                f"({total_trades:,} trades, "
                f"{monthly_df['isin'].nunique() if 'isin' in monthly_df.columns else 0} unique ISINs)"
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

                    # Count parquet files (trades.parquet = consolidated; trades-*.parquet = in-progress)
                    has_data = (day_dir / "trades.parquet").exists() or any(
                        day_dir.glob("trades-*.parquet")
                    )

                    if has_data:
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

        Writes a uniquely-named mini-file per call; caller must invoke
        _consolidate_daily_files() at end-of-date to produce trades.parquet.

        Args:
            df: DataFrame with trade data
            venue: Venue code (DETR, DFRA, etc.)
            trade_date: Trade date
            market: Market code (default: 'de' for Germany)
            source: Source code (default: 'xetra')
        """
        if df.empty:
            logger.warning("No trades to store (empty DataFrame)")
            return

        d = trade_date if isinstance(trade_date, datetime) else datetime.strptime(str(trade_date), "%Y-%m-%d")
        daily_dir = (
            self.root_path / market / source / "trades"
            / f"venue={venue}"
            / f"year={d.year}"
            / f"month={d.month:02d}"
            / f"day={d.day:02d}"
        )
        daily_dir.mkdir(parents=True, exist_ok=True)

        mini_name = f"trades-{os.getpid()}-{time.time_ns()}.parquet"
        mini_path = daily_dir / mini_name
        tmp_path = mini_path.with_suffix(".tmp")
        try:
            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_table(table, str(tmp_path), use_dictionary=False, compression="gzip")
            with open(tmp_path, "rb") as fd:
                os.fsync(fd.fileno())
            tmp_path.replace(mini_path)
            logger.debug(f"Staged {len(df)} trades → {mini_name}")
        except Exception as e:
            tmp_path.unlink(missing_ok=True)
            logger.warning(f"Failed to stage trades for {venue} {d.date()}: {e}")
            raise

    # ------------------------------------------------------------------
    # Schema migration helpers
    # ------------------------------------------------------------------

    #: Column renames required to upgrade 2025-legacy Parquet files to the
    #: MiFIR column names used by the current parser.
    LEGACY_COLUMN_RENAMES: dict[str, str] = {
        "trans_id": "transaction_id",
        "volume": "quantity",
        "currency": "price_currency",
        "trade_time": "trading_date_time",
        "venue": "execution_venue",
    }

    def _migration_sentinel_path(
        self, venue: str, market: str = "de", source: str = "xetra"
    ) -> Path:
        return self.root_path / market / source / "trades" / f"venue={venue}" / ".migration_complete"

    def find_unmigrated_files(
        self,
        venue: str,
        market: str = "de",
        source: str = "xetra",
    ) -> list[Path]:
        """
        Return paths of Parquet files that still use the 2025-legacy column names.

        Detection is fast: reads only Parquet file metadata (no row data).
        A file is considered unmigrated if it lacks a 'schema_version' column.
        Returns [] immediately when the migration-complete sentinel exists.
        """
        if self._migration_sentinel_path(venue, market, source).exists():
            logger.debug(f"Migration sentinel present for {venue} — skipping scan")
            return []

        base = self.root_path / market / source
        patterns = [
            f"trades/venue={venue}/year=*/month=*/day=*/trades.parquet",
            f"trades_monthly/venue={venue}/year=*/month=*/trades.parquet",
        ]
        unmigrated = []
        for pattern in patterns:
            for path in sorted(base.glob(pattern)):
                try:
                    schema = pq.read_schema(path)
                    if "schema_version" not in schema.names:
                        unmigrated.append(path)
                except Exception as e:
                    logger.warning(f"Could not read schema of {path}: {e}")

        if not unmigrated:
            # All files already migrated (or no files yet). Write sentinel so
            # future calls skip the scan — handles daemons migrated before this
            # change was deployed.
            sentinel = self._migration_sentinel_path(venue, market, source)
            if not sentinel.exists():
                sentinel.parent.mkdir(parents=True, exist_ok=True)
                sentinel.touch()
                logger.info(f"All files migrated for {venue} — wrote sentinel {sentinel}")

        return unmigrated

    def migrate_legacy_columns(
        self,
        venue: str,
        market: str = "de",
        source: str = "xetra",
        dry_run: bool = False,
    ) -> dict:
        """
        Rename 2025-legacy columns to MiFIR names and add schema_version column.

        Writes atomically (temp file → os.replace). Idempotent: files that
        already have 'schema_version' are skipped without modification.

        Returns a summary dict with keys: migrated, skipped, failed, paths_migrated.
        """
        unmigrated = self.find_unmigrated_files(venue, market, source)

        summary = {"migrated": 0, "skipped": 0, "failed": 0, "paths_migrated": []}

        if not unmigrated:
            logger.info(f"No unmigrated files found for {venue} — already up to date.")
            if not dry_run:
                sentinel = self._migration_sentinel_path(venue, market, source)
                sentinel.parent.mkdir(parents=True, exist_ok=True)
                sentinel.touch()
                logger.info(f"Wrote migration sentinel: {sentinel}")
            return summary

        logger.info(
            f"{'[DRY RUN] ' if dry_run else ''}Found {len(unmigrated)} unmigrated "
            f"file(s) for {venue}."
        )

        for path in unmigrated:
            try:
                if dry_run:
                    logger.info(f"[DRY RUN] Would migrate: {path}")
                    summary["migrated"] += 1
                    summary["paths_migrated"].append(str(path))
                    continue

                # Read one row group at a time so we never ask PyArrow to merge
                # row groups with incompatible column encodings (string vs
                # dictionary<string>). Each group is cast to plain string before
                # being written, keeping memory proportional to one row group.
                reader = pq.ParquetFile(path)
                tmp_path = path.with_suffix(".parquet.tmp")
                writer = None
                try:
                    for i in range(reader.metadata.num_row_groups):
                        rg = reader.read_row_group(i)
                        new_cols = {
                            field.name: (
                                rg.column(field.name).cast(
                                    rg.column(field.name).type.value_type
                                )
                                if pa.types.is_dictionary(rg.column(field.name).type)
                                else rg.column(field.name)
                            )
                            for field in rg.schema
                        }
                        rg = pa.table(new_cols)
                        new_names = [
                            self.LEGACY_COLUMN_RENAMES.get(name, name)
                            for name in rg.schema.names
                        ]
                        rg = rg.rename_columns(new_names)
                        rg = rg.append_column(
                            "schema_version",
                            pa.array(["2025-legacy"] * len(rg), type=pa.string()),
                        )
                        if writer is None:
                            writer = pq.ParquetWriter(
                                tmp_path, rg.schema, use_dictionary=False
                            )
                        writer.write_table(rg)
                    if writer is not None:
                        writer.close()
                        writer = None
                except Exception:
                    if writer is not None:
                        try:
                            writer.close()
                        except Exception:
                            pass
                    tmp_path.unlink(missing_ok=True)
                    raise
                os.replace(tmp_path, path)
                gc.collect()

                logger.info(f"Migrated: {path}")
                summary["migrated"] += 1
                summary["paths_migrated"].append(str(path))

            except Exception as e:
                logger.error(f"Failed to migrate {path}: {e}")
                summary["failed"] += 1

        logger.info(
            f"{'[DRY RUN] ' if dry_run else ''}Migration complete — "
            f"{summary['migrated']} migrated, {summary['skipped']} skipped, "
            f"{summary['failed']} failed."
        )
        if not dry_run and summary["failed"] == 0:
            sentinel = self._migration_sentinel_path(venue, market, source)
            sentinel.parent.mkdir(parents=True, exist_ok=True)
            sentinel.touch()
            logger.info(f"Wrote migration sentinel: {sentinel}")
        return summary

    def close(self) -> None:
        """Close HTTP client resources."""
        self.fetcher.close()

    def __enter__(self):
        """Context manager support."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup."""
        self.close()
