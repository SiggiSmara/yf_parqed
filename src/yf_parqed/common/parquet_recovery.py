"""Shared parquet file recovery logic for all storage backends.

This module provides unified recovery strategies for parquet files with schema issues.
Only truly corrupt/unreadable files are deleted; files with schema mismatches are
preserved for operator inspection while clear errors are raised.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import pandas as pd
from loguru import logger


class ParquetRecoveryError(Exception):
    """Raised when a parquet file cannot be recovered through safe transformations."""

    pass


def safe_read_parquet(
    path: Path,
    required_columns: set[str],
    normalizer: Callable[[pd.DataFrame], pd.DataFrame],
    empty_frame_factory: Callable[[], pd.DataFrame],
) -> pd.DataFrame:
    """
    Read a parquet file with comprehensive recovery strategies.

    This function implements a multi-stage recovery process:
    1. Attempt to read the file
    2. If successful but empty, raise ParquetRecoveryError (preserve file)
    3. If missing required columns, attempt safe promotions
    4. If recovery fails, raise ParquetRecoveryError (preserve file)
    5. If file is truly corrupt/unreadable, delete it and raise ParquetRecoveryError

    Args:
        path: Path to the parquet file
        required_columns: Set of column names that must be present
        normalizer: Function to normalize DataFrame types/columns
        empty_frame_factory: Function to create an empty DataFrame with correct schema

    Returns:
        Normalized DataFrame if successful

    Raises:
        ParquetRecoveryError: If file cannot be recovered (with details about why)
    """
    # Stage 1: Attempt to read the file
    try:
        df = pd.read_parquet(path)
    except (ValueError, FileNotFoundError, OSError) as exc:
        # File is truly corrupt/unreadable - safe to delete
        logger.warning(
            f"Unable to read parquet file {path.name}: {exc}. Deleting corrupt file."
        )
        _safe_remove(path)
        raise ParquetRecoveryError(
            f"Parquet file {path} is corrupt and unreadable. File has been deleted."
        ) from exc

    # Stage 2: Check for empty DataFrame
    if df.empty:
        logger.warning(
            f"Read empty DataFrame from {path.name}. File preserved for inspection."
        )
        raise ParquetRecoveryError(
            f"Parquet file {path} contains no data. File preserved for operator inspection."
        )

    # Stage 3: Check for missing required columns
    if not required_columns.issubset(df.columns):
        logger.debug(f"Missing columns in {path.name}. Attempting recovery...")
        df = _attempt_column_recovery(df, required_columns, path)

        # If still missing columns after recovery, preserve file and fail
        if not required_columns.issubset(df.columns):
            missing = required_columns - set(df.columns)
            logger.warning(
                f"Cannot recover {path.name}: missing columns {missing}. "
                f"File preserved for inspection."
            )
            raise ParquetRecoveryError(
                f"Parquet file {path} is missing required columns: {missing}. "
                f"Found columns: {set(df.columns)}. File preserved for operator inspection."
            )

    # Stage 4: Normalize and return
    try:
        df = normalizer(df)
        return df
    except Exception as exc:
        logger.warning(
            f"Normalization failed for {path.name}: {exc}. File preserved for inspection."
        )
        raise ParquetRecoveryError(
            f"Parquet file {path} normalization failed: {exc}. "
            f"File preserved for operator inspection."
        ) from exc


def _attempt_column_recovery(
    df: pd.DataFrame, required_columns: set[str], path: Path
) -> pd.DataFrame:
    """
    Attempt to recover missing columns through safe transformations.

    Recovery strategies (in order):
    1. Promote numeric, monotonic index to 'sequence' column
    2. Promote 'index' column to 'sequence' if safe
    3. Return DataFrame as-is if no safe recovery possible

    Args:
        df: DataFrame with potentially missing columns
        required_columns: Set of required column names
        path: Path to the file (for logging)

    Returns:
        DataFrame after attempted recovery
    """
    promoted = False

    # Strategy 1: Promote index to 'sequence' if it's numeric and monotonic
    # BUT: Skip if there's an 'index' column with datetime dtype (from reset_index on DatetimeIndex)
    # We want Strategy 2 to handle (and reject) datetime columns
    if "sequence" not in df.columns and not df.index.empty:
        # Check if there's a datetime 'index' column - skip Strategy 1 if so
        if "index" in df.columns and pd.api.types.is_datetime64_any_dtype(df["index"]):
            logger.debug(
                f"Skipping Strategy 1: 'index' column has datetime dtype in {path.name}"
            )
        else:
            idx = df.index

            # Don't promote datetime-like indexes
            try:
                if pd.api.types.is_datetime64_any_dtype(idx):
                    raise ValueError("datetime index: do not promote to sequence")
            except Exception:
                idx_is_datetime = True
            else:
                idx_is_datetime = False

            if not idx_is_datetime:
                try:
                    # Convert index values to numeric (coerce non-numeric -> NaN)
                    numeric = pd.to_numeric(pd.Series(idx), errors="coerce")

                    # Detect integer-encoded datetimes (e.g., ns since epoch stored as int)
                    # Only flag as epoch if the datetime falls in a reasonable range (year 2000+)
                    is_epoch_like = False
                    try:
                        dt = pd.to_datetime(numeric, errors="coerce")
                        if not dt.isnull().any():
                            # Check if dates are in reasonable range (after year 2000)
                            year_2000 = pd.Timestamp("2000-01-01")
                            if (dt >= year_2000).all() and (
                                dt.astype("int64") == numeric.astype("int64")
                            ).all():
                                is_epoch_like = True
                    except Exception:
                        is_epoch_like = False

                    # Promote if numeric and not epoch-like
                    # (monotonic check removed - handles both single values and sequences)
                    if not numeric.isnull().any():
                        as_int = numeric.astype("int64")
                        if (numeric == as_int).all() and not is_epoch_like:
                            tmp = df.reset_index()
                            idx_col = idx.name if idx.name is not None else "index"
                            if idx_col in tmp.columns and "sequence" not in tmp.columns:
                                tmp = tmp.rename(columns={idx_col: "sequence"})
                                df = tmp
                                promoted = True
                                logger.debug(
                                    f"Promoted index to sequence in {path.name}"
                                )
                except Exception as exc:
                    logger.debug(f"Index promotion failed: {exc}")

    # Strategy 2: Promote 'index' column to 'sequence' if safe
    if not promoted and "index" in df.columns and "sequence" not in df.columns:
        try:
            # Don't promote datetime-like columns
            if pd.api.types.is_datetime64_any_dtype(df["index"]):
                raise ValueError("datetime column: do not promote to sequence")

            col_numeric = pd.to_numeric(df["index"], errors="coerce")

            # Detect epoch-like datetimes encoded as integers
            # Only flag as epoch if the datetime falls in a reasonable range (year 2000+)
            is_epoch_like_col = False
            try:
                dtc = pd.to_datetime(col_numeric, errors="coerce")
                if not dtc.isnull().any():
                    # Check if dates are in reasonable range (after year 2000)
                    year_2000 = pd.Timestamp("2000-01-01")
                    if (dtc >= year_2000).all() and (
                        dtc.astype("int64") == col_numeric.astype("int64")
                    ).all():
                        is_epoch_like_col = True
            except Exception:
                is_epoch_like_col = False

            # Promote if numeric and not epoch-like
            if (
                (not col_numeric.isnull().any())
                and (col_numeric == col_numeric.astype("int64")).all()
                and not is_epoch_like_col
            ):
                df = df.rename(columns={"index": "sequence"})
                promoted = True
                logger.debug(f"Promoted 'index' column to sequence in {path.name}")
        except Exception as exc:
            logger.debug(f"Column promotion failed: {exc}")

    return df


def _safe_remove(path: Path) -> None:
    """Safely remove a file, handling both old and new pathlib APIs."""
    try:
        path.unlink(missing_ok=True)
    except TypeError:
        # Older Python versions don't have missing_ok parameter
        if path.exists():
            path.unlink()
