"""Shared parquet file recovery logic for all storage backends.

This module provides unified recovery strategies for parquet files with schema issues.
No file is deleted. A file that cannot be read at all is moved aside (renamed in
its directory) so that capture can continue into a new file; files with schema
mismatches stay where they are for operator inspection while clear errors are raised.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pandas as pd
from loguru import logger


# Part of the name an unreadable file is given when it is moved aside:
# data.parquet -> data.parquet.damaged-20261004T101500Z. The new name no longer
# ends in .parquet, so no reader or glob in this project picks it up.
DAMAGED_MARKER = ".damaged-"

#: Told about every unreadable file: its path, where it was moved (None if it
#: could not be moved and is still in place) and the error that was raised.
DamageRecorder = Callable[[Path, "Path | None", BaseException], None]


class ParquetRecoveryError(Exception):
    """Raised when a parquet file cannot be recovered through safe transformations."""

    #: True when the file is unreadable and could not be moved aside either.
    #: It is still at its path, and nothing may be written over it.
    unreadable_in_place = False


def move_aside(path: Path) -> Path | None:
    """
    Rename a damaged file to ``<name>.damaged-<UTC timestamp>`` in its directory.

    Returns the new path, or None when the rename failed and the file is still
    where it was. Renaming the file back undoes a false alarm.
    """
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = path.with_name(f"{path.name}{DAMAGED_MARKER}{stamp}")
    attempt = 1
    while target.exists():
        target = path.with_name(f"{path.name}{DAMAGED_MARKER}{stamp}-{attempt}")
        attempt += 1
    try:
        path.rename(target)
    except OSError as exc:
        logger.error(f"Could not move {path} aside: {exc}. It is left in place.")
        return None
    return target


def safe_read_parquet(
    path: Path,
    required_columns: set[str],
    normalizer: Callable[[pd.DataFrame], pd.DataFrame],
    empty_frame_factory: Callable[[], pd.DataFrame],
    on_damaged: DamageRecorder | None = None,
) -> pd.DataFrame:
    """
    Read a parquet file with comprehensive recovery strategies.

    This function implements a multi-stage recovery process:
    1. Attempt to read the file
    2. If successful but empty, raise ParquetRecoveryError (preserve file)
    3. If missing required columns, attempt safe promotions
    4. If recovery fails, raise ParquetRecoveryError (preserve file)
    5. If the file cannot be read at all, move it aside, tell ``on_damaged``
       and raise ParquetRecoveryError

    Args:
        path: Path to the parquet file
        required_columns: Set of column names that must be present
        normalizer: Function to normalize DataFrame types/columns
        empty_frame_factory: Function to create an empty DataFrame with correct schema
        on_damaged: Called with (path, moved_to, error) for an unreadable file

    Returns:
        Normalized DataFrame if successful

    Raises:
        ParquetRecoveryError: If file cannot be recovered (with details about why)
    """
    # Stage 1: Attempt to read the file. A second attempt lets an error that
    # passes (too many open files, a disk hiccup) go by without consequences.
    exc: Exception | None = None
    for _attempt in range(2):
        try:
            df = pd.read_parquet(path)
            exc = None
            break
        except FileNotFoundError as missing:
            raise ParquetRecoveryError(
                f"Parquet file {path} does not exist."
            ) from missing
        except (ValueError, OSError) as failure:
            exc = failure
    if exc is not None:
        # Unreadable twice. The bars in the file can rarely be fetched again,
        # so the file is kept under another name and the next write starts a
        # new one.
        moved_to = move_aside(path)
        outcome = (
            f"File moved aside to {moved_to.name}."
            if moved_to is not None
            else "File could not be moved aside and is left in place."
        )
        logger.error(f"Unable to read parquet file {path}: {exc}. {outcome}")
        if on_damaged is not None:
            try:
                on_damaged(path, moved_to, exc)
            except Exception as record_exc:
                logger.error(f"Could not record damaged file {path}: {record_exc}")
        error = ParquetRecoveryError(f"Parquet file {path} is unreadable. {outcome}")
        error.unreadable_in_place = moved_to is None
        raise error from exc

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
