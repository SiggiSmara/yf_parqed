"""Read-only check of stored monthly partition files.

The daemon opens only the month it is writing to, so a file that goes bad in a
closed month would never be noticed. This module reads the files of one month
(or of all months) completely, one file in memory at a time, and reports the
ones that cannot be read. It changes nothing in the data directories.
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

import pyarrow.parquet as pq
from loguru import logger

from .shutdown import StopCheck

CHECK_STATE_NAME = "partition_checks.json"
PARTITION_FILE_NAME = "data.parquet"

_MONTH_PATTERN = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")


def parse_month(month: str) -> tuple[int, int]:
    """Year and month number of a ``YYYY-MM`` string."""
    match = _MONTH_PATTERN.match(month)
    if match is None:
        raise ValueError(f"Month must be given as YYYY-MM, got {month!r}")
    return int(match.group(1)), int(match.group(2))


def month_of(day: date) -> str:
    return f"{day.year:04d}-{day.month:02d}"


def last_closed_month(today: date) -> str:
    """The calendar month before the one ``today`` falls in, as ``YYYY-MM``."""
    if today.month == 1:
        return f"{today.year - 1:04d}-12"
    return f"{today.year:04d}-{today.month - 1:02d}"


@dataclass
class MonthCheck:
    """What a check found in one month."""

    files: int = 0
    rows: int = 0
    damaged: list[tuple[Path, str]] = field(default_factory=list)


@dataclass
class PartitionCheck:
    """Result of one run. ``complete`` is False when a stop request cut it short."""

    months: dict[str, MonthCheck] = field(default_factory=dict)
    complete: bool = True
    seconds: float = 0.0

    @property
    def files(self) -> int:
        return sum(check.files for check in self.months.values())

    @property
    def damaged(self) -> list[tuple[Path, str]]:
        return [item for check in self.months.values() for item in check.damaged]


def stock_datasets(data_root: Path) -> list[Path]:
    """Partitioned price datasets under a data root: ``<market>/<source>/stocks_<interval>``."""
    if not data_root.is_dir():
        return []
    return sorted(path for path in data_root.glob("*/*/stocks_*") if path.is_dir())


def _ticker_dirs(dataset: Path) -> list[Path]:
    with os.scandir(dataset) as entries:
        return sorted(
            Path(entry.path) for entry in entries if entry.name.startswith("ticker=")
        )


def _partition_files(
    ticker_dir: Path, month: tuple[int, int] | None
) -> Iterator[tuple[str, Path]]:
    """(month, file) pairs of one ticker: the given month, or every month it has."""
    if month is not None:
        year, number = month
        # The path is built, not listed: listing each ticker costs a seek per
        # directory on the hard disk, and a ticker without the month is common.
        yield (
            f"{year:04d}-{number:02d}",
            ticker_dir
            / f"year={year:04d}"
            / f"month={number:02d}"
            / PARTITION_FILE_NAME,
        )
        return
    for path in sorted(ticker_dir.glob(f"year=*/month=*/{PARTITION_FILE_NAME}")):
        year_part = path.parent.parent.name.partition("=")[2]
        month_part = path.parent.name.partition("=")[2]
        yield f"{year_part}-{month_part}", path


def read_completely(path: Path) -> int:
    """Decode every column of every row group; returns the row count."""
    with pq.ParquetFile(path) as parquet_file:
        return parquet_file.read().num_rows


def check_partitions(
    datasets: Iterable[Path],
    month: str | None = None,
    should_stop: StopCheck | None = None,
) -> PartitionCheck:
    """
    Read every partition file of ``month`` (``YYYY-MM``; all months when None).

    A file counts as damaged when it cannot be decoded completely or holds no
    rows (the writer never stores an empty month). Nothing on disk is changed.
    ``should_stop`` is asked before each ticker.
    """
    wanted = parse_month(month) if month is not None else None
    result = PartitionCheck()
    if month is not None:
        result.months[month] = MonthCheck()
    started = time.perf_counter()

    for dataset in datasets:
        for ticker_dir in _ticker_dirs(dataset):
            if should_stop is not None and should_stop():
                result.complete = False
                result.seconds = time.perf_counter() - started
                return result
            for month_key, path in _partition_files(ticker_dir, wanted):
                try:
                    rows = read_completely(path)
                except FileNotFoundError:
                    continue
                except MemoryError:
                    raise
                except Exception as exc:
                    check = result.months.setdefault(month_key, MonthCheck())
                    check.files += 1
                    check.damaged.append((path, f"{type(exc).__name__}: {exc}"))
                    continue
                check = result.months.setdefault(month_key, MonthCheck())
                check.files += 1
                check.rows += rows
                if rows == 0:
                    check.damaged.append((path, "the file holds no rows"))

    result.seconds = time.perf_counter() - started
    return result


class PartitionCheckState:
    """Which closed months have been checked, and with what result (``partition_checks.json``)."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def load(self) -> dict[str, dict]:
        if not self.path.is_file():
            return {}
        try:
            data = json.loads(self.path.read_text("utf-8"))
        except (OSError, ValueError) as exc:  # includes bad JSON and bad bytes
            logger.warning(f"Could not read {self.path}: {exc}. Treating it as empty.")
            return {}
        return data if isinstance(data, dict) else {}

    def has(self, month: str) -> bool:
        return month in self.load()

    def record(self, results: dict[str, MonthCheck]) -> None:
        """Store the result of each month, replacing an earlier entry for it."""
        if not results:
            return
        state = self.load()
        checked_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        for month, check in results.items():
            state[month] = {
                "checked_at": checked_at,
                "files": check.files,
                "rows": check.rows,
                "damaged": len(check.damaged),
            }
        temp_path = self.path.with_name(f"{self.path.name}.tmp-{os.getpid()}")
        temp_path.write_text(
            json.dumps(dict(sorted(state.items())), indent=2) + "\n", "utf-8"
        )
        os.replace(temp_path, self.path)
