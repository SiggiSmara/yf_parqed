"""Record of data files that were found damaged.

One JSON object per line in ``damaged_partitions.jsonl`` in the working
directory. The capture cannot be repeated (Yahoo serves 1-minute bars for
seven days) and the data has no backup, so a damaged file is kept and written
down here rather than removed. The file is only ever appended to.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger

DAMAGE_LOG_NAME = "damaged_partitions.jsonl"


def describe_data_file(path: Path) -> dict[str, str | None]:
    """Ticker, interval and month of a data file, as far as its path tells."""
    parts = Path(path).parts
    keyed = dict(part.split("=", 1) for part in parts if "=" in part)
    ticker = keyed.get("ticker")
    month = None
    if "year" in keyed and "month" in keyed:
        month = f"{keyed['year']}-{keyed['month']}"

    dataset = None
    if ticker is not None:
        # .../stocks_1m/ticker=AAPL/year=2026/month=09/data.parquet
        index = parts.index(f"ticker={ticker}")
        dataset = parts[index - 1] if index > 0 else None
    elif len(parts) >= 2:
        # legacy layout: .../stocks_1d/AAPL.parquet
        dataset = parts[-2]
        ticker = parts[-1].split(".parquet")[0]
    interval = dataset.rpartition("_")[2] if dataset and "_" in dataset else None
    return {"ticker": ticker, "interval": interval, "month": month}


class DamageLog:
    """Append-only list of damaged files."""

    _SAME_FINDING = ("path", "error", "size", "modified", "moved_to")

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def record(
        self,
        *,
        path: Path,
        error: BaseException | str,
        found_by: str,
        moved_to: Path | None = None,
        once: bool = False,
    ) -> dict | None:
        """
        Append one damaged file. ``moved_to`` is None when it was left where it is.

        With ``once`` nothing is appended, and None returned, when the same
        finding is already there: same path, error, size and modification time,
        left in place. A check that is repeated then adds no lines, while a
        file that was replaced and is damaged again gets a new one.
        """
        path = Path(os.path.abspath(path))
        try:
            stat = (moved_to if moved_to is not None else path).stat()
            size: int | None = stat.st_size
            modified: str | None = datetime.fromtimestamp(
                stat.st_mtime, timezone.utc
            ).isoformat(timespec="seconds")
        except OSError:
            size, modified = None, None
        entry = {
            "when": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "path": str(path),
            **describe_data_file(path),
            "size": size,
            "modified": modified,
            "error": error
            if isinstance(error, str)
            else f"{type(error).__name__}: {error}",
            "action": "moved aside" if moved_to is not None else "left in place",
            "moved_to": str(moved_to) if moved_to is not None else None,
            "found_by": found_by,
        }
        if once and any(
            all(old.get(key) == entry[key] for key in self._SAME_FINDING)
            for old in self.entries()
        ):
            return None
        line = json.dumps(entry) + "\n"

        # One write call per line. A crash can still leave a line without its
        # newline; start a fresh line then, so the half line spoils only itself.
        fd = os.open(self.path, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o644)
        try:
            size = os.fstat(fd).st_size
            if size and os.pread(fd, 1, size - 1) != b"\n":
                line = "\n" + line
            os.write(fd, line.encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)
        return entry

    def entries(self) -> list[dict]:
        """Every recorded file, oldest first. A line that is not valid JSON is skipped."""
        if not self.path.is_file():
            return []
        entries = []
        text = self.path.read_text("utf-8", errors="replace")
        for number, line in enumerate(text.splitlines(), 1):
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                logger.warning(f"Skipping unreadable line {number} of {self.path}")
                continue
            if isinstance(entry, dict):
                entries.append(entry)
        return entries
