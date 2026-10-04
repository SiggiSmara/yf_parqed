from __future__ import annotations

import json
import os
import socket
import time
from pathlib import Path
from typing import Any

from loguru import logger

from .parquet_recovery import DAMAGED_MARKER, move_aside
from .partition_check import read_completely


class GlobalRunLock:
    """Simple global run lock using an atomic mkdir as the lock primitive.

    The lock directory contains an owner.json file with pid/host/timestamp so
    operators can inspect who holds the lock. This intentionally keeps behavior
    simple: there is no force-unlock path in the library. Callers may prompt
    the operator to run cleanup and then remove the lock directory manually.
    """

    def __init__(self, base_dir: Path, name: str = ".run_lock") -> None:
        self.base_dir = Path(base_dir)
        self.lock_dir = self.base_dir / name
        self.owner_file = self.lock_dir / "owner.json"

    def try_acquire(self) -> bool:
        """Try to acquire the lock by creating the lock directory.

        Returns True when acquired, False when the lock already exists.
        """
        try:
            # atomic on POSIX: mkdir either succeeds or raises FileExistsError
            self.lock_dir.mkdir(exist_ok=False)
        except FileExistsError:
            return False

        meta = {
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "ts": time.time(),
            "cwd": str(os.getcwd()),
        }
        try:
            self.owner_file.write_text(json.dumps(meta))
        except Exception as exc:  # best-effort
            logger.debug("Failed to write lock owner metadata: {exc}", exc=exc)
        return True

    def owner_info(self) -> dict[str, Any] | None:
        try:
            if not self.owner_file.exists():
                return None
            return json.loads(self.owner_file.read_text())
        except Exception:
            return None

    def release(self) -> None:
        try:
            if self.owner_file.exists():
                try:
                    self.owner_file.unlink()
                except Exception:
                    pass
            if self.lock_dir.exists():
                try:
                    self.lock_dir.rmdir()
                except Exception:
                    # if directory isn't empty or rmdir fails, leave it for operator
                    logger.debug(
                        "Unable to remove lock dir {path}", path=str(self.lock_dir)
                    )
        except Exception:
            logger.debug("Exception while releasing lock", exc_info=True)

    def cleanup_tmp_files(self) -> int:
        """Scan the data root for temp partition files and attempt recovery.

        Rules:
        - For each file matching "data.parquet.tmp-*":
            - If final "data.parquet" exists: remove the tmp file.
            - Else, if the tmp file reads back completely: atomically
              replace tmp -> final.
            - Else: move the tmp file aside (``.damaged-<timestamp>``). It
              is most likely a write that was cut short, but it may be the
              only copy of the fetch that was being stored.

        Returns the number of tmp files processed.
        """
        processed = 0
        data_root = self.base_dir / "data"
        if not data_root.exists():
            return 0

        for tmp in data_root.rglob("data.parquet.tmp-*"):
            if DAMAGED_MARKER in tmp.name:
                continue
            try:
                final = tmp.with_name("data.parquet")
                if final.exists():
                    try:
                        tmp.unlink()
                    except Exception:
                        logger.debug("Failed to remove tmp file {path}", path=str(tmp))
                elif not self._reads_completely(tmp):
                    moved_to = move_aside(tmp)
                    logger.warning(
                        "Tmp file {path} does not read back completely and was not "
                        "made the data file; kept as {moved}",
                        path=str(tmp),
                        moved=moved_to.name if moved_to is not None else tmp.name,
                    )
                else:
                    try:
                        os.replace(str(tmp), str(final))
                    except Exception as exc:
                        logger.warning(
                            "Failed to recover tmp file {path}: {err}",
                            path=str(tmp),
                            err=exc,
                        )
                processed += 1
            except Exception:
                logger.debug(
                    "Error handling tmp file {path}", path=str(tmp), exc_info=True
                )

        return processed

    @staticmethod
    def _reads_completely(path: Path) -> bool:
        """Whether a leftover tmp file is a whole Parquet file (full decode)."""
        try:
            read_completely(path)
            return True
        except MemoryError:
            raise
        except Exception:
            return False
