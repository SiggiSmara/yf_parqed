from __future__ import annotations

from datetime import datetime
from pathlib import Path

from .partition_path_builder import PartitionPathBuilder
from .storage import StorageRequest


class StorageRouter:
    """Thin helper that wraps partition path construction.

    This keeps services from reaching into PartitionPathBuilder internals while
    still allowing callers to build partitioned paths for reads/writes.
    """

    def __init__(self, root: Path) -> None:
        self._path_builder = PartitionPathBuilder(root=root)

    @property
    def path_builder(self) -> PartitionPathBuilder:
        return self._path_builder

    def partition_path(self, request: StorageRequest, *, timestamp: datetime) -> Path:
        return self._path_builder.build(
            market=request.market,
            source=request.source,
            dataset=request.dataset,
            interval=request.interval,
            ticker=request.ticker,
            timestamp=timestamp,
        )

    def ticker_root(self, request: StorageRequest) -> Path:
        return self._path_builder.ticker_root(
            market=request.market,
            source=request.source,
            dataset=request.dataset,
            interval=request.interval,
            ticker=request.ticker,
        )
