from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import pandas as pd


@dataclass(frozen=True)
class StorageRequest:
    root: Path
    interval: str
    ticker: str
    market: str | None = None
    source: str | None = None
    dataset: str = "stocks"

    def legacy_path(self) -> Path:
        return self.root / f"stocks_{self.interval}" / f"{self.ticker}.parquet"


class StorageInterface(Protocol):
    def read(self, request: StorageRequest) -> pd.DataFrame: ...

    def save(
        self,
        request: StorageRequest,
        new_data: pd.DataFrame,
        existing_data: pd.DataFrame,
    ) -> pd.DataFrame: ...
