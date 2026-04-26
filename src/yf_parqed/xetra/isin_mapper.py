from pathlib import Path

import pandas as pd
from loguru import logger


class ISINMapper:
    """In-memory ISIN→ticker lookup loaded from the local Parquet cache."""

    def __init__(self, cache_path: Path):
        self.cache_path = cache_path
        self._mapping: dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        if not self.cache_path.exists():
            logger.warning(f"ISIN mapping cache not found at {self.cache_path}")
            self._mapping = {}
            return
        df = pd.read_parquet(self.cache_path)
        active = df[df["status"] == "active"]
        self._mapping = dict(zip(active["isin"], active["ticker"]))
        logger.debug(f"Loaded {len(self._mapping)} active ISIN mappings from {self.cache_path}")

    def get_ticker(self, isin: str) -> str | None:
        """Return the Xetra mnemonic for an ISIN, or None if unmapped."""
        return self._mapping.get(isin)

    def reload(self) -> None:
        """Reload the mapping from disk (call after an update cycle)."""
        self._load()
