from pathlib import Path

import pandas as pd

from src.yf_parqed.storage_backend import StorageBackend, StorageRequest


def _empty_factory():
    return pd.DataFrame(
        columns=["stock", "date", "open", "high", "low", "close", "volume", "sequence"]
    )


def _normalizer(df: pd.DataFrame) -> pd.DataFrame:
    # Minimal normalizer for tests: ensure columns lowercased and 'sequence' exists
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    if "sequence" in df.columns:
        df["sequence"] = df["sequence"].astype(int, errors="ignore")
    return df


def _columns():
    return ["stock", "date", "open", "high", "low", "close", "volume", "sequence"]


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, compression=None)


def test_recover_from_unnamed_index(tmp_path: Path):
    # Create parquet with unnamed index (reset_index will create 'index' column)
    df = pd.DataFrame(
        {
            "stock": ["A"],
            "date": ["2020-01-01"],
            "open": [1],
            "high": [1],
            "low": [1],
            "close": [1],
            "volume": [100],
        }
    )
    df.index = pd.Index([5])

    p = tmp_path / "stocks_1d" / "A.parquet"
    # Write index as part of file by resetting (simulate real-file that has index)
    _write_parquet(p, df.reset_index())

    backend = StorageBackend(_empty_factory, _normalizer, _columns)
    req = StorageRequest(root=tmp_path, interval="1d", ticker="A")
    out = backend.read(req)
    assert "sequence" in out.reset_index().columns


def test_recover_from_named_index(tmp_path: Path):
    # Create parquet where index is named 'sequence'
    df = pd.DataFrame(
        {
            "stock": ["B"],
            "date": ["2020-01-02"],
            "open": [1],
            "high": [1],
            "low": [1],
            "close": [1],
            "volume": [200],
        }
    )
    df.index = pd.Index([7], name="sequence")
    p = tmp_path / "stocks_1d" / "B.parquet"
    _write_parquet(p, df.reset_index())

    backend = StorageBackend(_empty_factory, _normalizer, _columns)
    req = StorageRequest(root=tmp_path, interval="1d", ticker="B")
    out = backend.read(req)
    assert "sequence" in out.reset_index().columns


def test_recover_from_index_column(tmp_path: Path):
    # Create parquet with literal 'index' column (older writes)
    df = pd.DataFrame(
        {
            "index": [9],
            "stock": ["C"],
            "date": ["2020-01-03"],
            "open": [1],
            "high": [1],
            "low": [1],
            "close": [1],
            "volume": [300],
        }
    )
    p = tmp_path / "stocks_1d" / "C.parquet"
    _write_parquet(p, df)

    backend = StorageBackend(_empty_factory, _normalizer, _columns)
    req = StorageRequest(root=tmp_path, interval="1d", ticker="C")
    out = backend.read(req)
    assert "sequence" in out.reset_index().columns


def test_do_not_promote_datetime_index(tmp_path: Path):
    # Create parquet with datetime index (should NOT be promoted to sequence)
    df = pd.DataFrame(
        {
            "stock": ["D"],
            "date": ["2020-01-04"],
            "open": [1],
            "high": [1],
            "low": [1],
            "close": [1],
            "volume": [400],
        }
    )
    df.index = pd.DatetimeIndex(["2020-01-04T00:00:00Z"])  # datetime index
    p = tmp_path / "stocks_1d" / "D.parquet"
    _write_parquet(p, df.reset_index())

    backend = StorageBackend(_empty_factory, _normalizer, _columns)
    req = StorageRequest(root=tmp_path, interval="1d", ticker="D")
    out = backend.read(req)
    # Because required columns cannot be satisfied (sequence missing and not promoted), we expect empty frame
    # File should be PRESERVED (not deleted) for operator inspection
    assert out.empty
    assert p.exists()
