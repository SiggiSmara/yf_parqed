import logging
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd

logger = logging.getLogger("migration_benchmark_checker")


def _read_original(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index()
    else:
        df = df.reset_index(drop=True)
    if "stock" not in df.columns:
        df["stock"] = path.stem
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    # sort stable
    sort_cols = [c for c in ["date", "sequence"] if c in df.columns]
    if not sort_cols:
        sort_cols = ["date"]
    return df.sort_values(sort_cols).reset_index(drop=True)


def _read_migrated(strategy_root: Path, ticker: str) -> pd.DataFrame:
    ticker_root = strategy_root / f"ticker={ticker}"
    if not ticker_root.exists():
        return pd.DataFrame()
    parts: List[pd.DataFrame] = []
    for p in sorted(ticker_root.rglob("*.parquet")):
        try:
            df = pd.read_parquet(p)
            if isinstance(df.index, pd.MultiIndex):
                df = df.reset_index()
            else:
                df = df.reset_index(drop=True)
            parts.append(df)
        except Exception as exc:
            logger.warning("Failed to read migrated piece %s: %s", p, exc)
    if not parts:
        return pd.DataFrame()
    df_all = pd.concat(parts, ignore_index=True)
    if "stock" not in df_all.columns:
        df_all["stock"] = ticker
    df_all["date"] = pd.to_datetime(df_all["date"], errors="coerce")
    sort_cols = [c for c in ["date", "sequence"] if c in df_all.columns]
    if not sort_cols:
        sort_cols = ["date"]
    return df_all.sort_values(sort_cols).reset_index(drop=True)


def compare_ticker_rowwise(
    original_path: Path, strategy_root: Path
) -> Tuple[bool, str]:
    ticker = original_path.stem
    try:
        orig = _read_original(original_path)
    except Exception as exc:
        return False, f"orig-read-fail: {exc}"

    migrated = _read_migrated(strategy_root, ticker)

    if migrated.empty and orig.empty:
        return True, "both-empty"
    if migrated.empty and not orig.empty:
        return False, "migrated-empty"
    if orig.empty and not migrated.empty:
        return False, "original-empty"

    if len(orig) != len(migrated):
        return False, f"row-count-mismatch orig={len(orig)} migr={len(migrated)}"

    orig_min, orig_max = orig["date"].min(), orig["date"].max()
    mig_min, mig_max = migrated["date"].min(), migrated["date"].max()
    if (
        pd.isna(orig_min)
        or pd.isna(mig_min)
        or orig_min != mig_min
        or orig_max != mig_max
    ):
        return (
            False,
            f"date-range-mismatch orig=({orig_min},{orig_max}) mig=({mig_min},{mig_max})",
        )

    common_cols = [c for c in orig.columns if c in migrated.columns]
    if not common_cols:
        return False, "no-common-columns"
    o = orig[common_cols].copy().reset_index(drop=True)
    m = migrated[common_cols].copy().reset_index(drop=True)

    try:
        pd.testing.assert_frame_equal(o, m, check_dtype=False, check_like=True)
    except AssertionError as exc:
        return False, f"content-mismatch: {exc}"

    return True, "ok"


def run_checks(
    original_files: Iterable[Path], output_root: Path, strategies: Iterable[str]
) -> dict:
    results = {}
    original_list = list(original_files)
    for strategy in strategies:
        strategy_root = Path(output_root) / strategy
        passed = []
        failed: List[Tuple[str, str]] = []
        for orig in original_list:
            ok, reason = compare_ticker_rowwise(orig, strategy_root)
            if ok:
                passed.append(orig.stem)
            else:
                failed.append((orig.stem, reason))
        results[strategy] = {"passed": passed, "failed": failed}

    # print summary
    for strategy, res in results.items():
        total = len(res["passed"]) + len(res["failed"])
        logger.info(
            "Sanity check for strategy '%s': %d/%d passed",
            strategy,
            len(res["passed"]),
            total,
        )
        if res["failed"]:
            logger.warning("Sample failures for '%s': %s", strategy, res["failed"][:10])
    return results
