import argparse
import random
import shutil
from pathlib import Path
from time import perf_counter
from typing import Callable, Iterable, cast

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def _select_tickers(files: list[Path], sample_size: int, seed: int) -> list[Path]:
    if sample_size >= len(files):
        return files
    rng = random.Random(seed)
    return sorted(rng.sample(files, sample_size), key=lambda path: path.stem)


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _directory_size(path: Path) -> int:
    total = 0
    for child in path.rglob("*"):
        if child.is_file():
            total += child.stat().st_size
    return total


def _total_file_size(files: Iterable[Path]) -> int:
    return sum(path.stat().st_size for path in files if path.is_file())


def _detect_legacy_compression(files: Iterable[Path]) -> str:
    for path in files:
        if not path.exists():
            continue
        try:
            metadata = pq.read_metadata(path)
        except (OSError, pa.ArrowInvalid):  # file missing or corrupted
            continue
        compressions = set()
        for i in range(metadata.num_row_groups):
            row_group = metadata.row_group(i)
            for j in range(row_group.num_columns):
                compression = row_group.column(j).compression
                if compression:
                    compressions.add(compression)
        if compressions:
            return next(iter(compressions)).lower()
    return "gzip"


def _write_grouped_parquet(
    df: pd.DataFrame,
    ticker: str,
    dest_root: Path,
    *,
    partition_col: str,
    key_formatter: Callable[[pd.Series], Iterable[str]],
    prefix: str,
    compression: str,
) -> int:
    arrow_bytes = 0
    ticker_root = dest_root / f"ticker={ticker}"
    _ensure_dir(ticker_root)

    groups = df.groupby(partition_col, sort=True)
    for key, group in groups:
        identifiers = list(key_formatter(pd.Series([key])))
        identifier = identifiers[0] if identifiers else str(key)
        file_path = ticker_root / f"{prefix}={identifier}.parquet"
        table = pa.Table.from_pandas(group, preserve_index=False)
        pq.write_table(table, file_path, compression=compression)
        arrow_bytes += file_path.stat().st_size
    return arrow_bytes


def _write_daily(
    df: pd.DataFrame,
    ticker: str,
    dest_root: Path,
    compression: str,
) -> int:
    df = df.copy()
    df["day"] = df["date"].dt.normalize()
    return _write_grouped_parquet(
        df,
        ticker,
        dest_root,
        partition_col="day",
        key_formatter=lambda s: [s.dt.strftime("%Y-%m-%d").iloc[0]],
        prefix="day",
        compression=compression,
    )


def _write_weekly(
    df: pd.DataFrame,
    ticker: str,
    dest_root: Path,
    compression: str,
) -> int:
    df = df.copy()
    df["week_start"] = df["date"].dt.to_period("W-SUN").dt.start_time
    return _write_grouped_parquet(
        df,
        ticker,
        dest_root,
        partition_col="week_start",
        key_formatter=lambda s: [s.dt.strftime("%Y-%m-%d").iloc[0]],
        prefix="week",
        compression=compression,
    )


def _write_monthly(
    df: pd.DataFrame,
    ticker: str,
    dest_root: Path,
    compression: str,
) -> int:
    df = df.copy()
    df["month"] = df["date"].dt.to_period("M").astype(str)
    return _write_grouped_parquet(
        df,
        ticker,
        dest_root,
        partition_col="month",
        key_formatter=lambda s: s,
        prefix="month",
        compression=compression,
    )


def _write_yearly(
    df: pd.DataFrame,
    ticker: str,
    dest_root: Path,
    compression: str,
) -> int:
    df = df.copy()
    df["year"] = df["date"].dt.year.astype("Int64")
    return _write_grouped_parquet(
        df,
        ticker,
        dest_root,
        partition_col="year",
        key_formatter=lambda s: [str(int(s.iloc[0]))],
        prefix="year",
        compression=compression,
    )


def _load_frame(path: Path, ticker: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index()
    else:
        df = df.reset_index(drop=True)

    if "stock" not in df.columns:
        df["stock"] = ticker
    df["stock"] = df["stock"].astype("string")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


def benchmark_strategy(
    ticker_files: list[Path],
    *,
    strategy: str,
    output_root: Path,
    writer: Callable[[pd.DataFrame, str, Path, str], int],
    compression: str,
) -> dict[str, object]:
    strategy_root = output_root / strategy
    if strategy_root.exists():
        shutil.rmtree(strategy_root)
    strategy_root.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    per_ticker_times: list[float] = []
    start = perf_counter()

    for ticker_file in ticker_files:
        ticker = ticker_file.stem
        ticker_start = perf_counter()
        df = _load_frame(ticker_file, ticker)
        rows = len(df)
        total_rows += rows
        if rows:
            writer(df, ticker, strategy_root, compression)
        elapsed = perf_counter() - ticker_start
        per_ticker_times.append(elapsed)

    total_elapsed = perf_counter() - start
    output_bytes = _directory_size(strategy_root)
    per_ticker_avg = sum(per_ticker_times) / len(per_ticker_times)
    sorted_times = sorted(per_ticker_times)
    mid = len(sorted_times) // 2
    if len(sorted_times) % 2 == 0:
        per_ticker_median = (sorted_times[mid - 1] + sorted_times[mid]) / 2
    else:
        per_ticker_median = sorted_times[mid]

    return {
        "strategy": strategy,
        "tickers": len(ticker_files),
        "total_rows": total_rows,
        "total_time": total_elapsed,
        "per_ticker_avg": per_ticker_avg,
        "per_ticker_median": per_ticker_median,
        "output_bytes": output_bytes,
        "output_root": strategy_root,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark daily, weekly, monthly, and yearly parquet compaction during "
            "migration using legacy compression settings."
        ),
    )
    parser.add_argument(
        "--interval",
        default="1d",
        help="Legacy interval to sample (default: 1d).",
    )
    parser.add_argument(
        "--legacy-root",
        type=Path,
        default=Path("data/legacy"),
        help="Root directory containing legacy parquet files.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("/tmp/yf_parqed_migration_benchmark"),
        help="Directory to write benchmark outputs (default: /tmp/yf_parqed_migration_benchmark).",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=400,
        help="Number of tickers to benchmark (default: 400).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for ticker selection.",
    )
    args = parser.parse_args()

    legacy_interval_dir = args.legacy_root / f"stocks_{args.interval}"
    if not legacy_interval_dir.exists():
        raise SystemExit(f"Legacy directory not found: {legacy_interval_dir}")

    ticker_files = sorted(legacy_interval_dir.glob("*.parquet"))
    if not ticker_files:
        raise SystemExit(f"No parquet files found in {legacy_interval_dir}")

    sample = _select_tickers(ticker_files, args.sample_size, args.seed)
    print(
        f"Selected {len(sample)} tickers from {legacy_interval_dir} "
        f"(seed={args.seed})."
    )
    print("Tickers:", ", ".join(path.stem for path in sample))

    total_interval_tickers = len(ticker_files)
    args.output_root.mkdir(parents=True, exist_ok=True)

    compression = _detect_legacy_compression(sample)
    print(f"Using legacy compression codec: {compression}")

    baseline_bytes = _total_file_size(sample)
    print(
        "\nBaseline (legacy monoliths)\n"
        f"  Files counted        : {len(sample)}\n"
        f"  Total size (MiB)     : {baseline_bytes / (1024**2):.2f}"
    )

    if baseline_bytes == 0:
        raise SystemExit("Baseline legacy files are empty; cannot compare sizes.")

    strategies = {
        # "daily": _write_daily,
        # "weekly": _write_weekly,
        "monthly": _write_monthly,
        "yearly": _write_yearly,
    }

    for name, writer in strategies.items():
        metrics = benchmark_strategy(
            sample,
            strategy=name,
            output_root=args.output_root,
            writer=writer,
            compression=compression,
        )
        total_time = metrics["total_time"]
        per_ticker_avg = cast(float, metrics["per_ticker_avg"])
        per_ticker_median = cast(float, metrics["per_ticker_median"])
        output_bytes = cast(int, metrics["output_bytes"])
        size_ratio = output_bytes / baseline_bytes
        estimated_total_seconds = per_ticker_avg * total_interval_tickers
        estimated_total_hours = estimated_total_seconds / 3600
        estimated_total_days = estimated_total_hours / 24
        print(
            f"\nStrategy: {name}\n"
            f"  Tickers processed    : {metrics['tickers']}\n"
            f"  Total rows           : {metrics['total_rows']}\n"
            f"  Total time (s)       : {total_time:.2f}\n"
            f"  Avg per ticker (s)   : {per_ticker_avg:.2f}\n"
            f"  Median per ticker (s): {per_ticker_median:.2f}\n"
            f"  Output size (MiB)    : {output_bytes / (1024**2):.2f}\n"
            f"  Size vs legacy       : {size_ratio:.2f}×\n"
            f"  Est. full run (h)    : {estimated_total_hours:.2f}\n"
            f"  Est. full run (days) : {estimated_total_days:.2f}\n"
            f"  Output path          : {metrics['output_root']}"
        )


if __name__ == "__main__":
    main()
