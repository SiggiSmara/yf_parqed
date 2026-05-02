#!/usr/bin/env python3
"""
One-time registry migration and pruning.

Marks two classes of tickers as permanently_dead on all intervals so the
normal update_current_list pruning cycle can remove them on the next run:

  1. Old-schema globally-not-found tickers
     These have status="not_found" at the top level (old death-cycle model).
     The new code's legacy guard keeps them blocked, but they can never
     progress to permanently_dead on their own, so they are stuck forever.

  2. Dead instruments identifiable by symbol suffix
     Warrants (.W .WS .WT .WI), rights (.R .RT), and units (.U) will never
     trade as stocks and should not be collected.

Run without --apply first to see what would change.

Usage:
    uv run python tools/prune_registry.py [--wrk-dir PATH] [--apply]

Production:
    uv run python tools/prune_registry.py --wrk-dir /var/lib/yf_parqed --apply
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

# Symbol suffixes that identify non-stock instruments
_DEAD_SUFFIX = re.compile(r"\.(?:W[STI]?|R[T]?|U)$", re.IGNORECASE)


def _mark_permanently_dead(data: dict) -> int:
    """Set permanently_dead on every interval. Returns count of intervals changed."""
    changed = 0
    for iv in data.get("intervals", {}).values():
        if not iv.get("permanently_dead"):
            iv["permanently_dead"] = True
            changed += 1
    return changed


def migrate(wrk_dir: Path, apply: bool) -> None:
    tickers_path = wrk_dir / "tickers.json"
    if not tickers_path.exists():
        print(f"ERROR: {tickers_path} not found", file=sys.stderr)
        sys.exit(1)

    tickers: dict = json.loads(tickers_path.read_text())

    globally_dead: list[str] = []
    dead_suffix: list[str] = []
    intervals_changed = 0

    for symbol, data in tickers.items():
        reason = None
        if data.get("status") == "not_found":
            reason = "global_not_found"
        elif _DEAD_SUFFIX.search(symbol):
            reason = "dead_suffix"

        if reason is not None:
            n = _mark_permanently_dead(data)
            intervals_changed += n
            if n or not data.get("intervals"):
                # count the ticker even if it had no intervals (new entry, never checked)
                if reason == "global_not_found":
                    globally_dead.append(symbol)
                else:
                    dead_suffix.append(symbol)

    print(f"Old-schema global not_found  → permanently_dead: {len(globally_dead):>5} tickers")
    print(f"Dead instrument symbol suffix → permanently_dead: {len(dead_suffix):>5} tickers")
    print(f"Total intervals updated:                          {intervals_changed:>5}")

    if dead_suffix:
        sample = sorted(dead_suffix)[:30]
        print(f"\nDead suffix symbols ({len(dead_suffix)} total):")
        print("  " + "  ".join(sample))
        if len(dead_suffix) > 30:
            print(f"  ... and {len(dead_suffix) - 30} more")

    if not apply:
        print("\nDry run — pass --apply to write changes.")
        print("After applying, restart the daemon or run `yf-parqed update-tickers`")
        print("to trigger update_current_list, which will prune absent dead tickers.")
        return

    tmp = tickers_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(tickers, indent=4))
    tmp.rename(tickers_path)
    print(f"\nWritten to {tickers_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--wrk-dir",
        type=Path,
        default=Path("data"),
        help="Working directory containing tickers.json (default: ./data)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write changes. Without this flag the script is a dry run.",
    )
    args = parser.parse_args()
    migrate(args.wrk_dir, args.apply)


if __name__ == "__main__":
    main()
