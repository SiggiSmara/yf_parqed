"""
Add 2 more data points (0.60s and 1.50s delays) to the empirical mapping.
Then rerun regression analysis with all 5 points.
"""

import sys
import time
import csv
from pathlib import Path
from loguru import logger
from empirical_cooldown_mapping import (
    find_stable_cooldown_for_delay,
    run_cooldown,
    analyze_relationship,
)

# Configure logging
logger.remove()
logger.add("add_two_more_points.log", level="DEBUG", rotation="10 MB")
logger.add(sys.stderr, level="INFO")


def load_existing_results(csv_file: str) -> list[dict]:
    """Load existing results from CSV."""
    results = []
    if Path(csv_file).exists():
        with open(csv_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                results.append(
                    {
                        "delay": float(row["delay"]),
                        "cooldown": int(row["cooldown"]),
                        "total_time": float(row["total_time"]),
                        "feasible": row["feasible"] == "True",
                    }
                )
    return results


def save_all_results(results: list[dict], csv_file: str):
    """Save all results to CSV, sorted by delay."""
    sorted_results = sorted(results, key=lambda x: x["delay"])
    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["delay", "cooldown", "total_time", "feasible"]
        )
        writer.writeheader()
        writer.writerows(sorted_results)


def add_new_delays(
    new_delays: list[float],
    min_cooldown: int = 5,
    max_cooldown: int = 70,
    repeats: int = 3,
    csv_file: str = "cooldown_delay_mapping.csv",
):
    """
    Add new delay measurements to existing dataset.
    """
    logger.info("\n" + "=" * 70)
    logger.info("ADDING NEW DELAY MEASUREMENTS")
    logger.info("=" * 70)

    # Load existing results
    results = load_existing_results(csv_file)
    existing_delays = {r["delay"] for r in results}

    logger.info(f"Existing delays: {sorted(existing_delays)}")
    logger.info(f"New delays to test: {new_delays}")
    logger.info(f"Total after completion: {len(results) + len(new_delays)} points\n")

    overall_start = time.time()

    for i, delay in enumerate(new_delays, 1):
        logger.info(f"\n{'#' * 70}")
        logger.info(f"NEW DELAY {i}/{len(new_delays)}: {delay:.2f}s")
        logger.info(f"{'#' * 70}")

        # Find stable cooldown for this delay
        cooldown, total_time = find_stable_cooldown_for_delay(
            inter_request_delay=delay,
            min_cooldown=min_cooldown,
            max_cooldown=max_cooldown,
            repeats=repeats,
        )

        # Add to results
        results.append(
            {
                "delay": delay,
                "cooldown": cooldown,
                "total_time": total_time,
                "feasible": cooldown > 0,
            }
        )

        # Save updated results immediately
        save_all_results(results, csv_file)
        logger.info(f"✅ Updated {csv_file} with new data point")

        # Progress summary
        overall_elapsed = time.time() - overall_start
        logger.info(f"\n{'=' * 70}")
        logger.info(f"PROGRESS: {i}/{len(new_delays)} new delays tested")
        logger.info(
            f"Overall elapsed: {overall_elapsed:.1f}s ({overall_elapsed / 3600:.1f} hours)"
        )
        if i < len(new_delays):
            logger.info(f"Remaining: {len(new_delays) - i} delays")
        logger.info(f"{'=' * 70}\n")

        # Cooldown between different delays (2 minutes)
        if i < len(new_delays):
            run_cooldown(
                120, logger, context=f"before testing delay={new_delays[i]:.2f}s"
            )

    # Final summary
    overall_elapsed = time.time() - overall_start
    logger.success(f"\n{'=' * 70}")
    logger.success("🎉 NEW MEASUREMENTS COMPLETE!")
    logger.success(f"{'=' * 70}")
    logger.success(
        f"Total time: {overall_elapsed:.1f}s ({overall_elapsed / 3600:.1f} hours)"
    )
    logger.success(f"Total data points: {len(results)}")
    logger.success(f"Results saved to: {csv_file}\n")

    return results


if __name__ == "__main__":
    # Add the two new delay measurements
    results = add_new_delays(
        new_delays=[0.60, 1.50],
        min_cooldown=5,
        max_cooldown=70,
        repeats=3,
        csv_file="cooldown_delay_mapping.csv",
    )

    # Rerun regression analysis with all 5 points
    logger.info("\n" + "=" * 70)
    logger.info("UPDATED REGRESSION ANALYSIS WITH 5 DATA POINTS")
    logger.info("=" * 70 + "\n")

    analyze_relationship(results)
