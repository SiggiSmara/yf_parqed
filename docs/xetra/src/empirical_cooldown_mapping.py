"""
Empirical mapping of inter-request delay → minimum stable cooldown.

Strategy: For each delay in [0.25, 0.50, 0.75, 1.00, 1.25, 1.50, 1.75, 2.00],
run bisection search to find minimum stable cooldown.

Output: CSV with (delay, cooldown, total_time) for simple regression analysis.
"""

import sys
import time
import csv
from pathlib import Path
from loguru import logger
from test_stress_9_bursts import extended_stresstest, run_cooldown

# Configure logging
logger.remove()
logger.add("empirical_cooldown_mapping.log", level="DEBUG", rotation="10 MB")
logger.add(sys.stderr, level="INFO")


def find_stable_cooldown_for_delay(
    inter_request_delay: float,
    min_cooldown: int = 5,
    max_cooldown: int = 70,
    repeats: int = 3,
) -> tuple[int, float]:
    """
    Find minimum stable cooldown for given inter-request delay using bisection.

    Returns (optimal_cooldown, estimated_total_time)
    Returns (-1, -1) if no stable cooldown found in range.
    """
    logger.info(f"\n{'=' * 70}")
    logger.info(f"FINDING STABLE COOLDOWN FOR DELAY={inter_request_delay:.2f}s")
    logger.info(f"{'=' * 70}")
    logger.info(f"Search range: {min_cooldown}s - {max_cooldown}s")
    logger.info(f"Target: 9 bursts × {repeats} runs, zero 429 errors\n")

    working_cooldowns = []
    failed_cooldowns = []
    iteration = 0

    while min_cooldown <= max_cooldown:
        iteration += 1
        mid = (min_cooldown + max_cooldown) // 2
        logger.info(f"\n{'─' * 70}")
        logger.info(
            f"ITERATION {iteration}: Testing cooldown={mid}s, delay={inter_request_delay:.2f}s"
        )
        logger.info(f"  (range: {min_cooldown}s - {max_cooldown}s)")
        logger.info(f"{'─' * 70}")

        # Wait 120s before each test to clear API state
        if iteration > 1:
            run_cooldown(120, logger, context="before next bisection test")

        # Run extended stress test
        start_time = time.time()
        success = extended_stresstest(
            cooldown=mid, inter_request_delay=inter_request_delay, repeats=repeats
        )
        elapsed = time.time() - start_time

        if success:
            working_cooldowns.append((mid, elapsed))
            logger.info(
                f"\n✅ cooldown={mid}s WORKS (took {elapsed:.1f}s = {elapsed / 60:.1f} min)"
            )
            logger.info("   Trying shorter cooldown...")
            max_cooldown = mid - 1
        else:
            failed_cooldowns.append(mid)
            logger.info(f"\n❌ cooldown={mid}s FAILED")
            logger.info("   Trying longer cooldown...")
            min_cooldown = mid + 1

    if working_cooldowns:
        # Find the minimum cooldown that worked
        optimal_cooldown = min(c for c, _ in working_cooldowns)
        optimal_time = next(t for c, t in working_cooldowns if c == optimal_cooldown)

        logger.success(f"\n{'=' * 70}")
        logger.success(f"🎯 STABLE COOLDOWN FOUND for delay={inter_request_delay:.2f}s")
        logger.success(f"{'=' * 70}")
        logger.success(f"Optimal cooldown: {optimal_cooldown}s")
        logger.success(f"Total time: {optimal_time:.1f}s ({optimal_time / 60:.1f} min)")
        logger.success(
            f"Tested cooldowns that WORKED: {sorted([c for c, _ in working_cooldowns])}"
        )
        logger.success(f"Tested cooldowns that FAILED: {sorted(failed_cooldowns)}\n")

        return optimal_cooldown, optimal_time
    else:
        logger.error(f"\n{'=' * 70}")
        logger.error(
            f"❌ NO STABLE COOLDOWN FOUND for delay={inter_request_delay:.2f}s"
        )
        logger.error(f"{'=' * 70}")
        logger.error(f"All tested cooldowns failed: {sorted(failed_cooldowns)}")
        logger.error(f"Try increasing max_cooldown above {max_cooldown}s\n")

        return -1, -1.0


def empirical_cooldown_mapping(
    delays: list[float] = None,
    min_cooldown: int = 5,
    max_cooldown: int = 70,
    repeats: int = 3,
    output_file: str = "cooldown_delay_mapping.csv",
):
    """
    Map inter-request delays to minimum stable cooldowns.

    For each delay, runs bisection search to find optimal cooldown.
    Saves results to CSV for analysis.
    """
    if delays is None:
        delays = [0.25, 1.00, 2.00]  # Default reduced set for faster testing

    logger.info("\n" + "=" * 70)
    logger.info("EMPIRICAL COOLDOWN MAPPING")
    logger.info("=" * 70)
    logger.info(f"Testing {len(delays)} inter-request delays: {delays}")
    logger.info(f"Cooldown search range: {min_cooldown}s - {max_cooldown}s")
    logger.info(f"Repeats per test: {repeats}")
    logger.info(f"Output file: {output_file}")
    logger.info(
        f"Estimated duration: ~{len(delays) * 2}-{len(delays) * 4} hours (depends on bisection)\n"
    )

    results = []
    overall_start = time.time()

    for i, delay in enumerate(delays, 1):
        logger.info(f"\n{'#' * 70}")
        logger.info(f"DELAY {i}/{len(delays)}: {delay:.2f}s")
        logger.info(f"{'#' * 70}")

        # Find stable cooldown for this delay
        cooldown, total_time = find_stable_cooldown_for_delay(
            inter_request_delay=delay,
            min_cooldown=min_cooldown,
            max_cooldown=max_cooldown,
            repeats=repeats,
        )

        # Record result
        results.append(
            {
                "delay": delay,
                "cooldown": cooldown,
                "total_time": total_time,
                "feasible": cooldown > 0,
            }
        )

        # Save intermediate results
        _save_results(results, output_file)

        # Progress summary
        overall_elapsed = time.time() - overall_start
        logger.info(f"\n{'=' * 70}")
        logger.info(f"PROGRESS: {i}/{len(delays)} delays tested")
        logger.info(
            f"Overall elapsed: {overall_elapsed:.1f}s ({overall_elapsed / 3600:.1f} hours)"
        )
        if i < len(delays):
            logger.info(f"Remaining: {len(delays) - i} delays")
        logger.info(f"{'=' * 70}\n")

        # Cooldown between different delays (2 minutes)
        if i < len(delays):
            run_cooldown(120, logger, context=f"before testing delay={delays[i]:.2f}s")

    # Final summary
    overall_elapsed = time.time() - overall_start
    logger.success(f"\n{'=' * 70}")
    logger.success("🎉 EMPIRICAL MAPPING COMPLETE!")
    logger.success(f"{'=' * 70}")
    logger.success(
        f"Total time: {overall_elapsed:.1f}s ({overall_elapsed / 3600:.1f} hours)"
    )
    logger.success(f"Results saved to: {output_file}\n")

    # Display results table
    _display_results(results)

    return results


def _save_results(results: list[dict], output_file: str):
    """Save results to CSV file."""
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["delay", "cooldown", "total_time", "feasible"]
        )
        writer.writeheader()
        writer.writerows(results)


def _display_results(results: list[dict]):
    """Display results as formatted table."""
    logger.info("\n" + "=" * 70)
    logger.info("RESULTS SUMMARY")
    logger.info("=" * 70)
    logger.info(
        f"{'Delay (s)':<12} {'Cooldown (s)':<15} {'Total Time (min)':<20} {'Feasible'}"
    )
    logger.info("-" * 70)

    for r in results:
        delay_str = f"{r['delay']:.2f}"
        cooldown_str = f"{r['cooldown']}" if r["feasible"] else "NOT FOUND"
        time_str = f"{r['total_time'] / 60:.1f}" if r["feasible"] else "N/A"
        feasible_str = "✅" if r["feasible"] else "❌"
        logger.info(f"{delay_str:<12} {cooldown_str:<15} {time_str:<20} {feasible_str}")

    logger.info("=" * 70 + "\n")


def analyze_relationship(
    results: list[dict] = None, csv_file: str = "cooldown_delay_mapping.csv"
):
    """
    Linear regression analysis using scipy.

    Fits: cooldown = slope * delay + intercept
    Reports slope, intercept, R², p-value, and standard error.

    If results is None, reads data from csv_file instead.
    """
    from scipy import stats
    import numpy as np

    # Load results from CSV if not provided
    if results is None:
        if not Path(csv_file).exists():
            logger.error(f"CSV file not found: {csv_file}")
            return

        results = []
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
        logger.info(f"Loaded {len(results)} data points from {csv_file}\n")

    # Filter to feasible points only
    feasible = [r for r in results if r["feasible"]]

    if len(feasible) < 2:
        logger.error("Not enough feasible points for regression analysis")
        return

    delays = np.array([r["delay"] for r in feasible])
    cooldowns = np.array([r["cooldown"] for r in feasible])

    # Linear regression using scipy
    slope, intercept, r_value, p_value, std_err = stats.linregress(delays, cooldowns)
    r_squared = r_value**2

    logger.info("\n" + "=" * 70)
    logger.info("LINEAR REGRESSION ANALYSIS (scipy.stats.linregress)")
    logger.info("=" * 70)
    logger.info(f"Equation: cooldown = {slope:.3f} * delay + {intercept:.3f}")
    logger.info("")
    logger.info(f"R² = {r_squared:.4f}")
    logger.info(f"p-value = {p_value:.4e}")
    logger.info(f"Standard error = {std_err:.3f}")
    logger.info("")

    # Interpretation
    if p_value < 0.05:
        logger.info("✅ Relationship is statistically significant (p < 0.05)")
    else:
        logger.warning("⚠️  Relationship is NOT statistically significant (p >= 0.05)")

    if r_squared > 0.9:
        logger.info("✅ Strong linear relationship (R² > 0.9)")
    elif r_squared > 0.7:
        logger.info("⚠️  Moderate linear relationship (0.7 < R² < 0.9)")
    else:
        logger.warning("❌ Weak linear relationship (R² < 0.7)")

    logger.info("")

    # Show predictions
    logger.info("Predicted vs Actual:")
    logger.info(
        f"{'Delay':<10} {'Actual':<12} {'Predicted':<12} {'Error':<12} {'% Error'}"
    )
    logger.info("-" * 60)
    for r in feasible:
        predicted = slope * r["delay"] + intercept
        error = abs(r["cooldown"] - predicted)
        pct_error = (error / r["cooldown"]) * 100 if r["cooldown"] > 0 else 0
        logger.info(
            f"{r['delay']:<10.2f} {r['cooldown']:<12} {predicted:<12.1f} {error:<12.1f} {pct_error:.1f}%"
        )

    logger.info("=" * 70 + "\n")

    # Calculate optimal time for each configuration
    logger.info("Expected total times (3 × 9 bursts × 30 files):")
    logger.info(f"{'Delay':<10} {'Cooldown':<12} {'Total Time (min)'}")
    logger.info("-" * 40)
    for r in feasible:
        # time_per_run = 9 * (30*delay + 2) + 8*cooldown
        # total = 3 * time_per_run + 2*cooldown
        time_per_burst = 30 * r["delay"] + 2
        time_per_run = 9 * time_per_burst + 8 * r["cooldown"]
        total_time = 3 * time_per_run + 2 * r["cooldown"]
        logger.info(f"{r['delay']:<10.2f} {r['cooldown']:<12} {total_time / 60:.1f}")
    logger.info("=" * 70 + "\n")

    return slope, intercept, r_squared, p_value, std_err


if __name__ == "__main__":
    import sys

    # Check if we should only analyze existing data
    if len(sys.argv) > 1 and sys.argv[1] == "--analyze-only":
        # Just analyze existing CSV data
        logger.info("Running analysis on existing data only (--analyze-only mode)\n")
        analyze_relationship(csv_file="cooldown_delay_mapping.csv")
    else:
        # Run full empirical mapping
        results = empirical_cooldown_mapping(
            delays=[0.25, 1.00, 2.00],  # Reduced to 3 points for faster testing
            min_cooldown=5,
            max_cooldown=70,
            repeats=3,
            output_file="cooldown_delay_mapping.csv",
        )

        # Analyze relationship
        analyze_relationship(results)
