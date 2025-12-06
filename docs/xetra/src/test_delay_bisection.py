"""
Bisection search to find optimal inter-request delay with 35s cooldown.

Strategy:
- Test bursts of EXACTLY 30 files (confirmed burst limit)
- Use 35s cooldown (optimal from previous test)
- Run 3 bursts total to verify delay works consistently
- Stop immediately on FIRST 429 error (don't wait for retry)
- Use bisection to find minimum inter-request delay (0-250ms)
- Cool down 120s between bisection iterations for clean slate

Goal: Find minimum inter-request delay that allows zero 429 errors.

Output: Logs to delay_bisection_test.log and console (INFO level only)
"""

import sys
import time
import httpx
from loguru import logger
from src.yf_parqed.xetra.xetra_fetcher import XetraFetcher

# Configure logging: file gets everything, console gets INFO+ only (no DEBUG)
logger.remove()
logger.add("delay_bisection_test.log", level="DEBUG", rotation="10 MB")
logger.add(sys.stderr, level="INFO")


def test_delay(inter_request_delay: float, cooldown_seconds: int = 35) -> bool:
    """
    Test if a specific inter-request delay allows 3 bursts of 30 files without 429s.

    Uses 35s cooldown (optimal from cooldown bisection test).
    Refreshes file list before each burst to avoid signed URL expiration.

    Returns True if all 3 bursts succeed, False on first 429.
    """
    delay_ms = int(inter_request_delay * 1000)
    logger.info(f"\n{'=' * 70}")
    logger.info(
        f"Testing {delay_ms}ms inter-request delay with {cooldown_seconds}s cooldown"
    )
    logger.info(f"{'=' * 70}\n")

    # Get file list (disable rate limiting for list_available_files)
    fetcher = XetraFetcher(inter_request_delay=0.001, burst_size=9999, burst_cooldown=0)

    try:
        # Create raw HTTP client
        client = httpx.Client(timeout=30.0)
        base_url = "https://mfs.deutsche-boerse.com/api/download/"

        # Run 3 bursts of exactly 30 files each
        for burst_num in range(1, 4):
            # Refresh file list before each burst (signed URLs expire quickly)
            logger.info(f"Burst {burst_num}/3: Refreshing file list...")
            files = fetcher.list_available_files(venue="DETR")
            logger.info(f"Found {len(files)} files available")
            logger.info("Downloading exactly 30 files...")

            burst_start = time.time()
            success_count = 0

            # Download first 30 files (fresh URLs)
            for i, filename in enumerate(files[:30], (burst_num - 1) * 30 + 1):
                try:
                    # Raw API call - no retry logic
                    url = f"{base_url}{filename}"
                    response = client.get(url, follow_redirects=True)

                    # Check for 429 IMMEDIATELY
                    if response.status_code == 429:
                        elapsed = time.time() - burst_start
                        logger.error(
                            f"\n❌ 429 ERROR in burst {burst_num} after {success_count} files at {elapsed:.1f}s"
                        )
                        logger.error(f"   File #{i}: {filename}")
                        logger.error(f"   {delay_ms}ms delay is TOO SHORT")
                        client.close()
                        return False

                    response.raise_for_status()
                    success_count += 1

                    if success_count in [10, 20, 30]:
                        elapsed = time.time() - burst_start
                        logger.info(
                            f"  [{i}] {success_count}/30 files, {elapsed:.1f}s elapsed, {success_count / elapsed:.2f}/s"
                        )

                    # Inter-request delay
                    time.sleep(inter_request_delay)

                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        elapsed = time.time() - burst_start
                        logger.error(
                            f"\n❌ 429 ERROR in burst {burst_num} after {success_count} files at {elapsed:.1f}s"
                        )
                        logger.error(f"   File #{i}: {filename}")
                        logger.error(f"   {delay_ms}ms delay is TOO SHORT")
                        client.close()
                        return False
                    raise

            elapsed = time.time() - burst_start
            logger.success(
                f"✅ Burst {burst_num} complete: 30 files in {elapsed:.1f}s ({30 / elapsed:.2f}/s)\n"
            )

            # Cooldown before next burst (but not after last burst)
            if burst_num < 3:
                logger.info(
                    f"Cooldown: Waiting {cooldown_seconds}s before burst {burst_num + 1}..."
                )
                for remaining in range(cooldown_seconds, 0, -10):
                    if remaining <= cooldown_seconds:
                        logger.info(f"  {remaining}s remaining...")
                    time.sleep(10)
                logger.info("")

        # All 3 bursts succeeded!
        logger.success(
            f"\n🎯 SUCCESS! All 3 bursts of 30 files completed with {delay_ms}ms delay"
        )
        logger.success("   No 429 errors detected\n")
        client.close()
        return True

    finally:
        fetcher.close()


def bisection_search_delay(
    min_delay_ms: int = 0, max_delay_ms: int = 250, cooldown: int = 35
) -> float:
    """
    Use bisection to find the minimum inter-request delay (in ms).

    Waits 120 seconds between iterations to ensure API buffer is fully empty.

    Returns the minimum working delay in seconds (e.g., 0.125 for 125ms).
    """
    logger.info("\n" + "=" * 70)
    logger.info("BISECTION SEARCH for optimal inter-request delay")
    logger.info("=" * 70)
    logger.info(f"Search range: {min_delay_ms}ms - {max_delay_ms}ms")
    logger.info(f"Cooldown: {cooldown}s (fixed, optimal from previous test)")
    logger.info("Target: 3 bursts of 30 files each, zero 429 errors")
    logger.info("Inter-iteration cooldown: 120s (ensure clean slate)\n")

    # Track results
    working_delays = []
    failed_delays = []
    iteration = 0

    while min_delay_ms <= max_delay_ms:
        iteration += 1
        # Try midpoint
        mid_ms = (min_delay_ms + max_delay_ms) // 2
        mid_seconds = mid_ms / 1000.0

        logger.info(f"\n{'─' * 70}")
        logger.info(
            f"ITERATION {iteration}: Testing {mid_ms}ms delay (range: {min_delay_ms}ms - {max_delay_ms}ms)"
        )
        logger.info(f"{'─' * 70}")

        # Wait 120s before testing (except first iteration)
        if iteration > 1:
            logger.info("\nWaiting 120s for API buffer to reset before next test...")
            for remaining in range(120, 0, -20):
                logger.info(f"  {remaining}s remaining...")
                time.sleep(20)
            logger.info("")

        if test_delay(mid_seconds, cooldown):
            # Success! This delay works, try shorter
            working_delays.append(mid_ms)
            logger.info(f"\n✅ {mid_ms}ms WORKS - trying shorter delay...")
            max_delay_ms = mid_ms - 1
        else:
            # Failed - need longer delay
            failed_delays.append(mid_ms)
            logger.info(f"\n❌ {mid_ms}ms FAILED - trying longer delay...")
            min_delay_ms = mid_ms + 1

    if working_delays:
        optimal_ms = min(working_delays)
        optimal_seconds = optimal_ms / 1000.0

        logger.success(f"\n{'=' * 70}")
        logger.success(
            f"🎯 OPTIMAL INTER-REQUEST DELAY FOUND: {optimal_ms}ms ({optimal_seconds:.3f}s)"
        )
        logger.success(f"{'=' * 70}")
        logger.success(f"Tested delays that WORKED: {sorted(working_delays)} ms")
        logger.success(f"Tested delays that FAILED: {sorted(failed_delays)} ms")

        # Calculate throughput improvement
        old_rate = 30 / (30 * 0.250 + 13)  # Assuming ~13s base download time
        new_rate = 30 / (30 * optimal_seconds + 13)
        improvement = (new_rate - old_rate) / old_rate * 100

        logger.success(
            f"\nRecommendation: Use {optimal_ms}ms ({optimal_seconds:.3f}s) delay between requests"
        )
        logger.success(
            f"Expected burst time: ~{30 * optimal_seconds + 13:.1f}s (vs ~{30 * 0.250 + 13:.1f}s with 250ms)"
        )
        logger.success(f"Throughput improvement: {improvement:+.1f}%\n")

        return optimal_seconds
    else:
        logger.error(f"\n{'=' * 70}")
        logger.error("❌ NO WORKING DELAY FOUND")
        logger.error(f"{'=' * 70}")
        logger.error(f"All tested delays failed: {sorted(failed_delays)} ms")
        logger.error(f"Try increasing max_delay above {max_delay_ms}ms\n")
        return -1.0


if __name__ == "__main__":
    logger.info("=" * 70)
    logger.info("Deutsche Börse API Inter-Request Delay Optimization")
    logger.info("=" * 70)
    logger.info("Goal: Find minimum delay between requests (zero 429s)")
    logger.info("Method: Bisection search between 0ms and 250ms")
    logger.info("Strategy: Raw API calls (no retry), 120s cooldown between tests")
    logger.info("Fixed: 35s cooldown between bursts (optimal from cooldown test)")
    logger.info("Logging: Console (INFO+) + delay_bisection_test.log (all levels)\n")

    optimal_delay = bisection_search_delay(
        min_delay_ms=0, max_delay_ms=250, cooldown=35
    )

    if optimal_delay > 0:
        optimal_ms = int(optimal_delay * 1000)
        logger.success(
            f"\n🎉 Optimal inter-request delay: {optimal_ms}ms ({optimal_delay:.3f}s)"
        )
        logger.success("Full test log saved to: delay_bisection_test.log")
        sys.exit(0)
    else:
        logger.error("\n❌ Could not find working delay in range")
        logger.error("Full test log saved to: delay_bisection_test.log")
        sys.exit(1)
