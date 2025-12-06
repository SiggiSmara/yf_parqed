"""
Bisection search to find optimal cooldown period for Deutsche Börse API.

Strategy:
- Test bursts of EXACTLY 30 files (confirmed burst limit)
- Run 3 bursts total to verify cooldown works consistently
- Stop immediately on FIRST 429 error (don't wait for retry)
- Use bisection to efficiently find minimum reliable cooldown
- Make raw API calls to bypass retry mechanism
- Cool down 120s between bisection iterations for clean slate

Goal: Zero 429 errors across all 3 bursts of 30 files each.

Output: Logs to rate_limit_test.log and console (INFO level only, no DEBUG spam)
"""

import sys
import time
import httpx
from loguru import logger
from src.yf_parqed.xetra.xetra_fetcher import XetraFetcher

# Configure logging: file gets everything, console gets INFO+ only (no DEBUG)
logger.remove()  # Remove default handler
logger.add("rate_limit_test.log", level="DEBUG", rotation="10 MB")  # File: all levels
logger.add(
    sys.stderr, level="INFO"
)  # Console: INFO+ only (suppress DEBUG from XetraFetcher)


def test_cooldown_period(
    cooldown_seconds: int, inter_request_delay: float = 0.25
) -> bool:
    """
    Test if a specific cooldown period allows 3 bursts of 30 files without any 429s.

    Uses raw API calls to bypass retry mechanism and catch 429s immediately.
    Refreshes file list before each burst to avoid signed URL expiration.

    Returns True if all 3 bursts succeed, False on first 429.
    """
    logger.info(f"\n{'=' * 70}")
    logger.info(
        f"Testing {cooldown_seconds}s cooldown with {int(inter_request_delay * 1000)}ms delay"
    )
    logger.info(f"{'=' * 70}\n")

    # Get file list (uses XetraFetcher for filtering logic)
    fetcher = XetraFetcher(inter_request_delay=0.001, burst_size=9999, burst_cooldown=0)

    try:
        # Create raw HTTP client for downloads (no retry logic)
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
                        logger.error(f"   {cooldown_seconds}s cooldown is TOO SHORT")
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
                        logger.error(f"   {cooldown_seconds}s cooldown is TOO SHORT")
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
            f"\n🎯 SUCCESS! All 3 bursts of 30 files completed with {cooldown_seconds}s cooldown"
        )
        logger.success("   No 429 errors detected\n")
        client.close()
        return True

    finally:
        fetcher.close()


def bisection_search(
    min_cooldown: int = 30, max_cooldown: int = 120, delay: float = 0.25
) -> int:
    """
    Use bisection to find the minimum cooldown period that allows 3 bursts without 429s.

    Waits 120 seconds between iterations to ensure API buffer is fully empty.

    Returns the minimum working cooldown period in seconds.
    """
    logger.info("\n" + "=" * 70)
    logger.info("BISECTION SEARCH for optimal cooldown period")
    logger.info("=" * 70)
    logger.info(f"Search range: {min_cooldown}s - {max_cooldown}s")
    logger.info("Target: 3 bursts of 30 files each, zero 429 errors")
    logger.info("Inter-iteration cooldown: 120s (ensure clean slate)\n")

    # Track results
    working_cooldowns = []
    failed_cooldowns = []
    iteration = 0

    while min_cooldown <= max_cooldown:
        iteration += 1
        # Try midpoint
        mid = (min_cooldown + max_cooldown) // 2

        logger.info(f"\n{'─' * 70}")
        logger.info(
            f"ITERATION {iteration}: Testing {mid}s cooldown (range: {min_cooldown}s - {max_cooldown}s)"
        )
        logger.info(f"{'─' * 70}")

        # Wait 120s before testing (except first iteration) to ensure clean API state
        if iteration > 1:
            logger.info("\nWaiting 120s for API buffer to reset before next test...")
            for remaining in range(120, 0, -20):
                logger.info(f"  {remaining}s remaining...")
                time.sleep(20)
            logger.info("")

        if test_cooldown_period(mid, delay):
            # Success! This cooldown works, try shorter
            working_cooldowns.append(mid)
            logger.info(f"\n✅ {mid}s WORKS - trying shorter cooldown...")
            max_cooldown = mid - 1
        else:
            # Failed - need longer cooldown
            failed_cooldowns.append(mid)
            logger.info(f"\n❌ {mid}s FAILED - trying longer cooldown...")
            min_cooldown = mid + 1

    if working_cooldowns:
        optimal = min(working_cooldowns)
        logger.success(f"\n{'=' * 70}")
        logger.success(f"🎯 OPTIMAL COOLDOWN FOUND: {optimal}s")
        logger.success(f"{'=' * 70}")
        logger.success(f"Tested cooldowns that WORKED: {sorted(working_cooldowns)}")
        logger.success(f"Tested cooldowns that FAILED: {sorted(failed_cooldowns)}")
        logger.success(
            f"\nRecommendation: Use {optimal}s cooldown between bursts of 30 files\n"
        )
        return optimal
    else:
        logger.error(f"\n{'=' * 70}")
        logger.error("❌ NO WORKING COOLDOWN FOUND")
        logger.error(f"{'=' * 70}")
        logger.error(f"All tested cooldowns failed: {sorted(failed_cooldowns)}")
        logger.error(f"Try increasing max_cooldown above {max_cooldown}s\n")
        return -1


if __name__ == "__main__":
    import sys

    logger.info("=" * 70)
    logger.info("Deutsche Börse API Rate Limit Testing")
    logger.info("=" * 70)
    logger.info("Goal: Find minimum cooldown for 3 bursts of 30 files (zero 429s)")
    logger.info("Method: Bisection search between 30s and 120s")
    logger.info("Strategy: Raw API calls (no retry), 120s cooldown between tests")
    logger.info("Logging: Console (INFO+) + rate_limit_test.log (all levels)\n")

    optimal_cooldown = bisection_search(min_cooldown=30, max_cooldown=120, delay=0.25)

    if optimal_cooldown > 0:
        logger.success(f"\n🎉 Optimal cooldown: {optimal_cooldown}s")
        logger.success("Full test log saved to: rate_limit_test.log")
        sys.exit(0)
    else:
        logger.error("\n❌ Could not find working cooldown in range")
        logger.error("Full test log saved to: rate_limit_test.log")
        sys.exit(1)
