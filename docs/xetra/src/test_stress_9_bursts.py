"""
Stress test: 9 bursts of 30 files with 35s cooldown.

Goal: Verify 35s cooldown works reliably over longer windows (270 files total).
This tests if there are longer-term rate limiting windows we need to worry about.

Output: Logs to stress_test_9_bursts.log and console (INFO level only)
"""

import sys
import time
import httpx
from loguru import logger
from src.yf_parqed.xetra.xetra_fetcher import XetraFetcher

# Configure logging: file gets everything, console gets INFO+ only (no DEBUG)
logger.remove()
logger.add("stress_test_9_bursts.log", level="DEBUG", rotation="10 MB")
logger.add(sys.stderr, level="INFO")


def run_cooldown(cooldown_seconds: int, logger, context: str = None):
    """
    General cooldown function for burst tests.
    Logs countdown and sleeps in 10s increments for visibility.
    """
    msg = f"Cooldown: Waiting {cooldown_seconds}s"
    if context:
        msg += f" {context}"
    logger.info(msg + "...")
    for remaining in range(cooldown_seconds, 0, -10):
        if remaining <= cooldown_seconds:
            logger.info(f"  {remaining}s remaining...")
        time.sleep(min(10, remaining))
    logger.info("")


def stress_test_9_bursts(
    cooldown_seconds: int = 35, inter_request_delay: float = 0.25
) -> bool:
    """
    Test 9 bursts of 30 files each with cooldown (270 files total).

    Refreshes file list before each burst to avoid signed URL expiration.

    Returns True if all 9 bursts succeed, False on first 429.
    """
    logger.info("=" * 70)
    logger.info("STRESS TEST: 9 bursts of 30 files")
    logger.info("=" * 70)
    logger.info(f"Cooldown: {cooldown_seconds}s between bursts")
    logger.info(f"Inter-request delay: {int(inter_request_delay * 1000)}ms")
    logger.info("Total files: 270 (9 × 30)")
    logger.info(
        f"Expected duration: ~{9 * 15 + 8 * cooldown_seconds:.0f}s (~{(9 * 15 + 8 * cooldown_seconds) / 60:.1f} min)\n"
    )

    # Create fetcher for file listing (disable rate limiting for list_available_files)
    fetcher = XetraFetcher(inter_request_delay=0.001, burst_size=9999, burst_cooldown=0)

    try:
        # Create raw HTTP client
        client = httpx.Client(timeout=30.0)
        base_url = "https://mfs.deutsche-boerse.com/api/download/"

        test_start = time.time()
        total_files = 0
        num_bursts = 9

        # Run bursts
        for burst_num in range(1, num_bursts + 1):
            # REFRESH file list before each burst to avoid signed URL expiration
            logger.info(f"Burst {burst_num}/{num_bursts}: Refreshing file list...")
            files = fetcher.list_available_files(venue="DETR")
            logger.info(f"Found {len(files)} files available")

            if len(files) < 30:
                logger.error(f"Only {len(files)} files available, need 30 for burst")
                return False

            logger.info("Downloading 30 files...")
            burst_start = time.time()
            success_count = 0

            # Download until 30 successful files (skip expired)
            file_iter = iter(files)
            while success_count < 30:
                try:
                    filename = next(file_iter)
                    i = total_files + 1
                    url = f"{base_url}{filename}"
                    response = client.get(url, follow_redirects=True)
                    # Check for 429 IMMEDIATELY
                    if response.status_code == 429:
                        elapsed = time.time() - burst_start
                        total_elapsed = time.time() - test_start
                        logger.error(
                            f"\n❌ early catch 429 ERROR in burst {burst_num} after {success_count} files"
                            + f"   Burst elapsed: {elapsed:.1f}s"
                            + f"   Total elapsed: {total_elapsed:.1f}s ({total_elapsed / 60:.1f} min)"
                            + f"   Total files downloaded: {total_files}"
                            + f"   File #{i}: {filename}"
                        )
                        client.close()
                        return False
                    elif (
                        response.status_code == 400 and "ExpiredToken" in response.text
                    ):
                        logger.warning(
                            f"⚠️ 400 ExpiredToken for file #{i}: {filename} - skipping and continuing burst"
                        )
                        continue
                    elif response.status_code > 299:
                        elapsed = time.time() - burst_start
                        total_elapsed = time.time() - test_start
                        logger.error(
                            f"\n❌ ERROR {response.status_code} in burst {burst_num} after {success_count} files"
                            + f"  Response: {response.text}..."
                            + f"   Burst elapsed: {elapsed:.1f}s"
                            + f"   Total elapsed: {total_elapsed:.1f}s ({total_elapsed / 60:.1f} min)"
                            + f"   Total files downloaded: {total_files}"
                            + f"   File #{i}: {filename}"
                        )
                        client.close()
                        return False

                    response.raise_for_status()
                    success_count += 1
                    total_files += 1

                    # Progress at 10, 20, 30
                    if success_count in [10, 20, 30]:
                        elapsed = time.time() - burst_start
                        logger.info(
                            f"  [{total_files}] {success_count}/30 files, {elapsed:.1f}s elapsed, {success_count / elapsed:.2f}/s"
                        )

                    time.sleep(inter_request_delay)

                except StopIteration:
                    logger.error(
                        f"Ran out of files before reaching 30 successful downloads in burst {burst_num}"
                    )
                    client.close()
                    return False
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        elapsed = time.time() - burst_start
                        total_elapsed = time.time() - test_start
                        logger.error(
                            f"\n❌ late catch 429 ERROR in burst {burst_num} after {success_count} files"
                            + f"   Burst elapsed: {elapsed:.1f}s"
                            + f"   Total elapsed: {total_elapsed:.1f}s ({total_elapsed / 60:.1f} min)"
                            + f"   Total files downloaded: {total_files}"
                            + f"   File #{i}: {filename}"
                        )
                        client.close()
                        return False
                    elif e.response.status_code == 400 and "ExpiredToken" in str(
                        e.response.text
                    ):
                        logger.warning(
                            f"⚠️ 400 ExpiredToken for file #{i}: {filename} (exception) - skipping and continuing burst"
                        )
                        continue
                    raise

            burst_elapsed = time.time() - burst_start
            total_elapsed = time.time() - test_start
            logger.success(
                f"✅ Burst {burst_num} complete: 30 files in {burst_elapsed:.1f}s ({30 / burst_elapsed:.2f}/s)"
            )
            logger.success(
                f"   Total progress: {total_files}/{num_bursts * 30} files, {total_elapsed:.1f}s elapsed ({total_elapsed / 60:.1f} min)\n"
            )

            # Cooldown before next burst (not after last)
            if burst_num < num_bursts:
                run_cooldown(
                    cooldown_seconds, logger, context=f"before burst {burst_num + 1}"
                )

        # All bursts succeeded!
        total_elapsed = time.time() - test_start
        logger.success(f"\n{'=' * 70}")
        logger.success("🎯 STRESS TEST PASSED!")
        logger.success(f"{'=' * 70}")
        logger.success(f"Total files: {total_files}")
        logger.success(
            f"Total time: {total_elapsed:.1f}s ({total_elapsed / 60:.1f} min)"
        )
        logger.success(
            f"Average rate: {total_files / total_elapsed:.2f} files/s ({total_files / (total_elapsed / 60):.1f} files/min)"
        )
        logger.success("No 429 errors detected\n")

        client.close()
        return True

    finally:
        fetcher.close()


def extended_stresstest(
    cooldown: int = 70, inter_request_delay: float = 0.25, repeats: int = 3
) -> bool:
    """
    Runs the stress test multiple times with cooldown between runs.
    Returns True if all runs pass, False if any fail.
    """
    success_cnt = 0
    for i in range(repeats):
        logger.info(f"\n{'=' * 70}")
        logger.info(
            f"EXTENDED STRESS TEST RUN {i + 1}/{repeats} - Cooldown: {cooldown}s"
        )
        logger.info(f"{'=' * 70}")
        success = stress_test_9_bursts(
            cooldown_seconds=cooldown, inter_request_delay=inter_request_delay
        )
        if success:
            logger.success(
                f"\n🎉 Stress test PASSED - {cooldown}s cooldown is reliable!"
            )
            success_cnt += 1
            if i < repeats - 1:
                run_cooldown(
                    cooldown, logger, context="Between successful stress tests"
                )
        else:
            logger.error(
                "\n❌ Stress test FAILED - may need longer cooldown for sustained use"
            )
            logger.warning(
                f"Full cooldown of {2 * cooldown}s initiated before next test..."
            )
            if i < repeats - 1:
                run_cooldown(2 * cooldown, logger, context="after failed stress test")
    logger.info(
        f"\nSummary: {success_cnt}/{repeats} stress tests passed with {cooldown}s cooldown"
    )
    return success_cnt == repeats


def bisect_search_sustainable_cooldown(
    min_cooldown: int = 35,
    max_cooldown: int = 70,
    inter_request_delay: float = 0.25,
    repeats: int = 3,
):
    """
    Bisection search for minimum sustainable cooldown over multiple extended stress tests.
    Returns the minimum cooldown that passes all runs (no 429s).
    """
    logger.info("\n" + "=" * 70)
    logger.info("BISECTION SEARCH for sustainable cooldown (extended stress test)")
    logger.info("=" * 70)
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
            f"ITERATION {iteration}: Testing {mid}s cooldown (range: {min_cooldown}s - {max_cooldown}s)"
        )
        logger.info(f"{'─' * 70}")

        # Wait 120s before each test to clear API buffer
        if iteration > 1:
            run_cooldown(120, logger, context="before next bisection test")

        success = extended_stresstest(
            cooldown=mid, inter_request_delay=inter_request_delay, repeats=repeats
        )
        if success:
            working_cooldowns.append(mid)
            logger.info(f"\n✅ {mid}s WORKS - trying shorter cooldown...")
            max_cooldown = mid - 1
        else:
            failed_cooldowns.append(mid)
            logger.info(f"\n❌ {mid}s FAILED - trying longer cooldown...")
            min_cooldown = mid + 1

    if working_cooldowns:
        optimal = min(working_cooldowns)
        logger.success(f"\n{'=' * 70}")
        logger.success(f"🎯 SUSTAINABLE COOLDOWN FOUND: {optimal}s")
        logger.success(f"{'=' * 70}")
        logger.success(f"Tested cooldowns that WORKED: {sorted(working_cooldowns)}")
        logger.success(f"Tested cooldowns that FAILED: {sorted(failed_cooldowns)}")
        logger.success(
            f"\nRecommendation: Use {optimal}s cooldown between bursts of 30 files for sustained downloads\n"
        )
        return optimal
    else:
        logger.error(f"\n{'=' * 70}")
        logger.error("❌ NO SUSTAINABLE COOLDOWN FOUND")
        logger.error(f"{'=' * 70}")
        logger.error(f"All tested cooldowns failed: {sorted(failed_cooldowns)}")
        logger.error(f"Try increasing max_cooldown above {max_cooldown}s\n")
        return -1


if __name__ == "__main__":
    # Uncomment below to run bisection search for sustainable cooldown
    bisect_search_sustainable_cooldown(
        min_cooldown=35, max_cooldown=70, inter_request_delay=0.25, repeats=3
    )
