#!/usr/bin/env python3
"""
Test script to determine Deutsche Börse API rate limiting behavior.

Theory: API allows bursts of ~30 requests, then requires a cooldown period.
Goal: Find optimal burst size and cooldown period for maximum throughput.
"""

import time
from loguru import logger
from src.yf_parqed.xetra_fetcher import XetraFetcher

# Configure logger
logger.remove()
logger.add(
    lambda msg: print(msg, end=""),
    format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | {message}",
)


def test_burst_pattern(burst_size: int, cooldown_seconds: float, num_batches: int = 3):
    """
    Test downloading in bursts with cooldown periods.

    Args:
        burst_size: Number of files to download in each burst
        cooldown_seconds: How long to wait between bursts
        num_batches: How many batches to test
    """
    logger.info(
        f"Testing: {burst_size} files/burst, {cooldown_seconds}s cooldown, {num_batches} batches"
    )

    # Disable rate limiting by setting very short delays and no cooldown
    fetcher = XetraFetcher(inter_request_delay=0.001, burst_size=9999, burst_cooldown=0)

    try:
        # Get list of available files
        from datetime import datetime

        today = datetime.now().strftime("%Y-%m-%d")

        files = fetcher.list_available_files(venue="DETR", date=today)
        if not files:
            logger.error("No files available from API")
            return

        logger.info(f"Found {len(files)} files available")

        total_downloaded = 0
        total_429s = 0
        batch_times = []

        for batch_num in range(num_batches):
            logger.info(f"\n{'='*60}")
            logger.info(f"Batch {batch_num + 1}/{num_batches}")
            logger.info(f"{'='*60}")

            # Select files for this batch
            start_idx = (batch_num * burst_size) % len(files)
            end_idx = start_idx + burst_size
            batch_files = files[start_idx:end_idx]

            batch_start = time.time()
            batch_downloaded = 0
            batch_429s = 0

            for i, filename in enumerate(batch_files, 1):
                file_start = time.time()

                try:
                    # Extract date from filename
                    date_part = filename.split("-")[2]
                    date_str = f"{date_part}-{filename.split('-')[3]}-{filename.split('-')[4].split('T')[0]}"

                    # Download
                    content = fetcher.download_file("DETR", date_str, filename)

                    if content:
                        batch_downloaded += 1
                        total_downloaded += 1
                        elapsed = time.time() - file_start
                        logger.success(
                            f"  [{i}/{burst_size}] Downloaded {filename} ({len(content):,} bytes, {elapsed:.2f}s)"
                        )
                    else:
                        logger.warning(
                            f"  [{i}/{burst_size}] Empty response for {filename}"
                        )

                except Exception as e:
                    error_str = str(e)
                    if "429" in error_str or "Rate limit" in error_str:
                        batch_429s += 1
                        total_429s += 1
                        logger.error(
                            f"  [{i}/{burst_size}] 429 ERROR at file #{i} - {filename}"
                        )
                        logger.warning(
                            f"  Hit rate limit after {batch_downloaded} successful downloads in this batch"
                        )
                        break
                    else:
                        logger.error(f"  [{i}/{burst_size}] Error: {e}")

            batch_elapsed = time.time() - batch_start
            batch_times.append(batch_elapsed)

            logger.info(f"\nBatch {batch_num + 1} summary:")
            logger.info(f"  Downloaded: {batch_downloaded}/{burst_size} files")
            logger.info(f"  429 errors: {batch_429s}")
            logger.info(f"  Time: {batch_elapsed:.2f}s")
            if batch_elapsed > 0:
                logger.info(f"  Rate: {batch_downloaded/batch_elapsed:.2f} files/sec")

            # Cooldown before next batch (except after last batch)
            if batch_num < num_batches - 1:
                logger.info(f"\nCooling down for {cooldown_seconds}s...")
                time.sleep(cooldown_seconds)

        # Final summary
        logger.info(f"\n{'='*60}")
        logger.info("FINAL SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Total downloaded: {total_downloaded}/{burst_size * num_batches}")
        logger.info(f"Total 429 errors: {total_429s}")
        if batch_times:
            logger.info(f"Average batch time: {sum(batch_times)/len(batch_times):.2f}s")
            logger.info(
                f"Overall throughput: {total_downloaded/sum(batch_times):.2f} files/sec"
            )

        # Calculate estimated time for full day
        files_per_day = 1142  # Typical for DETR
        if total_downloaded > 0 and total_429s == 0:
            total_time = sum(batch_times)
            time_per_file = total_time / total_downloaded
            full_day_time = (files_per_day * time_per_file) + (
                (files_per_day / burst_size) * cooldown_seconds
            )
            logger.info(
                f"\nEstimated time for full day (1142 files): {full_day_time/60:.1f} minutes"
            )

    finally:
        fetcher.close()


def test_no_rate_limit_until_429():
    """
    Download as fast as possible until we hit 429, to find the burst limit.
    """
    logger.info("Testing: Download until 429 (no rate limiting)")

    # Disable rate limiting by setting very high limits
    fetcher = XetraFetcher(max_requests=9999, duration=1)

    try:
        # Get all available files (date-agnostic API)
        files = fetcher.list_available_files(venue="DETR")
        if not files:
            logger.error("No files available")
            return

        logger.info(f"Found {len(files)} files available")

        start_time = time.time()
        downloaded = 0
        burst_boundaries = []  # Track where 429s occur

        for i, filename in enumerate(files[:100], 1):  # Test up to 100 files
            try:
                # Extract date from filename for the API call
                # Format: DETR-posttrade-2025-11-04T13_54.json.gz
                date_part = filename.split("-")[2]  # "2025"
                date_str = f"{date_part}-{filename.split('-')[3]}-{filename.split('-')[4].split('T')[0]}"  # "2025-11-04"

                content = fetcher.download_file("DETR", date_str, filename)
                if content:
                    downloaded += 1
                    elapsed = time.time() - start_time

                    # Log every 10th file for less noise
                    if i % 10 == 0 or i <= 50:
                        logger.success(
                            f"[{i}] Downloaded ({downloaded} total, {elapsed:.2f}s, {downloaded/elapsed:.2f}/sec)"
                        )

            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "Rate limit" in error_str:
                    elapsed = time.time() - start_time
                    burst_boundaries.append((downloaded, elapsed))
                    logger.warning(
                        f"\n⚠️  429 HIT at file #{i} (after {downloaded} successful downloads)"
                    )
                    logger.info(
                        f"Time elapsed: {elapsed:.2f}s, Rate: {downloaded/elapsed:.2f}/sec"
                    )

                    # Don't break - let the retry mechanism handle it and continue
                    # This way we can see the full pattern
                    continue
                else:
                    logger.error(f"Error: {e}")

        # Summary
        elapsed = time.time() - start_time
        logger.info(f"\n{'='*60}")
        logger.info(f"SUMMARY: Downloaded {downloaded}/100 files in {elapsed:.2f}s")
        logger.info(f"Overall rate: {downloaded/elapsed:.2f} files/sec")

        if burst_boundaries:
            logger.info("\n429 errors occurred at these points:")
            for idx, (count, time_elapsed) in enumerate(burst_boundaries, 1):
                logger.info(f"  #{idx}: After {count} files, at {time_elapsed:.2f}s")

            # Calculate burst intervals
            if len(burst_boundaries) > 1:
                logger.info("\nBurst intervals (files between 429s):")
                for i in range(1, len(burst_boundaries)):
                    interval = burst_boundaries[i][0] - burst_boundaries[i - 1][0]
                    time_diff = burst_boundaries[i][1] - burst_boundaries[i - 1][1]
                    logger.info(f"  Burst #{i+1}: {interval} files in {time_diff:.2f}s")
        else:
            logger.info("\nNo 429 errors encountered!")

        logger.info(f"{'='*60}")

    finally:
        fetcher.close()


def test_optimal_cooldown():
    """Test different cooldown periods to find optimal reset time."""
    logger.info("TEST 2: Finding optimal cooldown period\n")

    # Test cooldown periods: 10s, 15s, 20s, 30s
    cooldown_periods = [10, 15, 20, 30]

    for cooldown in cooldown_periods:
        logger.info(f"\n{'='*60}")
        logger.info(f"Testing {cooldown}s cooldown period")
        logger.info(f"{'='*60}\n")

        fetcher = XetraFetcher(max_requests=9999, duration=1)

        try:
            files = fetcher.list_available_files(venue="DETR")
            if not files:
                logger.error("No files available")
                continue

            logger.info(f"Found {len(files)} files available")

            # Download first burst until 429
            logger.info("\nPhase 1: Downloading until first 429...")
            burst1_count = 0
            start_time = time.time()

            for i, filename in enumerate(files[:60], 1):
                try:
                    date_part = filename.split("-")[2]
                    date_str = f"{date_part}-{filename.split('-')[3]}-{filename.split('-')[4].split('T')[0]}"

                    content = fetcher.download_file("DETR", date_str, filename)
                    if content:
                        burst1_count += 1

                        if i % 10 == 0:
                            elapsed = time.time() - start_time
                            logger.info(
                                f"  [{i}] {burst1_count} downloaded, {elapsed:.2f}s"
                            )

                except Exception as e:
                    if "429" in str(e) or "Rate limit" in str(e):
                        elapsed = time.time() - start_time
                        logger.warning(
                            f"\n⚠️  First 429 after {burst1_count} files at {elapsed:.2f}s"
                        )
                        break
                    else:
                        logger.error(f"Error: {e}")
                        break

            # Cooldown period
            logger.info(f"\nPhase 2: Cooling down for {cooldown}s...")
            time.sleep(cooldown)

            # Try second burst
            logger.info(
                f"\nPhase 3: Testing if burst quota reset after {cooldown}s cooldown..."
            )
            burst2_count = 0
            burst2_start = time.time()
            second_429 = False

            for i, filename in enumerate(files[60:100], 61):
                try:
                    date_part = filename.split("-")[2]
                    date_str = f"{date_part}-{filename.split('-')[3]}-{filename.split('-')[4].split('T')[0]}"

                    content = fetcher.download_file("DETR", date_str, filename)
                    if content:
                        burst2_count += 1

                        if burst2_count % 10 == 0:
                            elapsed = time.time() - burst2_start
                            logger.info(
                                f"  [{i}] {burst2_count} downloaded, {elapsed:.2f}s"
                            )

                except Exception as e:
                    if "429" in str(e) or "Rate limit" in str(e):
                        elapsed = time.time() - burst2_start
                        logger.warning(
                            f"\n⚠️  Second 429 after only {burst2_count} files at {elapsed:.2f}s"
                        )
                        logger.error(f"❌ {cooldown}s cooldown was NOT sufficient!")
                        second_429 = True
                        break
                    else:
                        logger.error(f"Error: {e}")
                        break

            if not second_429:
                logger.success(
                    f"\n✅ SUCCESS! {cooldown}s cooldown allowed {burst2_count} more downloads without 429!"
                )
                logger.info(
                    f"Recommendation: Use {cooldown}s cooldown between bursts of ~{burst1_count} files"
                )
                break  # Found optimal cooldown, no need to test longer ones

        finally:
            fetcher.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "find-limit":
        test_no_rate_limit_until_429()
    elif len(sys.argv) > 1 and sys.argv[1] == "cooldown":
        test_optimal_cooldown()
    elif len(sys.argv) >= 3:
        # Test 3: Test specific burst/cooldown pattern
        burst_size = int(sys.argv[1])
        cooldown = float(sys.argv[2])
        batches = int(sys.argv[3]) if len(sys.argv) > 3 else 3

        test_burst_pattern(burst_size, cooldown, batches)

    else:
        print("Usage:")
        print("  python test_rate_limit_burst.py find-limit")
        print("  python test_rate_limit_burst.py cooldown")
        print(
            "  python test_rate_limit_burst.py <burst_size> <cooldown_seconds> [num_batches]"
        )
        print("\nExamples:")
        print(
            "  python test_rate_limit_burst.py find-limit          # Find burst limit (100 files)"
        )
        print(
            "  python test_rate_limit_burst.py cooldown            # Find optimal cooldown period"
        )
        print(
            "  python test_rate_limit_burst.py 30 10 3             # 30 files, 10s cooldown, 3 batches"
        )
