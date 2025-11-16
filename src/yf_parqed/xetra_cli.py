import typer
from pathlib import Path
from loguru import logger
import sys
from typing_extensions import Annotated

app = typer.Typer()


@app.callback()
def main(
    wrk_dir: Annotated[
        Path, typer.Option(help="Working directory, default is current directory")
    ] = Path.cwd(),
    log_level: Annotated[str, typer.Option(help="Log level")] = "INFO",
):
    """
    Xetra delayed data CLI - Deutsche Börse parquet storage.
    Use --wrk-dir to set working directory, --log-level to set logging verbosity.
    """
    logger.remove()
    logger.add(sys.stderr, level=log_level)


@app.command()
def fetch_trades(
    venue: Annotated[
        str,
        typer.Argument(
            help=(
                "Xetra venue code:\n"
                "  DETR = Xetra (main Frankfurt electronic exchange)\n"
                "  DFRA = Frankfurt Stock Exchange (floor trading)\n"
                "  DGAT = Xetra Gateways\n"
                "  DEUR = Eurex (derivatives exchange)"
            )
        ),
    ],
    no_store: Annotated[
        bool, typer.Option("--no-store", help="Display only, don't store")
    ] = False,
):
    """
    Intelligently fetch and store Xetra trades for a venue.

    Automatically determines what dates to fetch based on:
    - What's available from Deutsche Börse API (~24 hours of data)
    - What's already stored locally

    Only fetches and stores missing data. This is the recommended way to collect
    daily Xetra trade data.

    Data Type:
      Currently fetches POSTTRADE data (executed trades) for OHLCV aggregation.
      PRETRADE data (order book snapshots) is planned for future implementation.

    Venue codes:
      • DETR - Xetra (main Frankfurt electronic exchange)
      • DFRA - Frankfurt Stock Exchange (floor trading)
      • DGAT - Xetra Gateways
      • DEUR - Eurex (derivatives exchange)

    Examples:
        xetra-parqed fetch-trades DETR              # Fetch missing data for Xetra
        xetra-parqed fetch-trades DETR --no-store   # Check what's available (dry run)
        xetra-parqed fetch-trades DEUR              # Fetch Eurex derivatives data
    """
    from .xetra_service import XetraService

    # Market and source are fixed for Xetra
    market = "de"
    source = "xetra"

    with XetraService() as service:
        if no_store:
            # Dry run mode - just show what would be fetched
            logger.info(f"Checking missing dates for {venue} (dry run mode)")
            missing_dates = service.get_missing_dates(venue, market, source)

            if not missing_dates:
                typer.echo(f"✓ All available data already stored for {venue}")
            else:
                typer.echo(f"Would fetch {len(missing_dates)} date(s) for {venue}:")
                for date in missing_dates:
                    typer.echo(f"  - {date}")
                typer.echo("\nRemove --no-store to fetch and store this data")
        else:
            # Actually fetch and store (using incremental mode for interrupt safety)
            summary = service.fetch_and_store_missing_trades_incremental(
                venue, market, source
            )

            if summary["total_trades"] == 0:
                typer.echo(f"✓ All available data already stored for {venue}")
            else:
                typer.echo(f"\n✓ Fetched and stored trades for {venue}:")
                if summary["dates_fetched"]:
                    typer.echo(
                        f"  - Completed dates: {', '.join(summary['dates_fetched'])}"
                    )
                if summary["dates_partial"]:
                    typer.echo(
                        f"  - Partial dates: {', '.join(summary['dates_partial'])}"
                    )
                typer.echo(f"  - Total trades: {summary['total_trades']:,}")
                typer.echo(f"  - Total files: {summary['total_files']}")

                if summary["consolidated"]:
                    typer.echo("\n📦 Monthly consolidation completed")
                    typer.echo(
                        f"  Daily files preserved in: data/{market}/{source}/trades/venue={venue}/..."
                    )
                    typer.echo(
                        f"  Monthly file: data/{market}/{source}/trades_monthly/venue={venue}/..."
                    )

                if summary.get("dates_partial"):
                    typer.echo(
                        "\n⚠ Process had partial downloads - progress has been saved"
                    )
                    typer.echo("Re-run the command to resume from where you left off")


@app.command()
def check_status(
    venue: Annotated[
        str,
        typer.Argument(
            help=(
                "Xetra venue code:\n"
                "  DETR = Xetra (main Frankfurt electronic exchange)\n"
                "  DFRA = Frankfurt Stock Exchange (floor trading)\n"
                "  DGAT = Xetra Gateways\n"
                "  DEUR = Eurex (derivatives exchange)"
            )
        ),
    ],
):
    """
    Check what data is available vs what's already stored for a venue.

    Shows which dates have data available from Deutsche Börse and which are
    already stored locally.

    Data Type:
      Currently checks POSTTRADE data (executed trades) only.
      PRETRADE data (order book) support planned for future.

    Venue codes:
      • DETR - Xetra (main Frankfurt electronic exchange)
      • DFRA - Frankfurt Stock Exchange (floor trading)
      • DGAT - Xetra Gateways
      • DEUR - Eurex (derivatives exchange)

    Examples:
        xetra-parqed check-status DETR    # Check Xetra status
        xetra-parqed check-status DEUR    # Check Eurex status
    """
    from datetime import datetime, timedelta
    from .xetra_service import XetraService

    market = "de"
    source = "xetra"

    service = XetraService()

    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    typer.echo(f"\nStatus for {venue}:")
    typer.echo("-" * 50)

    for check_date in [today, yesterday]:
        date_str = check_date.strftime("%Y-%m-%d")

        # Check API availability
        try:
            files = service.list_files(venue, date_str)
            api_status = f"✓ {len(files)} files available" if files else "✗ No files"
        except Exception as e:
            api_status = f"✗ Error: {e}"

        # Check local storage
        trade_date = datetime.strptime(date_str, "%Y-%m-%d")
        year = trade_date.year
        month = f"{trade_date.month:02d}"
        day = f"{trade_date.day:02d}"

        base_dir = (
            service.backend._path_builder._root
            / market
            / source
            / "trades"
            / f"venue={venue}"
            / f"year={year}"
            / f"month={month}"
            / f"day={day}"
        )
        parquet_path = base_dir / "trades.parquet"
        storage_status = "✓ Stored locally" if parquet_path.exists() else "✗ Not stored"

        typer.echo(f"\n{date_str}:")
        typer.echo(f"  API:     {api_status}")
        typer.echo(f"  Storage: {storage_status}")

    service.close()


@app.command()
def list_files(
    venue: Annotated[
        str,
        typer.Argument(
            help=(
                "Xetra venue code:\n"
                "  DETR = Xetra (main Frankfurt electronic exchange)\n"
                "  DFRA = Frankfurt Stock Exchange (floor trading)\n"
                "  DGAT = Xetra Gateways\n"
                "  DEUR = Eurex (derivatives exchange)"
            )
        ),
    ],
    date: Annotated[
        str | None, typer.Option(help="Trade date (YYYY-MM-DD, default: today)")
    ] = None,
):
    """
    List available trade files for a venue/date.

    Data Type:
      Currently lists POSTTRADE files (executed trades) only.
      PRETRADE files (order book) support planned for future.

    Venue codes:
      • DETR - Xetra (main Frankfurt electronic exchange)
      • DFRA - Frankfurt Stock Exchange (floor trading)
      • DGAT - Xetra Gateways
      • DEUR - Eurex (derivatives exchange)

    Examples:
        xetra-parqed list-files DETR                    # Today's Xetra files
        xetra-parqed list-files DETR --date 2025-11-01  # Specific date
        xetra-parqed list-files DEUR                    # Today's Eurex files
    """
    from datetime import datetime
    from .xetra_service import XetraService

    # Default to today if no date provided
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    service = XetraService()
    files = service.list_files(venue, date)
    if files:
        typer.echo(f"Found {len(files)} files for {venue} on {date}:")
        for filename in files:
            typer.echo(f"  - {filename}")
    else:
        typer.echo(f"No files found for {venue} on {date}")


@app.command()
def check_partial(
    venue: Annotated[
        str,
        typer.Argument(
            help=(
                "Xetra venue code:\n"
                "  DETR = Xetra (main Frankfurt electronic exchange)\n"
                "  DFRA = Frankfurt Stock Exchange (floor trading)\n"
                "  DGAT = Xetra Gateways\n"
                "  DEUR = Eurex (derivatives exchange)"
            )
        ),
    ],
    market: Annotated[str, typer.Option(help="Market code")] = "de",
    source: Annotated[str, typer.Option(help="Source code")] = "xetra",
):
    """
    Check for partial/interrupted downloads that can be resumed or consolidated.

    Scans stored data to identify:
    - Dates with complete downloads (have parquet files)
    - Dates with partial/empty downloads (interrupted)
    - Months ready for consolidation

    Use this after an interrupted download to see what needs to be resumed.

    Examples:
        xetra-parqed check-partial DETR         # Check Xetra download status
        xetra-parqed check-partial DFRA         # Check Frankfurt status
    """
    from .xetra_service import XetraService

    service = XetraService()
    status = service.check_partial_downloads(venue, market, source)

    typer.echo(f"\n📊 Download Status for {venue}:\n")

    if status["complete_dates"]:
        typer.echo(f"✓ Complete dates ({len(status['complete_dates'])}):")
        for date in status["complete_dates"][-10:]:  # Show last 10
            typer.echo(f"  - {date}")
        if len(status["complete_dates"]) > 10:
            typer.echo(f"  ... and {len(status['complete_dates']) - 10} more")
    else:
        typer.echo("✓ No complete dates found")

    typer.echo()

    if status["partial_dates"]:
        typer.echo(f"⚠ Partial/empty dates ({len(status['partial_dates'])}):")
        for item in status["partial_dates"]:
            typer.echo(f"  - {item['date']}: {item['status']}")
        typer.echo("\n  💡 Re-run 'fetch-trades' to resume interrupted downloads")
    else:
        typer.echo("✓ No partial downloads found")

    typer.echo()

    if status["months_ready"]:
        typer.echo(
            f"📅 Months ready for consolidation ({len(status['months_ready'])}):"
        )
        for year, month in status["months_ready"]:
            typer.echo(f"  - {year}-{month:02d}")
        typer.echo("\n  💡 Use 'consolidate-month' to create monthly parquet files")
    else:
        typer.echo("✓ No months ready for consolidation")


@app.command()
def consolidate_month(
    venue: Annotated[
        str,
        typer.Argument(
            help=(
                "Xetra venue code:\n"
                "  DETR = Xetra (main Frankfurt electronic exchange)\n"
                "  DFRA = Frankfurt Stock Exchange (floor trading)\n"
                "  DGAT = Xetra Gateways\n"
                "  DEUR = Eurex (derivatives exchange)"
            )
        ),
    ],
    all_months: Annotated[
        bool, typer.Option("--all", help="Consolidate all available months")
    ] = False,
    market: Annotated[str, typer.Option(help="Market code")] = "de",
    source: Annotated[str, typer.Option(help="Source code")] = "xetra",
):
    """
    Consolidate daily files into optimized monthly parquet files.

    Auto-detects years/months from stored daily files. By default, shows what would
    be consolidated and prompts for confirmation. Use --all to consolidate all
    available months without prompting.

    This reads all daily parquet files for each month and combines them into a
    single monthly file with optimal compression and row group sizing.

    Monthly files are written to:
      data/{market}/{source}/trades_monthly/venue={venue}/year={year}/month={month}/trades.parquet

    Daily files are preserved for safety (can be manually deleted after verification).

    File size estimate: ~86 MB/day × 22 trading days = ~1.9 GB/month

    Examples:
        xetra-parqed consolidate-month DETR           # Interactive consolidation
        xetra-parqed consolidate-month DETR --all     # Consolidate all months
    """
    from .xetra_service import XetraService

    service = XetraService()

    # Auto-detect months from stored data
    status = service.check_partial_downloads(venue, market, source)

    if not status["months_ready"]:
        typer.echo(f"✓ No months found with daily data for {venue}")
        typer.echo("  Run 'fetch-trades' first to download data")
        return

    months_to_consolidate = status["months_ready"]

    typer.echo(
        f"\n📦 Found {len(months_to_consolidate)} month(s) ready for consolidation:\n"
    )
    for year, month in months_to_consolidate:
        typer.echo(f"  - {year}-{month:02d}")

    if not all_months:
        typer.echo("\nℹ️  Use --all to consolidate all months, or Ctrl+C to cancel")
        confirm = typer.confirm("\nConsolidate these months?", default=True)
        if not confirm:
            typer.echo("Cancelled")
            return

    # Consolidate each month
    success_count = 0
    fail_count = 0

    for year, month in months_to_consolidate:
        try:
            typer.echo(f"\n📊 Consolidating {venue} {year}-{month:02d}...")
            service._consolidate_to_monthly(venue, year, month, market, source)
            typer.echo("   ✓ Success")
            success_count += 1
        except Exception as e:
            typer.echo(f"   ❌ Failed: {e}", err=True)
            fail_count += 1

    typer.echo(f"\n{'='*60}")
    typer.echo(
        f"Consolidation complete: {success_count} succeeded, {fail_count} failed"
    )

    if success_count > 0:
        typer.echo(
            f"\n✓ Monthly files: data/{market}/{source}/trades_monthly/venue={venue}/..."
        )
        typer.echo(
            f"  Daily files preserved in: data/{market}/{source}/trades/venue={venue}/..."
        )
