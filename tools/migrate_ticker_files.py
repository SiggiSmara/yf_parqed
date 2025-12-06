from pathlib import Path
import json
from datetime import datetime
from loguru import logger


def migrate_ticker_files(working_dir: Path = None):
    """
    Migrate existing current_tickers.json and not_found_tickers.json files
    into the new unified tickers.json format with interval-aware structure.
    """
    if working_dir is None:
        working_dir = Path.cwd()

    current_tickers_path = working_dir / "current_tickers.json"
    not_found_tickers_path = working_dir / "not_found_tickers.json"
    new_tickers_path = working_dir / "tickers.json"

    # Check if migration is needed
    if new_tickers_path.exists():
        logger.warning(
            f"Target file {new_tickers_path} already exists. Skipping migration."
        )
        return

    if not current_tickers_path.exists() and not not_found_tickers_path.exists():
        logger.warning("No ticker files found to migrate.")
        return

    unified_tickers = {}
    current_date = datetime.now().strftime("%Y-%m-%d")

    # Load current tickers (active) - assume verified with 1d interval
    if current_tickers_path.exists():
        logger.info(f"Loading current tickers from {current_tickers_path}")
        try:
            current_tickers = json.loads(current_tickers_path.read_text())
            logger.info(f"Found {len(current_tickers)} current tickers")

            for ticker, data in current_tickers.items():
                # Create new interval-aware structure
                ticker_data = {
                    "ticker": ticker,
                    "added_date": data.get("added_date", current_date),
                    "status": "active",
                    "last_checked": data.get("last_checked", current_date),
                    "intervals": {
                        "1d": {
                            "status": "active",
                            "last_found_date": current_date,
                            "last_data_date": data.get("added_date", current_date),
                            "last_checked": current_date,
                        }
                    },
                }

                # Preserve any additional fields from original data
                for key, value in data.items():
                    if key not in ["ticker", "added_date", "status", "last_checked"]:
                        ticker_data[key] = value

                unified_tickers[ticker] = ticker_data

        except Exception as e:
            logger.error(f"Error loading current tickers: {e}")

    # Load not found tickers
    if not_found_tickers_path.exists():
        logger.info(f"Loading not found tickers from {not_found_tickers_path}")
        try:
            not_found_tickers = json.loads(not_found_tickers_path.read_text())
            logger.info(f"Found {len(not_found_tickers)} not found tickers")

            for ticker, data in not_found_tickers.items():
                # Create new interval-aware structure for not found tickers
                # Keep them as globally not found as they were in the not found list
                ticker_data = {
                    "ticker": ticker,
                    "added_date": data.get("added_date", current_date),
                    "status": "not_found",
                    "last_checked": data.get("last_checked", current_date),
                    "intervals": {},
                }

                # Create 1d interval data based on whether they had data before
                if data.get("last_date_found"):
                    # Had data before but then was marked as not found
                    ticker_data["intervals"]["1d"] = {
                        "status": "active",
                        "last_found_date": data["last_date_found"],
                        "last_data_date": data["last_date_found"],
                        "last_checked": data.get("last_checked", current_date),
                    }
                else:
                    # Never found any data for this ticker
                    ticker_data["intervals"]["1d"] = {
                        "status": "not_found",
                        "last_not_found_date": data.get("last_checked", current_date),
                        "last_checked": data.get("last_checked", current_date),
                    }

                # Preserve any additional fields from original data
                for key, value in data.items():
                    if key not in [
                        "ticker",
                        "added_date",
                        "status",
                        "last_checked",
                        "last_date_found",
                    ]:
                        ticker_data[key] = value

                unified_tickers[ticker] = ticker_data

        except Exception as e:
            logger.error(f"Error loading not found tickers: {e}")

    # Save unified tickers
    if unified_tickers:
        logger.info(f"Saving {len(unified_tickers)} tickers to {new_tickers_path}")
        new_tickers_path.write_text(json.dumps(unified_tickers, indent=4))

        # Create backup of original files
        backup_dir = working_dir / "backup_ticker_files"
        backup_dir.mkdir(exist_ok=True)

        if current_tickers_path.exists():
            backup_current = (
                backup_dir
                / f"current_tickers_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            backup_current.write_text(current_tickers_path.read_text())
            logger.info(f"Backed up current tickers to {backup_current}")

        if not_found_tickers_path.exists():
            backup_not_found = (
                backup_dir
                / f"not_found_tickers_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            backup_not_found.write_text(not_found_tickers_path.read_text())
            logger.info(f"Backed up not found tickers to {backup_not_found}")

        logger.info("Migration completed successfully!")

        # Print summary
        active_count = sum(
            1 for data in unified_tickers.values() if data.get("status") == "active"
        )
        not_found_count = sum(
            1 for data in unified_tickers.values() if data.get("status") == "not_found"
        )
        with_1d_data_count = sum(
            1
            for data in unified_tickers.values()
            if data.get("intervals", {}).get("1d", {}).get("status") == "active"
        )

        print("\nMigration Summary:")
        print(f"  Total tickers: {len(unified_tickers)}")
        print(f"  Active tickers: {active_count}")
        print(f"  Not found tickers: {not_found_count}")
        print(f"  Tickers with 1d interval data: {with_1d_data_count}")
        print(f"  Unified file created: {new_tickers_path}")
        print(f"  Backups created in: {backup_dir}")

    else:
        logger.warning("No ticker data found to migrate.")


def verify_migration(working_dir: Path = None):
    """
    Verify the migration was successful by comparing data integrity.
    """
    if working_dir is None:
        working_dir = Path.cwd()

    new_tickers_path = working_dir / "tickers.json"

    if not new_tickers_path.exists():
        logger.error("No unified tickers.json file found to verify.")
        return False

    try:
        unified_tickers = json.loads(new_tickers_path.read_text())

        # Check data structure
        required_fields = {
            "ticker",
            "added_date",
            "status",
            "last_checked",
            "intervals",
        }
        valid_statuses = {"active", "not_found"}
        valid_interval_statuses = {"active", "not_found"}

        for ticker, data in unified_tickers.items():
            if not isinstance(data, dict):
                logger.error(f"Invalid data structure for ticker {ticker}")
                return False

            missing_fields = required_fields - set(data.keys())
            if missing_fields:
                logger.error(
                    f"Missing required fields for ticker {ticker}: {missing_fields}"
                )
                return False

            if data.get("status") not in valid_statuses:
                logger.error(
                    f"Invalid status for ticker {ticker}: {data.get('status')}"
                )
                return False

            if data.get("ticker") != ticker:
                logger.error(
                    f"Ticker field mismatch for {ticker}: {data.get('ticker')}"
                )
                return False

            # Check intervals structure
            intervals = data.get("intervals", {})
            if not isinstance(intervals, dict):
                logger.error(f"Invalid intervals structure for ticker {ticker}")
                return False

            # Validate interval data
            for interval_name, interval_data in intervals.items():
                if not isinstance(interval_data, dict):
                    logger.error(f"Invalid interval data for {ticker}:{interval_name}")
                    return False

                if "status" not in interval_data:
                    logger.error(
                        f"Missing status in interval data for {ticker}:{interval_name}"
                    )
                    return False

                if interval_data["status"] not in valid_interval_statuses:
                    logger.error(
                        f"Invalid interval status for {ticker}:{interval_name}: {interval_data['status']}"
                    )
                    return False

                if interval_data["status"] == "active":
                    required_active_fields = {"last_found_date", "last_checked"}
                    missing_active_fields = required_active_fields - set(
                        interval_data.keys()
                    )
                    if missing_active_fields:
                        logger.error(
                            f"Missing active interval fields for {ticker}:{interval_name}: {missing_active_fields}"
                        )
                        return False

                if interval_data["status"] == "not_found":
                    required_not_found_fields = {"last_not_found_date", "last_checked"}
                    missing_not_found_fields = required_not_found_fields - set(
                        interval_data.keys()
                    )
                    if missing_not_found_fields:
                        logger.error(
                            f"Missing not_found interval fields for {ticker}:{interval_name}: {missing_not_found_fields}"
                        )
                        return False

        logger.info(
            f"Migration verification successful! {len(unified_tickers)} tickers validated."
        )
        return True

    except Exception as e:
        logger.error(f"Error verifying migration: {e}")
        return False


if __name__ == "__main__":
    import typer

    app = typer.Typer()

    @app.command()
    def migrate(
        directory: str = typer.Option(
            ".", "--dir", "-d", help="Directory containing the ticker JSON files"
        ),
        verify: bool = typer.Option(
            True, "--verify/--no-verify", help="Verify migration after completion"
        ),
    ):
        """Migrate existing ticker JSON files to unified format."""
        working_dir = Path(directory).resolve()

        if not working_dir.exists():
            typer.echo(f"Directory {working_dir} does not exist.", err=True)
            raise typer.Exit(1)

        logger.info(f"Starting migration in directory: {working_dir}")
        migrate_ticker_files(working_dir)

        if verify:
            logger.info("Verifying migration...")
            if verify_migration(working_dir):
                typer.echo("✅ Migration completed and verified successfully!")
            else:
                typer.echo("❌ Migration verification failed!", err=True)
                raise typer.Exit(1)
        else:
            typer.echo("✅ Migration completed!")

    @app.command()
    def verify_only(
        directory: str = typer.Option(
            ".", "--dir", "-d", help="Directory containing the tickers.json file"
        ),
    ):
        """Verify an existing tickers.json file."""
        working_dir = Path(directory).resolve()

        if verify_migration(working_dir):
            typer.echo("✅ Verification successful!")
        else:
            typer.echo("❌ Verification failed!", err=True)
            raise typer.Exit(1)

    app()
