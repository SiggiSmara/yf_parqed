from pathlib import Path
import json
from datetime import datetime
from loguru import logger


def fix_migrated_tickers(working_dir: Path = None):
    """
    Fix the existing migrated tickers.json file to ensure all required fields are present.
    """
    if working_dir is None:
        working_dir = Path.cwd()

    tickers_path = working_dir / "tickers.json"

    if not tickers_path.exists():
        logger.error("No tickers.json file found to fix.")
        return False

    current_date = datetime.now().strftime("%Y-%m-%d")

    try:
        tickers = json.loads(tickers_path.read_text())
        logger.info(f"Fixing {len(tickers)} tickers")

        fixed_count = 0
        for ticker, data in tickers.items():
            needs_fix = False

            # Ensure required fields exist
            if "last_checked" not in data:
                data["last_checked"] = data.get("added_date", current_date)
                needs_fix = True

            if "intervals" not in data:
                data["intervals"] = {}
                needs_fix = True

            # Ensure intervals structure is correct
            if not isinstance(data["intervals"], dict):
                data["intervals"] = {}
                needs_fix = True

            # Add 1d interval if missing
            if "1d" not in data["intervals"]:
                if data.get("status") == "not_found":
                    data["intervals"]["1d"] = {
                        "status": "not_found",
                        "last_not_found_date": data.get("last_checked", current_date),
                        "last_checked": data.get("last_checked", current_date),
                    }
                else:
                    data["intervals"]["1d"] = {
                        "status": "active",
                        "last_found_date": current_date,
                        "last_data_date": data.get("added_date", current_date),
                        "last_checked": current_date,
                    }
                needs_fix = True

            if needs_fix:
                fixed_count += 1

        if fixed_count > 0:
            # Create backup before fixing
            backup_path = (
                working_dir
                / f"tickers_backup_before_fix_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            backup_path.write_text(tickers_path.read_text())
            logger.info(f"Created backup: {backup_path}")

            # Write fixed data
            tickers_path.write_text(json.dumps(tickers, indent=4))
            logger.info(f"Fixed {fixed_count} tickers and saved to {tickers_path}")
        else:
            logger.info("No tickers needed fixing")

        return True

    except Exception as e:
        logger.error(f"Error fixing tickers: {e}")
        return False


if __name__ == "__main__":
    import typer

    app = typer.Typer()

    @app.command()
    def fix(
        directory: str = typer.Option(
            ".", "--dir", "-d", help="Directory containing the tickers.json file"
        ),
    ):
        """Fix an existing tickers.json file to ensure all required fields are present."""
        working_dir = Path(directory).resolve()

        if not working_dir.exists():
            typer.echo(f"Directory {working_dir} does not exist.", err=True)
            raise typer.Exit(1)

        logger.info(f"Fixing tickers in directory: {working_dir}")
        if fix_migrated_tickers(working_dir):
            typer.echo("✅ Fix completed successfully!")
        else:
            typer.echo("❌ Fix failed!", err=True)
            raise typer.Exit(1)

    app()
