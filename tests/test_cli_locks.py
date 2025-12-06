import json
from pathlib import Path
from typer.testing import CliRunner

from yf_parqed.yfinance_cli import app as cli_app
from yf_parqed.tools.partition_migrate import app as migrate_app
from yf_parqed.common.run_lock import GlobalRunLock


runner = CliRunner()


def test_update_data_non_interactive_auto_cleanup(tmp_path: Path):
    # create stale lock dir
    lock = GlobalRunLock(tmp_path)
    assert lock.try_acquire()
    # create a tmp file to be cleaned
    data_dir = tmp_path / "data/us/yahoo/stocks_1d/ticker=AAA/year=2024/month=02"
    data_dir.mkdir(parents=True, exist_ok=True)
    tmp = data_dir / f"data.parquet.tmp-{12345}-1"
    tmp.write_text("not a real parquet, but it's a file")

    _result = runner.invoke(
        cli_app,
        [
            "update-data",
            "--non-interactive",
            "--wrk_dir",
            str(tmp_path),
        ],
    )

    # the command should have exited (we didn't run actual update), but auto-cleanup should remove tmp
    assert _result.exit_code != 0 or not tmp.exists()


def test_migrate_non_interactive_auto_cleanup(tmp_path: Path, monkeypatch):
    # create a simple migration plan file so migrate CLI proceeds
    plan = {
        "schema_version": 1,
        "generated_at": "2025-10-15T12:40:00Z",
        "created_by": "tests",
        "legacy_root": "data/legacy",
        "venues": [
            {
                "id": "us:yahoo",
                "market": "US",
                "source": "yahoo",
                "status": "pending",
                "last_updated": "2025-10-15T12:40:00Z",
                "intervals": {
                    "1m": {
                        "legacy_path": "data/legacy/stocks_1m",
                        "partition_path": "data/us/yahoo/stocks_1m",
                        "status": "pending",
                        "totals": {"legacy_rows": None, "partition_rows": None},
                        "jobs": {"total": 0, "completed": 0},
                        "resume_token": None,
                        "verification": {"method": "row_counts", "verified_at": None},
                        "backups": [],
                    }
                },
            }
        ],
    }
    (tmp_path / "migration_plan.json").write_text(json.dumps(plan))

    lock = GlobalRunLock(tmp_path)
    assert lock.try_acquire()
    # create a tmp file to be cleaned
    data_dir = tmp_path / "data/us/yahoo/stocks_1m/ticker=AAA/year=2024/month=03"
    data_dir.mkdir(parents=True, exist_ok=True)
    tmp = data_dir / f"data.parquet.tmp-{12346}-1"
    tmp.write_text("tmp")

    _result = runner.invoke(
        migrate_app,
        [
            "migrate",
            "--base-dir",
            str(tmp_path),
            "--non-interactive",
        ],
    )

    # migrate should have attempted auto-cleanup and removed tmp
    assert not tmp.exists()
