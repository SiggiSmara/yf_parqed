from pathlib import Path
from typer.testing import CliRunner

from yf_parqed.main import app as cli_app

from yf_parqed.run_lock import GlobalRunLock

runner = CliRunner()


def test_run_lock_status_and_cleanup(tmp_path: Path):
    # No lock present
    result = runner.invoke(cli_app, ["run-lock", "status", "--base-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "No run lock present" in result.output

    # Create lock and owner file
    lock = GlobalRunLock(tmp_path)
    assert lock.try_acquire()
    info = lock.owner_info()
    assert isinstance(info, dict)

    result = runner.invoke(cli_app, ["run-lock", "status", "--base-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "pid" in result.output

    # create a tmp file and test cleanup non-interactive
    data_dir = tmp_path / "data/us/yahoo/stocks_1d/ticker=EEE/year=2024/month=06"
    data_dir.mkdir(parents=True, exist_ok=True)
    tmpf = data_dir / f"data.parquet.tmp-{12345}-1"
    tmpf.write_text("tmp")

    result = runner.invoke(
        cli_app,
        ["run-lock", "cleanup", "--base-dir", str(tmp_path), "--non-interactive"],
    )
    assert result.exit_code == 0
    assert "Processed" in result.output
    assert not tmpf.exists()
