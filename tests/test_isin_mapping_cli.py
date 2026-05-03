"""Tests for the update-isin-mapping CLI command."""

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from typer.testing import CliRunner

from tests.conftest import strip_ansi

from yf_parqed.xetra_cli import app

runner = CliRunner(env={"NO_COLOR": "1"})


def _make_updater_mock(tmp_path: Path) -> MagicMock:
    """Return a mock ISINMappingUpdater that writes a minimal Parquet on run()."""

    def fake_run(cache_path: Path) -> None:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [
                {
                    "isin": "DE0005140008",
                    "ticker": "DBK",
                    "name": "DEUTSCHE BANK",
                    "currency": "EUR",
                    "wkn": "514000",
                    "status": "active",
                    "first_seen": date.today(),
                    "last_seen": date.today(),
                    "source": "deutsche_boerse_csv",
                }
            ]
        ).to_parquet(cache_path, index=False)

    mock_instance = MagicMock()
    mock_instance.__enter__ = MagicMock(return_value=mock_instance)
    mock_instance.__exit__ = MagicMock(return_value=False)
    mock_instance.run.side_effect = fake_run
    return mock_instance


# ---------------------------------------------------------------------------
# One-shot mode
# ---------------------------------------------------------------------------


def test_update_isin_mapping_one_shot(tmp_path):
    mock_instance = _make_updater_mock(tmp_path)

    with patch("yf_parqed.xetra_cli.ISINMappingUpdater", return_value=mock_instance):
        result = runner.invoke(app, ["--wrk-dir", str(tmp_path), "update-isin-mapping"])

    assert result.exit_code == 0
    assert "ISIN mapping updated" in result.output
    mock_instance.run.assert_called_once()


def test_update_isin_mapping_help():
    result = runner.invoke(app, ["update-isin-mapping", "--help"])
    assert result.exit_code == 0
    clean = strip_ansi(result.output)
    assert "--force" in clean
    assert "--dry-run" in clean
    assert "--daemon" in clean


# ---------------------------------------------------------------------------
# --force flag bypasses cache age check
# ---------------------------------------------------------------------------


def test_update_isin_mapping_force_bypasses_fresh_cache(tmp_path):
    """--force must run even if the cache was written seconds ago."""
    cache_path = tmp_path / "data" / "reference" / "isin_mapping.parquet"
    cache_path.parent.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "isin": "DE0005140008",
                "ticker": "DBK",
                "name": "DEUTSCHE BANK",
                "currency": "EUR",
                "wkn": "514000",
                "status": "active",
                "first_seen": date.today(),
                "last_seen": date.today(),
                "source": "deutsche_boerse_csv",
            }
        ]
    ).to_parquet(cache_path, index=False)

    mock_instance = _make_updater_mock(tmp_path)

    with patch("yf_parqed.xetra_cli.ISINMappingUpdater", return_value=mock_instance):
        result = runner.invoke(
            app, ["--wrk-dir", str(tmp_path), "update-isin-mapping", "--force"]
        )

    assert result.exit_code == 0
    mock_instance.run.assert_called_once()


# ---------------------------------------------------------------------------
# Fresh cache is skipped without --force
# ---------------------------------------------------------------------------


def test_update_isin_mapping_skips_fresh_cache(tmp_path):
    """Without --force, a cache written <24h ago should be skipped."""
    cache_path = tmp_path / "data" / "reference" / "isin_mapping.parquet"
    cache_path.parent.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "isin": "DE0005140008",
                "ticker": "DBK",
                "name": "DEUTSCHE BANK",
                "currency": "EUR",
                "wkn": "514000",
                "status": "active",
                "first_seen": date.today(),
                "last_seen": date.today(),
                "source": "deutsche_boerse_csv",
            }
        ]
    ).to_parquet(cache_path, index=False)

    mock_instance = _make_updater_mock(tmp_path)

    with patch("yf_parqed.xetra_cli.ISINMappingUpdater", return_value=mock_instance):
        result = runner.invoke(app, ["--wrk-dir", str(tmp_path), "update-isin-mapping"])

    assert result.exit_code == 0
    assert "fresh" in result.output.lower() or "skip" in result.output.lower()
    mock_instance.run.assert_not_called()


# ---------------------------------------------------------------------------
# --dry-run
# ---------------------------------------------------------------------------


def test_update_isin_mapping_dry_run_does_not_write(tmp_path):
    """--dry-run must not write any files."""
    csv_url = "https://www.xetra.com/fake.csv"
    mock_instance = MagicMock()
    mock_instance.__enter__ = MagicMock(return_value=mock_instance)
    mock_instance.__exit__ = MagicMock(return_value=False)
    mock_instance.get_csv_url.return_value = csv_url
    mock_instance.download_and_parse.return_value = pd.DataFrame(
        [
            {
                "isin": "DE0005140008",
                "ticker": "DBK",
                "name": "DEUTSCHE BANK",
                "currency": "EUR",
                "wkn": "514000",
                "status": "active",
                "last_seen": date.today(),
                "source": "deutsche_boerse_csv",
            }
        ]
    )
    mock_instance.merge_with_cache.return_value = pd.DataFrame(
        [
            {
                "isin": "DE0005140008",
                "ticker": "DBK",
                "name": "DEUTSCHE BANK",
                "currency": "EUR",
                "wkn": "514000",
                "status": "active",
                "first_seen": date.today(),
                "last_seen": date.today(),
                "source": "deutsche_boerse_csv",
            }
        ]
    )

    with patch("yf_parqed.xetra_cli.ISINMappingUpdater", return_value=mock_instance):
        result = runner.invoke(
            app, ["--wrk-dir", str(tmp_path), "update-isin-mapping", "--dry-run"]
        )

    assert result.exit_code == 0
    assert "dry-run" in result.output.lower()

    cache_path = tmp_path / "data" / "reference" / "isin_mapping.parquet"
    assert not cache_path.exists()


# ---------------------------------------------------------------------------
# wrk-dir wiring
# ---------------------------------------------------------------------------


def test_update_isin_mapping_uses_wrk_dir(tmp_path):
    """Verify cache_path is constructed under wrk_dir."""
    captured_paths: list[Path] = []

    def fake_run(cache_path: Path) -> None:
        captured_paths.append(cache_path)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            columns=[
                "isin",
                "ticker",
                "name",
                "currency",
                "wkn",
                "status",
                "first_seen",
                "last_seen",
                "source",
            ]
        ).to_parquet(cache_path, index=False)

    mock_instance = MagicMock()
    mock_instance.__enter__ = MagicMock(return_value=mock_instance)
    mock_instance.__exit__ = MagicMock(return_value=False)
    mock_instance.run.side_effect = fake_run

    with patch("yf_parqed.xetra_cli.ISINMappingUpdater", return_value=mock_instance):
        result = runner.invoke(app, ["--wrk-dir", str(tmp_path), "update-isin-mapping"])

    assert result.exit_code == 0
    assert len(captured_paths) == 1
    assert captured_paths[0] == tmp_path / "data" / "reference" / "isin_mapping.parquet"
