"""Tests for Xetra CLI commands."""

from datetime import datetime
from unittest.mock import patch

from typer.testing import CliRunner

from yf_parqed.xetra_cli import app

runner = CliRunner()


def test_fetch_trades_help():
    """Verify fetch-trades --help works."""
    result = runner.invoke(app, ["fetch-trades", "--help"])
    assert result.exit_code == 0
    assert "venue" in result.output.lower()
    assert "xetra" in result.output.lower()  # Should show venue descriptions
    # Should NOT have --date anymore
    assert "--date" not in result.output


@patch(
    "yf_parqed.xetra.xetra_service.XetraService.fetch_and_store_missing_trades_incremental"
)
def test_fetch_trades_smart_default(mock_fetch_incremental):
    """Test fetch-trades with smart date detection (default behavior)."""
    # Mock the incremental fetch method
    mock_fetch_incremental.return_value = {
        "dates_checked": ["2025-11-04"],
        "dates_fetched": ["2025-11-04"],
        "dates_partial": [],
        "total_trades": 1500,
        "total_files": 100,
        "consolidated": True,
    }

    result = runner.invoke(app, ["fetch-trades", "DETR"])

    assert result.exit_code == 0
    assert "✓ Fetched and stored" in result.output
    assert "1,500" in result.output  # formatted with commas
    assert "100" in result.output  # total files
    mock_fetch_incremental.assert_called_once_with("DETR", "de", "xetra")


@patch("yf_parqed.xetra.xetra_service.XetraService.get_missing_dates")
def test_fetch_trades_no_store_dry_run(mock_get_missing):
    """Test fetch-trades with --no-store flag (dry run mode)."""
    # Mock missing dates check
    mock_get_missing.return_value = ["2025-11-04", "2025-11-03"]

    result = runner.invoke(app, ["fetch-trades", "DETR", "--no-store"])

    assert result.exit_code == 0
    assert "Would fetch 2 date(s)" in result.output
    assert "2025-11-04" in result.output
    assert "2025-11-03" in result.output
    assert "Remove --no-store" in result.output
    mock_get_missing.assert_called_once_with("DETR", "de", "xetra")


@patch(
    "yf_parqed.xetra.xetra_service.XetraService.fetch_and_store_missing_trades_incremental"
)
def test_fetch_trades_already_stored(mock_fetch_incremental):
    """Test fetch-trades when all data is already stored."""
    # Mock response showing nothing to fetch
    mock_fetch_incremental.return_value = {
        "dates_checked": [],
        "dates_fetched": [],
        "dates_partial": [],
        "total_trades": 0,
        "total_files": 0,
        "consolidated": False,
    }

    result = runner.invoke(app, ["fetch-trades", "DETR"])

    assert result.exit_code == 0
    assert "All available data already stored" in result.output


@patch("yf_parqed.xetra.xetra_service.XetraService.get_missing_dates")
def test_fetch_trades_no_store_already_stored(mock_get_missing):
    """Test --no-store when all data is already stored."""
    # Mock no missing dates
    mock_get_missing.return_value = []

    result = runner.invoke(app, ["fetch-trades", "DETR", "--no-store"])

    assert result.exit_code == 0
    assert "All available data already stored" in result.output


@patch(
    "yf_parqed.xetra.xetra_service.XetraService.fetch_and_store_missing_trades_incremental"
)
def test_fetch_trades_with_skipped_dates(mock_fetch_incremental):
    """Test fetch-trades when some dates are partial."""
    # Mock response with some partial dates
    mock_fetch_incremental.return_value = {
        "dates_checked": ["2025-11-04", "2025-11-03"],
        "dates_fetched": ["2025-11-04"],
        "dates_partial": ["2025-11-03"],
        "total_trades": 800,
        "total_files": 50,
        "consolidated": True,
    }

    result = runner.invoke(app, ["fetch-trades", "DETR"])

    assert result.exit_code == 0
    assert "✓ Fetched and stored" in result.output
    assert "⚠ Process had partial downloads" in result.output
    assert "2025-11-03" in result.output


@patch("yf_parqed.xetra.xetra_service.XetraService.list_files")
def test_check_status_command(mock_list_files):
    """Test check-status command."""
    from datetime import datetime
    today = datetime.now()
    mock_list_files.return_value = [
        f"DETR-posttrade-{today.strftime('%Y-%m-%d')}T10_00.json.gz",
        f"DETR-posttrade-{today.strftime('%Y-%m-%d')}T11_00.json.gz",
    ]

    result = runner.invoke(app, ["check-status", "DETR"])

    assert result.exit_code == 0
    assert "Status for DETR" in result.output
    # Should show today's date in output
    assert today.strftime("%Y-%m-%d") in result.output


@patch("yf_parqed.xetra.xetra_service.XetraService.list_files")
def test_list_files(mock_list_files):
    """Test list-files command."""
    mock_list_files.return_value = [
        "DETR-posttrade-2025-11-01T10_00.json.gz",
        "DETR-posttrade-2025-11-01T11_00.json.gz",
    ]

    result = runner.invoke(app, ["list-files", "DETR", "--date", "2025-11-01"])

    assert result.exit_code == 0
    assert "Found 2 files" in result.output
    assert "DETR-posttrade-2025-11-01T10_00.json.gz" in result.output
    assert "DETR-posttrade-2025-11-01T11_00.json.gz" in result.output


@patch("yf_parqed.xetra.xetra_service.XetraService.list_files")
def test_list_files_no_files(mock_list_files):
    """Test list-files when no files available."""
    mock_list_files.return_value = []

    result = runner.invoke(app, ["list-files", "DETR", "--date", "2025-11-01"])

    assert result.exit_code == 0
    assert "No files found" in result.output


def test_list_files_help():
    """Verify list-files --help works."""
    result = runner.invoke(app, ["list-files", "--help"])
    assert result.exit_code == 0
    assert "venue" in result.output.lower()
    assert "date" in result.output.lower()
    assert "xetra" in result.output.lower()  # Should show venue descriptions


@patch("yf_parqed.xetra.xetra_service.XetraService.list_files")
def test_list_files_default_date(mock_list_files):
    """Test list-files defaults to today's date when not specified."""
    mock_list_files.return_value = ["DETR-posttrade-file.json.gz"]

    # Don't specify --date, should default to today
    result = runner.invoke(app, ["list-files", "DETR"])

    assert result.exit_code == 0
    assert "Found 1 files" in result.output

    # Should be called with today's date
    today = datetime.now().strftime("%Y-%m-%d")
    mock_list_files.assert_called_once_with("DETR", today)


@patch("yf_parqed.xetra.xetra_service.XetraService.list_files")
def test_check_status_api_error(mock_list_files):
    """Test check-status handles API errors gracefully."""
    mock_list_files.side_effect = Exception("Connection timeout")

    result = runner.invoke(app, ["check-status", "DETR"])

    assert result.exit_code == 0
    assert "✗ Error: Connection timeout" in result.output


@patch("yf_parqed.xetra.xetra_service.XetraService.check_partial_downloads")
def test_check_partial_complete_dates(mock_check_partial):
    """Test check-partial shows complete dates."""
    mock_check_partial.return_value = {
        "complete_dates": ["2025-11-01", "2025-11-02", "2025-11-03"],
        "partial_dates": [],
        "months_ready": [],
    }

    result = runner.invoke(app, ["check-partial", "DETR"])

    assert result.exit_code == 0
    assert "Download Status for DETR" in result.output
    assert "✓ Complete dates (3)" in result.output
    assert "2025-11-01" in result.output


@patch("yf_parqed.xetra.xetra_service.XetraService.check_partial_downloads")
def test_check_partial_with_partial_dates(mock_check_partial):
    """Test check-partial shows partial downloads."""
    mock_check_partial.return_value = {
        "complete_dates": ["2025-11-01"],
        "partial_dates": [
            {"date": "2025-11-02", "status": "50 files (incomplete)"},
            {"date": "2025-11-03", "status": "0 files (empty)"},
        ],
        "months_ready": [],
    }

    result = runner.invoke(app, ["check-partial", "DETR"])

    assert result.exit_code == 0
    assert "⚠ Partial/empty dates (2)" in result.output
    assert "2025-11-02: 50 files (incomplete)" in result.output
    assert "2025-11-03: 0 files (empty)" in result.output
    assert "Re-run 'fetch-trades'" in result.output


@patch("yf_parqed.xetra.xetra_service.XetraService.check_partial_downloads")
def test_check_partial_months_ready(mock_check_partial):
    """Test check-partial shows months ready for consolidation."""
    mock_check_partial.return_value = {
        "complete_dates": ["2025-11-01"],
        "partial_dates": [],
        "months_ready": [(2025, 10), (2025, 11)],
    }

    result = runner.invoke(app, ["check-partial", "DETR"])

    assert result.exit_code == 0
    assert "📅 Months ready for consolidation (2)" in result.output
    assert "2025-10" in result.output
    assert "2025-11" in result.output
    assert "consolidate-month" in result.output


@patch("yf_parqed.xetra.xetra_service.XetraService.check_partial_downloads")
def test_check_partial_no_data(mock_check_partial):
    """Test check-partial when no data found."""
    mock_check_partial.return_value = {
        "complete_dates": [],
        "partial_dates": [],
        "months_ready": [],
    }

    result = runner.invoke(app, ["check-partial", "DETR"])

    assert result.exit_code == 0
    assert "✓ No complete dates found" in result.output
    assert "✓ No partial downloads found" in result.output
    assert "✓ No months ready for consolidation" in result.output


@patch("yf_parqed.xetra.xetra_service.XetraService.check_partial_downloads")
def test_check_partial_many_complete_dates(mock_check_partial):
    """Test check-partial truncates long list of complete dates."""
    # Generate 15 complete dates
    complete_dates = [f"2025-11-{i:02d}" for i in range(1, 16)]
    mock_check_partial.return_value = {
        "complete_dates": complete_dates,
        "partial_dates": [],
        "months_ready": [],
    }

    result = runner.invoke(app, ["check-partial", "DETR"])

    assert result.exit_code == 0
    assert "✓ Complete dates (15)" in result.output
    # Should show last 10 + "and X more" message
    assert "... and 5 more" in result.output


@patch("yf_parqed.xetra.xetra_service.XetraService.check_partial_downloads")
def test_consolidate_month_no_data(mock_check_partial):
    """Test consolidate-month when no months ready."""
    mock_check_partial.return_value = {
        "complete_dates": [],
        "partial_dates": [],
        "months_ready": [],
    }

    result = runner.invoke(app, ["consolidate-month", "DETR"])

    assert result.exit_code == 0
    assert "✓ No months found with daily data for DETR" in result.output
    assert "Run 'fetch-trades' first" in result.output


@patch("yf_parqed.xetra.xetra_service.XetraService._consolidate_to_monthly")
@patch("yf_parqed.xetra.xetra_service.XetraService.check_partial_downloads")
def test_consolidate_month_with_all_flag(mock_check_partial, mock_consolidate):
    """Test consolidate-month --all consolidates without prompting."""
    mock_check_partial.return_value = {
        "complete_dates": [],
        "partial_dates": [],
        "months_ready": [(2025, 10), (2025, 11)],
    }

    result = runner.invoke(app, ["consolidate-month", "DETR", "--all"])

    assert result.exit_code == 0
    assert "Found 2 month(s) ready for consolidation" in result.output
    assert "2025-10" in result.output
    assert "2025-11" in result.output
    assert "✓ Success" in result.output
    assert "Consolidation complete: 2 succeeded, 0 failed" in result.output
    # Should call consolidate twice (once per month)
    assert mock_consolidate.call_count == 2


@patch("yf_parqed.xetra.xetra_service.XetraService._consolidate_to_monthly")
@patch("yf_parqed.xetra.xetra_service.XetraService.check_partial_downloads")
def test_consolidate_month_interactive_confirm(mock_check_partial, mock_consolidate):
    """Test consolidate-month interactive mode with confirmation."""
    mock_check_partial.return_value = {
        "complete_dates": [],
        "partial_dates": [],
        "months_ready": [(2025, 10)],
    }

    # Simulate user confirming with 'y'
    result = runner.invoke(app, ["consolidate-month", "DETR"], input="y\n")

    assert result.exit_code == 0
    assert "Consolidate these months?" in result.output
    assert "✓ Success" in result.output
    mock_consolidate.assert_called_once()


@patch("yf_parqed.xetra.xetra_service.XetraService.check_partial_downloads")
def test_consolidate_month_interactive_cancel(mock_check_partial):
    """Test consolidate-month interactive mode with cancellation."""
    mock_check_partial.return_value = {
        "complete_dates": [],
        "partial_dates": [],
        "months_ready": [(2025, 10)],
    }

    # Simulate user canceling with 'n'
    result = runner.invoke(app, ["consolidate-month", "DETR"], input="n\n")

    assert result.exit_code == 0
    assert "Cancelled" in result.output


@patch("yf_parqed.xetra.xetra_service.XetraService._consolidate_to_monthly")
@patch("yf_parqed.xetra.xetra_service.XetraService.check_partial_downloads")
def test_consolidate_month_with_errors(mock_check_partial, mock_consolidate):
    """Test consolidate-month handles errors gracefully."""
    mock_check_partial.return_value = {
        "complete_dates": [],
        "partial_dates": [],
        "months_ready": [(2025, 10), (2025, 11)],
    }

    # First succeeds, second fails
    mock_consolidate.side_effect = [None, Exception("Write error")]

    result = runner.invoke(app, ["consolidate-month", "DETR", "--all"])

    assert result.exit_code == 0
    assert "✓ Success" in result.output
    assert "❌ Failed: Write error" in result.output
    assert "Consolidation complete: 1 succeeded, 1 failed" in result.output
