"""Tests for XetraFetcher class."""

import gzip
import json
import pytest
import httpx
from unittest.mock import Mock, patch
from yf_parqed.xetra.xetra_fetcher import XetraFetcher


class TestXetraFetcher:
    """Test suite for XetraFetcher HTTP client."""

    def test_initialization(self):
        """Test fetcher initializes with default base URL."""
        fetcher = XetraFetcher()
        assert fetcher.base_url == "https://mfs.deutsche-boerse.com/api/"
        assert isinstance(fetcher.client, httpx.Client)
        fetcher.close()

    def test_custom_base_url(self):
        """Test fetcher accepts custom base URL."""
        custom_url = "https://test.example.com/api/"
        fetcher = XetraFetcher(base_url=custom_url)
        assert fetcher.base_url == custom_url
        fetcher.close()

    def test_context_manager(self):
        """Test fetcher works as context manager."""
        with XetraFetcher() as fetcher:
            assert fetcher.client is not None
        # Client should be closed after exiting context

    @patch("httpx.Client.get")
    def test_list_available_files_success(self, mock_get):
        """Test successful file listing for both posttrade and pretrade."""
        # Mock posttrade response with SourcePrefix
        posttrade_response = Mock()
        posttrade_response.status_code = 200
        posttrade_response.json.return_value = {
            "SourcePrefix": "DETR-posttrade-2025-10-31",
            "CurrentFiles": [
                "DETR-posttrade-2025-10-31-2025-10-31T13_54.json.gz",
                "DETR-posttrade-2025-10-31-2025-10-31T14_23.json.gz",
            ],
        }

        # Mock response returns posttrade data
        mock_get.return_value = posttrade_response

        with XetraFetcher() as fetcher:
            files = fetcher.list_available_files("DETR")

        assert len(files) == 2
        assert "DETR-posttrade-2025-10-31T13_54.json.gz" in files
        assert "DETR-posttrade-2025-10-31T14_23.json.gz" in files

        # Verify correct URL was called (without date parameter)
        assert mock_get.call_count == 1
        calls = mock_get.call_args_list
        assert calls[0][0][0] == "https://mfs.deutsche-boerse.com/api/DETR-posttrade"

    @patch("httpx.Client.get")
    def test_list_available_files_all_venues(self, mock_get):
        """Test file listing works for all 4 venues."""

        def mock_response(*args, **kwargs):
            response = Mock()
            response.status_code = 200
            # Extract venue from URL
            url = args[0]
            if "DETR-" in url:
                prefix = (
                    "DETR-posttrade-2025-10-31"
                    if "posttrade" in url
                    else "DETR-pretrade-2025-10-31"
                )
            elif "DFRA-" in url:
                prefix = (
                    "DFRA-posttrade-2025-10-31"
                    if "posttrade" in url
                    else "DFRA-pretrade-2025-10-31"
                )
            elif "DGAT-" in url:
                prefix = (
                    "DGAT-posttrade-2025-10-31"
                    if "posttrade" in url
                    else "DGAT-pretrade-2025-10-31"
                )
            else:
                prefix = (
                    "DEUR-posttrade-2025-10-31"
                    if "posttrade" in url
                    else "DEUR-pretrade-2025-10-31"
                )

            response.json.return_value = {
                "SourcePrefix": prefix,
                "CurrentFiles": [f"{prefix}-2025-10-31T13_54.json.gz"],
            }
            return response

        mock_get.side_effect = mock_response

        venues = ["DETR", "DFRA", "DGAT", "DEUR"]

        with XetraFetcher() as fetcher:
            for venue in venues:
                files = fetcher.list_available_files(venue)
                assert len(files) > 0
                assert files[0].startswith(venue)

    @patch("httpx.Client.get")
    def test_list_available_files_404_no_data(self, mock_get):
        """Test graceful handling of 404 (no data available)."""
        response = Mock()
        response.status_code = 404
        mock_get.return_value = response

        with XetraFetcher() as fetcher:
            files = fetcher.list_available_files("DETR")

        assert files == []  # Should return empty list, not raise

    @patch("httpx.Client.get")
    def test_list_available_files_500_server_error(self, mock_get):
        """Test handling of server errors."""
        response = Mock()
        response.status_code = 500
        response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Server error", request=Mock(), response=response
        )
        mock_get.return_value = response

        with XetraFetcher() as fetcher:
            files = fetcher.list_available_files("DETR")

        # Should log error but return partial results
        assert isinstance(files, list)

    @patch("httpx.Client.get")
    def test_list_available_files_network_error(self, mock_get):
        """Test handling of network failures."""
        mock_get.side_effect = httpx.RequestError("Connection timeout")

        with XetraFetcher() as fetcher:
            files = fetcher.list_available_files("DETR")

        assert files == []  # Should return empty on network error

    @patch("httpx.Client.get")
    def test_list_available_files_invalid_json(self, mock_get):
        """Test handling of malformed JSON response."""
        response = Mock()
        response.status_code = 200
        response.json.side_effect = ValueError("Invalid JSON")
        mock_get.return_value = response

        with XetraFetcher() as fetcher:
            files = fetcher.list_available_files("DETR")

        assert files == []  # Should handle gracefully

    @patch("httpx.Client.get")
    def test_download_file_success(self, mock_get):
        """Test successful file download."""
        test_data = b"compressed binary data here"
        response = Mock()
        response.status_code = 200
        response.content = test_data
        mock_get.return_value = response

        with XetraFetcher() as fetcher:
            data = fetcher.download_file(
                "DETR", "2025-10-31", "DETR-posttrade-2025-10-31T13_54.json.gz"
            )

        assert data == test_data
        mock_get.assert_called_once()
        # Verify correct URL construction
        called_url = mock_get.call_args[0][0]
        assert "DETR-posttrade-2025-10-31T13_54.json.gz" in called_url

    @patch("httpx.Client.get")
    def test_download_file_404_not_found(self, mock_get):
        """Test handling of 404 on file download."""
        response = Mock()
        response.status_code = 404
        response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Not found", request=Mock(), response=response
        )
        mock_get.return_value = response

        with XetraFetcher() as fetcher:
            with pytest.raises(httpx.HTTPStatusError):
                fetcher.download_file(
                    "DETR", "2025-10-31", "DETR-posttrade-2025-10-31T13_54.json.gz"
                )

    @patch("httpx.Client.get")
    def test_download_file_network_error(self, mock_get):
        """Test handling of network errors on download."""
        mock_get.side_effect = httpx.RequestError("Connection reset")

        with XetraFetcher() as fetcher:
            with pytest.raises(httpx.RequestError):
                fetcher.download_file(
                    "DETR", "2025-10-31", "DETR-posttrade-2025-10-31T13_54.json.gz"
                )

    def test_decompress_gzip_valid_data(self):
        """Test successful gzip decompression."""
        original_json = '{"key": "value", "trades": [1, 2, 3]}'
        compressed = gzip.compress(original_json.encode("utf-8"))

        with XetraFetcher() as fetcher:
            decompressed = fetcher.decompress_gzip(compressed)

        assert decompressed == original_json

    def test_decompress_gzip_large_file(self):
        """Test decompression of realistic trade data size."""
        # Simulate a large trade file (~100KB uncompressed)
        large_json = json.dumps({"trades": [{"id": i} for i in range(1000)]})
        compressed = gzip.compress(large_json.encode("utf-8"))

        with XetraFetcher() as fetcher:
            decompressed = fetcher.decompress_gzip(compressed)

        assert len(decompressed) == len(large_json)
        assert json.loads(decompressed)["trades"][0]["id"] == 0

    def test_decompress_gzip_invalid_data(self):
        """Test handling of non-gzip data."""
        invalid_data = b"not gzipped data"

        with XetraFetcher() as fetcher:
            with pytest.raises(gzip.BadGzipFile):
                fetcher.decompress_gzip(invalid_data)

    def test_decompress_gzip_non_utf8(self):
        """Test handling of non-UTF-8 data after decompression."""
        # Compress invalid UTF-8 bytes
        invalid_utf8 = b"\xff\xfe\xfd"
        compressed = gzip.compress(invalid_utf8)

        with XetraFetcher() as fetcher:
            with pytest.raises(UnicodeDecodeError):
                fetcher.decompress_gzip(compressed)

    @patch("httpx.Client.get")
    def test_full_workflow_integration(self, mock_get):
        """Test complete fetch → download → decompress workflow."""
        # Mock list response
        list_response = Mock()
        list_response.status_code = 200
        list_response.json.return_value = {
            "CurrentFiles": ["-2025-10-31T13_54.json.gz"]
        }

        # Mock download response
        test_json = '{"trade": "data"}'
        compressed_data = gzip.compress(test_json.encode("utf-8"))
        download_response = Mock()
        download_response.status_code = 200
        download_response.content = compressed_data

        mock_get.side_effect = [list_response, download_response]

        with XetraFetcher() as fetcher:
            # List files (only posttrade now)
            files = fetcher.list_available_files("DETR")
            assert len(files) == 1

            # Download first file
            filename = files[0]
            compressed_data = fetcher.download_file("DETR", "2025-10-31", filename)

            # Decompress
            json_str = fetcher.decompress_gzip(compressed_data)

        assert json_str == test_json

    @patch("httpx.Client.get")
    def test_http_headers_include_json_accept(self, mock_get):
        """Test that API requests include Accept: application/json header."""
        response = Mock()
        response.status_code = 200
        response.json.return_value = {"CurrentFiles": []}
        mock_get.return_value = response

        with XetraFetcher() as fetcher:
            fetcher.list_available_files("DETR")

        # Check headers in the call
        call_kwargs = mock_get.call_args[1]
        assert "headers" in call_kwargs
        assert call_kwargs["headers"]["Accept"] == "application/json"

    @patch("httpx.Client.get")
    def test_follows_redirects(self, mock_get):
        """Test that fetcher follows HTTP redirects."""
        response = Mock()
        response.status_code = 200
        response.json.return_value = {"CurrentFiles": []}
        mock_get.return_value = response

        with XetraFetcher() as fetcher:
            fetcher.list_available_files("DETR")

        # Check follow_redirects in the call
        call_kwargs = mock_get.call_args[1]
        assert call_kwargs.get("follow_redirects") is True

    def test_timezone_conversion_winter_cet(self):
        """Trading-hours filtering is disabled; always returns True."""
        fetcher = XetraFetcher(filter_empty_files=True)

        assert (
            fetcher.is_within_trading_hours(
                "DETR-posttrade-2025-11-03T01_00.json.gz", "DETR"
            )
            is True
        )
        assert (
            fetcher.is_within_trading_hours(
                "DETR-posttrade-2025-11-03T23_59.json.gz", "DETR"
            )
            is True
        )
        fetcher.close()

    def test_timezone_conversion_summer_cest(self):
        """Trading-hours filtering is disabled; always returns True."""
        fetcher = XetraFetcher(filter_empty_files=True)

        assert (
            fetcher.is_within_trading_hours(
                "DETR-posttrade-2025-07-15T00_00.json.gz", "DETR"
            )
            is True
        )
        assert (
            fetcher.is_within_trading_hours(
                "DETR-posttrade-2025-07-15T23_59.json.gz", "DETR"
            )
            is True
        )
        fetcher.close()

    def test_trading_hours_boundary_conditions(self):
        """Test exact boundary conditions for trading hours."""
        fetcher = XetraFetcher(filter_empty_files=True)

        # Winter (CET = UTC+1): boundary is 08:30 CET = 07:30 UTC
        # Just inside lower boundary
        assert (
            fetcher.is_within_trading_hours(
                "DETR-posttrade-2025-11-03T07_30.json.gz", "DETR"
            )
            is True
        )

        # Boundaries are ignored when filtering disabled
        assert (
            fetcher.is_within_trading_hours(
                "DETR-posttrade-2025-11-03T06_29.json.gz", "DETR"
            )
            is True
        )
        assert (
            fetcher.is_within_trading_hours(
                "DETR-posttrade-2025-11-03T17_31.json.gz", "DETR"
            )
            is True
        )

        fetcher.close()

    def test_filter_disabled_returns_true(self):
        """Test that filtering disabled allows all files through."""
        fetcher = XetraFetcher(filter_empty_files=False)

        # All times should return True when filtering is disabled
        assert (
            fetcher.is_within_trading_hours(
                "DETR-posttrade-2025-11-03T01_00.json.gz", "DETR"
            )
            is True
        )

        assert (
            fetcher.is_within_trading_hours(
                "DETR-posttrade-2025-11-03T23_00.json.gz", "DETR"
            )
            is True
        )

        fetcher.close()

    def test_unknown_venue_returns_true(self):
        """Test that unknown venues return True (no filtering)."""
        fetcher = XetraFetcher(filter_empty_files=True)

        # Unknown venue should not be filtered
        assert (
            fetcher.is_within_trading_hours(
                "UNKNOWN-posttrade-2025-11-03T01_00.json.gz", "UNKNOWN"
            )
            is True
        )

        fetcher.close()

    def test_malformed_filename_returns_true(self):
        """Test that malformed filenames return True (graceful fallback)."""
        fetcher = XetraFetcher(filter_empty_files=True)

        # No 'T' in filename
        assert (
            fetcher.is_within_trading_hours("DETR-posttrade-2025-11-03.json.gz", "DETR")
            is True
        )

        # Invalid time format
        assert (
            fetcher.is_within_trading_hours(
                "DETR-posttrade-2025-11-03Tinvalid.json.gz", "DETR"
            )
            is True
        )

        fetcher.close()

    def test_dst_transition_dates(self):
        """Test timezone conversion around DST transition dates."""
        fetcher = XetraFetcher(filter_empty_files=True)

        # March 30, 2025: DST transition (CET to CEST at 02:00 -> 03:00)
        # Before transition (CET): UTC 08:00 = CET 09:00
        assert (
            fetcher.is_within_trading_hours(
                "DETR-posttrade-2025-03-30T08_00.json.gz", "DETR"
            )
            is True
        )

        # After transition (CEST): UTC 07:00 = CEST 09:00
        assert (
            fetcher.is_within_trading_hours(
                "DETR-posttrade-2025-03-31T07_00.json.gz", "DETR"
            )
            is True
        )

        # October 26, 2025: DST transition (CEST to CET at 03:00 -> 02:00)
        # Before transition (CEST): UTC 07:00 = CEST 09:00
        assert (
            fetcher.is_within_trading_hours(
                "DETR-posttrade-2025-10-25T07_00.json.gz", "DETR"
            )
            is True
        )

        # After transition (CET): UTC 08:00 = CET 09:00
        assert (
            fetcher.is_within_trading_hours(
                "DETR-posttrade-2025-10-27T08_00.json.gz", "DETR"
            )
            is True
        )

        fetcher.close()

    def test_all_venues_use_same_hours(self):
        """Test that all configured venues use the same trading hours."""
        fetcher = XetraFetcher(filter_empty_files=True)

        # Winter: UTC 08:00 = CET 09:00 (within window for all venues)
        for venue in ["DETR", "DFRA", "DGAT", "DEUR"]:
            assert (
                fetcher.is_within_trading_hours(
                    f"{venue}-posttrade-2025-11-03T08_00.json.gz", venue
                )
                is True
            )

            # Early morning: UTC 01:00 = CET 02:00 (previously outside window)
            # Now permitted because trading-hour filtering is disabled for downloads.
            assert (
                fetcher.is_within_trading_hours(
                    f"{venue}-posttrade-2025-11-03T01_00.json.gz", venue
                )
                is True
            )

        fetcher.close()

    def test_malformed_filename_no_T_separator(self):
        """Test that malformed filenames without T separator return True (no filtering)."""
        fetcher = XetraFetcher(filter_empty_files=True)

        # Filename without 'T' should return True (skip filtering)
        assert (
            fetcher.is_within_trading_hours("DETR-posttrade-malformed.json.gz", "DETR")
            is True
        )

        fetcher.close()

    def test_malformed_filename_wrong_parts(self):
        """Test that filenames with wrong number of parts return True (no filtering)."""
        fetcher = XetraFetcher(filter_empty_files=True)

        # Filename that splits incorrectly should return True (skip filtering)
        # This could happen if rsplit returns unexpected number of parts
        assert fetcher.is_within_trading_hours("DETR-posttrade.json.gz", "DETR") is True

        fetcher.close()

    def test_unknown_venue_logs_warning(self):
        """Test that unknown venue code logs warning and returns True."""
        fetcher = XetraFetcher(filter_empty_files=True)

        # Unknown venue should trigger warning log and return True
        with patch("yf_parqed.xetra.xetra_fetcher.logger") as mock_logger:
            result = fetcher.is_within_trading_hours(
                "XXXX-posttrade-2025-11-03T10_00.json.gz", "XXXX"
            )
            assert result is True
            # Filtering is disabled, so we do not warn on unknown venue codes.
            mock_logger.warning.assert_not_called()

        fetcher.close()

    @patch("httpx.Client.get")
    @patch("time.sleep")
    def test_burst_cooldown_triggers_after_burst_size(self, mock_sleep, mock_get):
        """Test that burst cooldown is triggered after burst_size requests."""
        # Configure fetcher with small burst size for testing
        fetcher = XetraFetcher(
            inter_request_delay=0.01,  # Very short delay for test speed
            burst_size=3,  # Small burst for easy testing
            burst_cooldown=0.1,  # Short cooldown for test speed
        )

        # Mock successful downloads
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = gzip.compress(b'{"test": "data"}')
        mock_get.return_value = mock_response

        # Download 4 files (should trigger cooldown after 3rd file)
        for i in range(4):
            fetcher.download_file(
                "DETR", "2025-11-03", f"DETR-posttrade-test-{i}.json.gz"
            )

        # Verify cooldown was called (after 3rd request, before 4th)
        # Should have: inter-request delays + one burst cooldown
        cooldown_calls = [
            call for call in mock_sleep.call_args_list if call[0][0] >= 0.09
        ]
        assert len(cooldown_calls) >= 1, "Burst cooldown should have been triggered"

        fetcher.close()

    @patch("httpx.Client.get")
    @patch("time.sleep")
    def test_inter_request_delay_enforced(self, mock_sleep, mock_get):
        """Test that inter-request delay is enforced between consecutive requests."""
        fetcher = XetraFetcher(
            inter_request_delay=0.5,
            burst_size=100,  # High burst size to avoid cooldown in this test
            burst_cooldown=1,
        )

        # Mock successful downloads
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = gzip.compress(b'{"test": "data"}')
        mock_get.return_value = mock_response

        # Download 2 files
        fetcher.download_file("DETR", "2025-11-03", "file1.json.gz")
        fetcher.download_file("DETR", "2025-11-03", "file2.json.gz")

        # Verify inter-request delay was called
        # Should have at least one delay between requests
        delay_calls = [
            call for call in mock_sleep.call_args_list if 0.4 <= call[0][0] <= 0.6
        ]
        assert len(delay_calls) >= 1, "Inter-request delay should have been enforced"

        fetcher.close()


def test_network_error_during_download_raises_request_error():
    """Test that network errors during download are properly raised."""
    fetcher = XetraFetcher(inter_request_delay=0.0)

    with patch("httpx.Client.get") as mock_get:
        # Simulate network error
        mock_get.side_effect = httpx.RequestError("Connection timeout")

        with pytest.raises(httpx.RequestError, match="Connection timeout"):
            fetcher.download_file(
                "DETR", "2025-11-03", "DETR-posttrade-2025-11-03T09_00.json.gz"
            )

    fetcher.close()


def test_retry_logic_on_429_with_exponential_backoff():
    """Test that 429 rate limit errors trigger exponential backoff retries."""
    fetcher = XetraFetcher(inter_request_delay=0.0)

    with patch("httpx.Client.get") as mock_get, patch("time.sleep") as mock_sleep:
        # Mock response for 429 errors followed by success
        mock_429_response = Mock()
        mock_429_response.status_code = 429

        mock_success_response = Mock()
        mock_success_response.status_code = 200
        mock_success_response.content = gzip.compress(b'{"test": "data"}')

        # First two attempts return 429, third succeeds
        mock_get.side_effect = [
            httpx.HTTPStatusError(
                "Rate limited", request=Mock(), response=mock_429_response
            ),
            httpx.HTTPStatusError(
                "Rate limited", request=Mock(), response=mock_429_response
            ),
            mock_success_response,
        ]

        # Should succeed after retries
        result = fetcher.download_file(
            "DETR", "2025-11-03", "DETR-posttrade-2025-11-03T09_00.json.gz"
        )
        assert result == gzip.compress(b'{"test": "data"}')

        # Verify exponential backoff delays: 2s, 4s
        sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
        # Filter out inter-request delays (< 1s), keep only backoff delays
        backoff_delays = [d for d in sleep_calls if d >= 2.0]
        assert len(backoff_delays) == 2, "Should have two backoff delays"
        assert backoff_delays[0] == 2, "First backoff should be 2s"
        assert backoff_delays[1] == 4, "Second backoff should be 4s (exponential)"

    fetcher.close()


def test_retry_exhaustion_on_persistent_429_errors():
    """Test that persistent 429 errors eventually raise after max retries."""
    fetcher = XetraFetcher(inter_request_delay=0.0)

    with patch("httpx.Client.get") as mock_get, patch("time.sleep"):
        # Mock persistent 429 errors
        mock_429_response = Mock()
        mock_429_response.status_code = 429

        mock_get.side_effect = httpx.HTTPStatusError(
            "Rate limited", request=Mock(), response=mock_429_response
        )

        # Should raise after exhausting retries
        with pytest.raises(httpx.HTTPStatusError, match="Rate limited"):
            fetcher.download_file(
                "DETR", "2025-11-03", "DETR-posttrade-2025-11-03T09_00.json.gz"
            )

    fetcher.close()


def test_network_error_during_list_files_logs_and_continues():
    """Test that network errors during file listing are caught and logged."""
    fetcher = XetraFetcher(inter_request_delay=0.0)

    with patch("httpx.Client.get") as mock_get:
        # Simulate network error
        mock_get.side_effect = httpx.RequestError("Connection timeout")

        # Should return empty list and log error (not raise)
        files = fetcher.list_available_files("DETR")
        assert files == []

    fetcher.close()


def test_http_error_during_list_files_logs_and_continues():
    """Test that non-404 HTTP errors during file listing are caught and logged."""
    fetcher = XetraFetcher(inter_request_delay=0.0)

    with patch("httpx.Client.get") as mock_get:
        # Simulate 500 server error
        mock_500_response = Mock()
        mock_500_response.status_code = 500

        mock_get.side_effect = httpx.HTTPStatusError(
            "Server error", request=Mock(), response=mock_500_response
        )

        # Should return empty list and log error (not raise)
        files = fetcher.list_available_files("DETR")
        assert files == []

    fetcher.close()
