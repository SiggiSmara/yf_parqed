"""Tests for daemon mode trading hours functionality."""

import pytest
from datetime import time as dt_time, datetime, timedelta
from zoneinfo import ZoneInfo
from unittest.mock import patch, MagicMock

from yf_parqed.xetra_cli import (
    _parse_active_hours,
    _is_within_active_hours,
    _seconds_until_active,
)


class TestParseActiveHours:
    """Test active hours string parsing."""

    def test_parse_standard_hours(self):
        """Test parsing standard trading hours."""
        start, end = _parse_active_hours("08:30-18:00")
        assert start == dt_time(8, 30)
        assert end == dt_time(18, 0)

    def test_parse_24_hour_format(self):
        """Test parsing 24-hour format."""
        start, end = _parse_active_hours("00:00-23:59")
        assert start == dt_time(0, 0)
        assert end == dt_time(23, 59)

    def test_parse_extended_hours(self):
        """Test parsing extended hours."""
        start, end = _parse_active_hours("07:00-19:00")
        assert start == dt_time(7, 0)
        assert end == dt_time(19, 0)

    def test_parse_midnight_crossing(self):
        """Test parsing hours that cross midnight."""
        start, end = _parse_active_hours("22:00-02:00")
        assert start == dt_time(22, 0)
        assert end == dt_time(2, 0)

    def test_parse_single_digit_hours(self):
        """Test parsing with single digit hours (actually works - flexible parsing)."""
        start, end = _parse_active_hours("8:30-18:00")
        assert start == dt_time(8, 30)
        assert end == dt_time(18, 0)

    def test_parse_invalid_format_no_dash(self):
        """Test parsing invalid format without dash."""
        with pytest.raises(ValueError, match="Invalid active-hours format"):
            _parse_active_hours("08:30 18:00")

    def test_parse_invalid_format_no_colon(self):
        """Test parsing invalid format without colon."""
        with pytest.raises(ValueError, match="Invalid active-hours format"):
            _parse_active_hours("0830-1800")

    def test_parse_invalid_format_too_many_parts(self):
        """Test parsing invalid format with too many parts."""
        with pytest.raises(ValueError, match="Invalid active-hours format"):
            _parse_active_hours("08:30-12:00-18:00")

    def test_parse_invalid_hour_value(self):
        """Test parsing with invalid hour value."""
        with pytest.raises(ValueError):
            _parse_active_hours("25:00-18:00")

    def test_parse_invalid_minute_value(self):
        """Test parsing with invalid minute value."""
        with pytest.raises(ValueError):
            _parse_active_hours("08:60-18:00")


class TestIsWithinActiveHours:
    """Test active hours checking."""

    def test_within_hours_morning(self):
        """Test time within active hours (morning)."""
        start = dt_time(8, 30)
        end = dt_time(18, 0)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # Mock current time to 10:00 Berlin time
        mock_now = datetime(2025, 12, 1, 10, 0, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            result = _is_within_active_hours(start, end, berlin_tz)
        
        assert result is True

    def test_within_hours_afternoon(self):
        """Test time within active hours (afternoon)."""
        start = dt_time(8, 30)
        end = dt_time(18, 0)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # Mock current time to 15:00 Berlin time
        mock_now = datetime(2025, 12, 1, 15, 0, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            result = _is_within_active_hours(start, end, berlin_tz)
        
        assert result is True

    def test_exactly_at_start_time(self):
        """Test time exactly at start of active hours."""
        start = dt_time(8, 30)
        end = dt_time(18, 0)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        mock_now = datetime(2025, 12, 1, 8, 30, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            result = _is_within_active_hours(start, end, berlin_tz)
        
        assert result is True

    def test_exactly_at_end_time(self):
        """Test time exactly at end of active hours."""
        start = dt_time(8, 30)
        end = dt_time(18, 0)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        mock_now = datetime(2025, 12, 1, 18, 0, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            result = _is_within_active_hours(start, end, berlin_tz)
        
        assert result is True

    def test_outside_hours_early_morning(self):
        """Test time outside active hours (early morning)."""
        start = dt_time(8, 30)
        end = dt_time(18, 0)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # Mock current time to 07:00 Berlin time
        mock_now = datetime(2025, 12, 1, 7, 0, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            result = _is_within_active_hours(start, end, berlin_tz)
        
        assert result is False

    def test_outside_hours_late_evening(self):
        """Test time outside active hours (late evening)."""
        start = dt_time(8, 30)
        end = dt_time(18, 0)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # Mock current time to 20:00 Berlin time
        mock_now = datetime(2025, 12, 1, 20, 0, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            result = _is_within_active_hours(start, end, berlin_tz)
        
        assert result is False

    def test_midnight_crossing_within(self):
        """Test midnight-crossing hours when currently within."""
        start = dt_time(22, 0)
        end = dt_time(2, 0)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # Mock current time to 23:00 (within 22:00-02:00)
        mock_now = datetime(2025, 12, 1, 23, 0, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            result = _is_within_active_hours(start, end, berlin_tz)
        
        assert result is True

    def test_midnight_crossing_within_after_midnight(self):
        """Test midnight-crossing hours when currently within (after midnight)."""
        start = dt_time(22, 0)
        end = dt_time(2, 0)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # Mock current time to 01:00 (within 22:00-02:00)
        mock_now = datetime(2025, 12, 1, 1, 0, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            result = _is_within_active_hours(start, end, berlin_tz)
        
        assert result is True

    def test_midnight_crossing_outside(self):
        """Test midnight-crossing hours when currently outside."""
        start = dt_time(22, 0)
        end = dt_time(2, 0)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # Mock current time to 10:00 (outside 22:00-02:00)
        mock_now = datetime(2025, 12, 1, 10, 0, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            result = _is_within_active_hours(start, end, berlin_tz)
        
        assert result is False

    def test_24_hour_operation(self):
        """Test 24-hour operation (always within)."""
        start = dt_time(0, 0)
        end = dt_time(23, 59)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # Test various times throughout the day
        test_hours = [0, 6, 12, 18, 23]
        for hour in test_hours:
            mock_now = datetime(2025, 12, 1, hour, 0, 0, tzinfo=berlin_tz)
            with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
                mock_datetime.now.return_value = mock_now
                result = _is_within_active_hours(start, end, berlin_tz)
                assert result is True, f"Failed for hour {hour}"


class TestSecondsUntilActive:
    """Test calculation of seconds until active period."""

    def test_seconds_until_active_same_day(self):
        """Test seconds until active when start is later today."""
        start = dt_time(8, 30)
        end = dt_time(18, 0)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # Mock current time to 07:00, should wait 1.5 hours (5400 seconds)
        mock_now = datetime(2025, 12, 1, 7, 0, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            seconds = _seconds_until_active(start, end, berlin_tz)
        
        assert seconds == 5400  # 1.5 hours = 90 minutes = 5400 seconds

    def test_seconds_until_active_next_day(self):
        """Test seconds until active when start is tomorrow."""
        start = dt_time(8, 30)
        end = dt_time(18, 0)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # Mock current time to 20:00, should wait until 08:30 next day
        mock_now = datetime(2025, 12, 1, 20, 0, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            seconds = _seconds_until_active(start, end, berlin_tz)
        
        # 20:00 to 08:30 next day = 12.5 hours = 45000 seconds
        assert seconds == 45000

    def test_seconds_until_active_currently_active(self):
        """Test seconds until active when currently within active hours."""
        start = dt_time(8, 30)
        end = dt_time(18, 0)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # Mock current time to 10:00 (within active hours)
        mock_now = datetime(2025, 12, 1, 10, 0, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            seconds = _seconds_until_active(start, end, berlin_tz)
        
        assert seconds == 0.0

    def test_seconds_until_active_one_minute_before(self):
        """Test seconds until active when one minute before start."""
        start = dt_time(8, 30)
        end = dt_time(18, 0)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # Mock current time to 08:29
        mock_now = datetime(2025, 12, 1, 8, 29, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            seconds = _seconds_until_active(start, end, berlin_tz)
        
        assert seconds == 60  # 1 minute

    def test_seconds_until_active_just_after_end(self):
        """Test seconds until active just after end time."""
        start = dt_time(8, 30)
        end = dt_time(18, 0)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # Mock current time to 18:01 (just after end)
        mock_now = datetime(2025, 12, 1, 18, 1, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            seconds = _seconds_until_active(start, end, berlin_tz)
        
        # Should wait until 08:30 next day
        # 18:01 to 08:30 next day = 14h 29m = 52140 seconds
        assert seconds == 52140


class TestCLIIntegration:
    """Integration tests for CLI with trading hours."""

    def test_invalid_active_hours_format(self):
        """Test that invalid active hours format raises error."""
        # Test the parsing function directly
        with pytest.raises(ValueError, match="Invalid active-hours format"):
            _parse_active_hours("invalid")
    
    def test_valid_active_hours_formats(self):
        """Test various valid active hours formats."""
        valid_formats = [
            ("08:30-18:00", (dt_time(8, 30), dt_time(18, 0))),
            ("00:00-23:59", (dt_time(0, 0), dt_time(23, 59))),
            ("7:00-19:00", (dt_time(7, 0), dt_time(19, 0))),
            ("22:00-02:00", (dt_time(22, 0), dt_time(2, 0))),
        ]
        
        for format_str, expected in valid_formats:
            start, end = _parse_active_hours(format_str)
            assert (start, end) == expected, f"Failed for format: {format_str}"


class TestTimezoneBehavior:
    """Test timezone-specific behavior."""

    def test_handles_cet_winter_time(self):
        """Test behavior during CET (winter time)."""
        start = dt_time(8, 30)
        end = dt_time(18, 0)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # January is CET (UTC+1)
        mock_now = datetime(2025, 1, 15, 10, 0, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            result = _is_within_active_hours(start, end, berlin_tz)
        
        assert result is True

    def test_handles_cest_summer_time(self):
        """Test behavior during CEST (summer time)."""
        start = dt_time(8, 30)
        end = dt_time(18, 0)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # July is CEST (UTC+2)
        mock_now = datetime(2025, 7, 15, 10, 0, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            result = _is_within_active_hours(start, end, berlin_tz)
        
        assert result is True

    def test_dst_transition_spring_forward(self):
        """Test behavior during spring DST transition (clocks forward)."""
        start = dt_time(2, 30)
        end = dt_time(4, 0)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # DST transition happens at 02:00 -> 03:00 on last Sunday of March
        # 2025-03-30 is the transition date
        # Time 02:30 doesn't exist (skipped), but our logic should handle it
        mock_now = datetime(2025, 3, 30, 3, 30, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            result = _is_within_active_hours(start, end, berlin_tz)
        
        # Should be within hours (03:30 is between 02:30 and 04:00 conceptually)
        assert result is True

    def test_dst_transition_fall_back(self):
        """Test behavior during fall DST transition (clocks back)."""
        start = dt_time(2, 30)
        end = dt_time(4, 0)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # DST transition happens at 03:00 -> 02:00 on last Sunday of October
        # 2025-10-26 is the transition date
        # Time 02:30 happens twice
        mock_now = datetime(2025, 10, 26, 2, 30, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            result = _is_within_active_hours(start, end, berlin_tz)
        
        assert result is True


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_one_minute_window(self):
        """Test with a very short active window."""
        start = dt_time(12, 0)
        end = dt_time(12, 1)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # Within the window
        mock_now = datetime(2025, 12, 1, 12, 0, 30, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            result = _is_within_active_hours(start, end, berlin_tz)
        assert result is True
        
        # Just outside
        mock_now = datetime(2025, 12, 1, 12, 2, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            result = _is_within_active_hours(start, end, berlin_tz)
        assert result is False

    def test_full_day_except_one_minute(self):
        """Test with almost full-day active hours."""
        start = dt_time(0, 1)
        end = dt_time(23, 59)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # At 00:00 (outside)
        mock_now = datetime(2025, 12, 1, 0, 0, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            result = _is_within_active_hours(start, end, berlin_tz)
        assert result is False
        
        # At 00:01 (inside)
        mock_now = datetime(2025, 12, 1, 0, 1, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            result = _is_within_active_hours(start, end, berlin_tz)
        assert result is True

    def test_same_start_and_end_time(self):
        """Test with same start and end time (edge case)."""
        start = dt_time(12, 0)
        end = dt_time(12, 0)
        berlin_tz = ZoneInfo("Europe/Berlin")
        
        # Exactly at the time
        mock_now = datetime(2025, 12, 1, 12, 0, 0, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            result = _is_within_active_hours(start, end, berlin_tz)
        assert result is True  # Exactly on the boundary
        
        # One second after
        mock_now = datetime(2025, 12, 1, 12, 0, 1, tzinfo=berlin_tz)
        with patch("yf_parqed.xetra_cli.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            result = _is_within_active_hours(start, end, berlin_tz)
        assert result is False
