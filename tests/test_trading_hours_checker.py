"""Tests for TradingHoursChecker service."""

import pytest
from datetime import time as dt_time, datetime
from freezegun import freeze_time

from yf_parqed.xetra.trading_hours_checker import TradingHoursChecker


class TestTradingHoursCheckerBasic:
    """Basic trading hours checking without timezone complications."""

    @freeze_time("2025-12-04 10:00:00", tz_offset=0)  # 10:00 UTC
    def test_within_hours_utc(self):
        """Test within hours when system is UTC."""
        # 09:00-17:00 UTC
        checker = TradingHoursChecker(
            start_time=dt_time(9, 0),
            end_time=dt_time(17, 0),
            market_timezone="UTC",
            system_timezone="UTC",
        )
        assert checker.is_within_hours() is True

    @freeze_time("2025-12-04 08:00:00", tz_offset=0)  # 08:00 UTC
    def test_before_hours_utc(self):
        """Test before trading hours."""
        checker = TradingHoursChecker(
            start_time=dt_time(9, 0),
            end_time=dt_time(17, 0),
            market_timezone="UTC",
            system_timezone="UTC",
        )
        assert checker.is_within_hours() is False

    @freeze_time("2025-12-04 18:00:00", tz_offset=0)  # 18:00 UTC
    def test_after_hours_utc(self):
        """Test after trading hours."""
        checker = TradingHoursChecker(
            start_time=dt_time(9, 0),
            end_time=dt_time(17, 0),
            market_timezone="UTC",
            system_timezone="UTC",
        )
        assert checker.is_within_hours() is False


class TestNYSEHours:
    """Test NYSE regular hours (09:30-16:00 US/Eastern) from various timezones."""

    @freeze_time("2025-12-04 08:00:00-05:00")  # 08:00 EST (before market open at 09:30)
    def test_nyse_before_open_eastern(self):
        """NYSE before open from US/Eastern."""
        checker = TradingHoursChecker(
            start_time=dt_time(9, 30),
            end_time=dt_time(16, 0),
            market_timezone="US/Eastern",
            system_timezone="US/Eastern",
        )
        assert checker.is_within_hours() is False

        # Should open in 90 minutes
        seconds = checker.seconds_until_active()
        assert 5390 < seconds < 5410  # ~5400 seconds = 90 minutes

    @freeze_time("2025-12-04 19:00:00")  # 19:00 UTC = 14:00 EST (market open)
    def test_nyse_during_hours_eastern(self):
        """NYSE during trading hours from US/Eastern."""
        checker = TradingHoursChecker(
            start_time=dt_time(9, 30),
            end_time=dt_time(16, 0),
            market_timezone="US/Eastern",
            system_timezone="US/Eastern",
        )
        assert checker.is_within_hours() is True
        assert checker.seconds_until_active() == 0.0

    @freeze_time("2025-12-04 22:00:00")  # 22:00 UTC = 17:00 EST (after close)
    def test_nyse_after_close_eastern(self):
        """NYSE after close from US/Eastern."""
        checker = TradingHoursChecker(
            start_time=dt_time(9, 30),
            end_time=dt_time(16, 0),
            market_timezone="US/Eastern",
            system_timezone="US/Eastern",
        )
        assert checker.is_within_hours() is False

        # Should open next day at 09:30 (16.5 hours from 17:00)
        seconds = checker.seconds_until_active()
        expected = 16.5 * 3600
        assert expected - 10 < seconds < expected + 10

    @freeze_time("2025-12-04 19:00:00")  # 19:00 UTC = 20:00 CET = 14:00 EST
    def test_nyse_from_europe(self):
        """NYSE trading hours viewed from Europe/Berlin."""
        checker = TradingHoursChecker(
            start_time=dt_time(9, 30),
            end_time=dt_time(16, 0),
            market_timezone="US/Eastern",
            system_timezone="Europe/Berlin",
        )
        # 14:00 EST is during trading hours
        assert checker.is_within_hours() is True

        # Local hours should show conversion
        local_hours = checker._calculate_local_hours()
        assert "15:30" in local_hours  # 09:30 EST = 15:30 CET
        assert "22:00" in local_hours  # 16:00 EST = 22:00 CET

    @freeze_time("2025-12-04 18:00:00")  # 18:00 UTC = 10:00 PST = 13:00 EST
    def test_nyse_from_california(self):
        """NYSE trading hours viewed from US/Pacific."""
        checker = TradingHoursChecker(
            start_time=dt_time(9, 30),
            end_time=dt_time(16, 0),
            market_timezone="US/Eastern",
            system_timezone="US/Pacific",
        )
        # 13:00 EST is during trading hours
        assert checker.is_within_hours() is True

        # Local hours should show conversion
        local_hours = checker._calculate_local_hours()
        assert "06:30" in local_hours  # 09:30 EST = 06:30 PST
        assert "13:00" in local_hours  # 16:00 EST = 13:00 PST


class TestXetraHours:
    """Test Xetra hours (08:30-18:00 Europe/Berlin) from various timezones."""

    @freeze_time("2025-12-04 09:00:00")  # 09:00 UTC = 10:00 CET (during hours)
    def test_xetra_during_hours_berlin(self):
        """Xetra during trading hours from Europe/Berlin."""
        checker = TradingHoursChecker(
            start_time=dt_time(8, 30),
            end_time=dt_time(18, 0),
            market_timezone="Europe/Berlin",
            system_timezone="Europe/Berlin",
        )
        assert checker.is_within_hours() is True

    @freeze_time("2025-12-04 09:30:00")  # 09:30 UTC = 01:30 PST = 10:30 CET
    def test_xetra_from_california(self):
        """Xetra trading hours viewed from US/Pacific."""
        checker = TradingHoursChecker(
            start_time=dt_time(8, 30),
            end_time=dt_time(18, 0),
            market_timezone="Europe/Berlin",
            system_timezone="US/Pacific",
        )
        # 10:30 CET is during trading hours
        assert checker.is_within_hours() is True

        # Local hours should show day boundary crossing
        local_hours = checker._calculate_local_hours()
        assert "23:30" in local_hours  # 08:30 CET = 23:30 PST (previous day)
        assert "09:00" in local_hours  # 18:00 CET = 09:00 PST (same day)
        assert "Dec" in local_hours  # Shows dates due to day crossing


class TestExtendedHours:
    """Test extended hours (pre-market + after-hours)."""

    @freeze_time("2025-12-04 05:00:00", tz_offset=-5)  # 05:00 EST (pre-market)
    def test_pre_market_hours(self):
        """Test pre-market hours (04:00-09:30 US/Eastern)."""
        checker = TradingHoursChecker(
            start_time=dt_time(4, 0),
            end_time=dt_time(20, 0),  # Extended 04:00-20:00
            market_timezone="US/Eastern",
            system_timezone="US/Eastern",
        )
        assert checker.is_within_hours() is True

    @freeze_time("2025-12-04 17:00:00", tz_offset=-5)  # 17:00 EST (after-hours)
    def test_after_hours(self):
        """Test after-hours trading (16:00-20:00 US/Eastern)."""
        checker = TradingHoursChecker(
            start_time=dt_time(4, 0),
            end_time=dt_time(20, 0),  # Extended 04:00-20:00
            market_timezone="US/Eastern",
            system_timezone="US/Eastern",
        )
        assert checker.is_within_hours() is True


class TestMidnightCrossing:
    """Test handling of hours that cross midnight."""

    def test_overnight_hours(self):
        """Test hours crossing midnight (e.g., 22:00-02:00)."""
        # At 23:00, should be within hours
        with freeze_time("2025-12-04 23:00:00", tz_offset=0):
            checker = TradingHoursChecker(
                start_time=dt_time(22, 0),
                end_time=dt_time(2, 0),
                market_timezone="UTC",
                system_timezone="UTC",
            )
            assert checker.is_within_hours() is True

        # At 01:00, should be within hours
        with freeze_time("2025-12-04 01:00:00", tz_offset=0):
            checker = TradingHoursChecker(
                start_time=dt_time(22, 0),
                end_time=dt_time(2, 0),
                market_timezone="UTC",
                system_timezone="UTC",
            )
            assert checker.is_within_hours() is True

        # At 03:00, should be outside hours
        with freeze_time("2025-12-04 03:00:00", tz_offset=0):
            checker = TradingHoursChecker(
                start_time=dt_time(22, 0),
                end_time=dt_time(2, 0),
                market_timezone="UTC",
                system_timezone="UTC",
            )
            assert checker.is_within_hours() is False


class TestNextActiveTime:
    """Test next_active_time calculation."""

    @freeze_time("2025-12-04 13:00:00")  # 13:00 UTC = 08:00 EST (before open)
    def test_next_active_same_day(self):
        """Next active time is later same day."""
        checker = TradingHoursChecker(
            start_time=dt_time(9, 30),
            end_time=dt_time(16, 0),
            market_timezone="US/Eastern",
            system_timezone="US/Eastern",
        )
        next_active = checker.next_active_time()

        # Should be today at 09:30
        assert next_active.hour == 9
        assert next_active.minute == 30
        assert next_active.date() == datetime(2025, 12, 4).date()

    @freeze_time("2025-12-04 22:00:00")  # 22:00 UTC = 17:00 EST (after close)
    def test_next_active_next_day(self):
        """Next active time is next day."""
        checker = TradingHoursChecker(
            start_time=dt_time(9, 30),
            end_time=dt_time(16, 0),
            market_timezone="US/Eastern",
            system_timezone="US/Eastern",
        )
        next_active = checker.next_active_time()

        # Should be tomorrow at 09:30
        assert next_active.hour == 9
        assert next_active.minute == 30
        assert next_active.date() == datetime(2025, 12, 5).date()


class TestSecondsUntilClose:
    """Test seconds_until_close uses UTC windows."""

    @freeze_time("2025-12-04 16:15:00")  # 16:15 UTC = 17:15 CET
    def test_returns_remaining_seconds(self):
        checker = TradingHoursChecker(
            start_time=dt_time(8, 30),
            end_time=dt_time(18, 0),
            market_timezone="Europe/Berlin",
            system_timezone="UTC",
        )

        remaining = checker.seconds_until_close()
        # Close at 17:00 UTC => ~45 minutes remaining
        assert 2600 < remaining < 2800

    @freeze_time("2025-12-04 18:30:00")  # 18:30 UTC = 19:30 CET (after close)
    def test_zero_after_close(self):
        checker = TradingHoursChecker(
            start_time=dt_time(8, 30),
            end_time=dt_time(18, 0),
            market_timezone="Europe/Berlin",
            system_timezone="UTC",
        )

        assert checker.seconds_until_close() == 0.0


class TestParseActiveHours:
    """Test active hours string parsing."""

    def test_parse_valid_hours(self):
        """Parse valid hours string."""
        start, end = TradingHoursChecker.parse_active_hours("09:30-16:00")
        assert start == dt_time(9, 30)
        assert end == dt_time(16, 0)

    def test_parse_with_spaces(self):
        """Parse hours with extra spaces."""
        start, end = TradingHoursChecker.parse_active_hours("  09:30  -  16:00  ")
        assert start == dt_time(9, 30)
        assert end == dt_time(16, 0)

    def test_parse_midnight(self):
        """Parse midnight hours."""
        start, end = TradingHoursChecker.parse_active_hours("00:00-23:59")
        assert start == dt_time(0, 0)
        assert end == dt_time(23, 59)

    def test_parse_invalid_format(self):
        """Parse invalid format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid active hours format"):
            TradingHoursChecker.parse_active_hours("9:30-16")

        with pytest.raises(ValueError, match="Invalid active hours format"):
            TradingHoursChecker.parse_active_hours("invalid")


class TestAutoTimezoneDetection:
    """Test automatic timezone detection."""

    def test_auto_detect_system_timezone(self):
        """System timezone auto-detected when not provided."""
        checker = TradingHoursChecker(
            start_time=dt_time(9, 30),
            end_time=dt_time(16, 0),
            market_timezone="US/Eastern",
            system_timezone=None,  # Auto-detect
        )

        # Should have detected something
        assert checker.system_tz is not None
        # Can't assert exact timezone as it depends on test environment
        # but can verify it's a timezone info object
        assert hasattr(checker.system_tz, "tzname")
