"""
Trading hours checker with timezone support.

Reusable service for daemon mode that handles:
- Multi-timezone conversions (market timezone → system timezone)
- Auto-detection of system timezone
- Active hours checking with DST transitions
- Next active period calculation
"""

from datetime import datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo
from loguru import logger


class TradingHoursChecker:
    """
    Check if current time is within trading hours with timezone awareness.

    Attributes:
        market_timezone: Timezone where the market operates (e.g., US/Eastern, Europe/Berlin)
        system_timezone: Timezone where the daemon is running (auto-detected or explicit)
        start_time: Market opening time
        end_time: Market closing time
    """

    def __init__(
        self,
        start_time: dt_time,
        end_time: dt_time,
        market_timezone: str = "US/Eastern",
        system_timezone: str | None = None,
    ):
        """
        Initialize trading hours checker.

        Args:
            start_time: Market opening time (in market timezone)
            end_time: Market closing time (in market timezone)
            market_timezone: Market timezone string (default: US/Eastern for NYSE/NASDAQ)
            system_timezone: System timezone string (default: auto-detect)

        Example:
            >>> # NYSE regular hours (09:30-16:00 US/Eastern)
            >>> checker = TradingHoursChecker(
            ...     start_time=dt_time(9, 30),
            ...     end_time=dt_time(16, 0),
            ...     market_timezone="US/Eastern"
            ... )

            >>> # Xetra hours (08:30-18:00 Europe/Berlin)
            >>> checker = TradingHoursChecker(
            ...     start_time=dt_time(8, 30),
            ...     end_time=dt_time(18, 0),
            ...     market_timezone="Europe/Berlin"
            ... )
        """
        self.start_time = start_time
        self.end_time = end_time
        self.market_tz = ZoneInfo(market_timezone)

        # Auto-detect system timezone if not provided
        if system_timezone is None:
            # Get local timezone from system
            self.system_tz = datetime.now().astimezone().tzinfo
            logger.debug(f"Auto-detected system timezone: {self.system_tz}")
        else:
            self.system_tz = ZoneInfo(system_timezone)
            logger.debug(f"Using explicit system timezone: {self.system_tz}")

        # Log converted hours
        local_hours = self._calculate_local_hours()
        logger.info(
            f"Market hours: {start_time.strftime('%H:%M')}-{end_time.strftime('%H:%M')} "
            f"{market_timezone} ({local_hours})"
        )

    def is_within_hours(self) -> bool:
        """
        Check if current time is within trading hours.

        Returns:
            True if currently within trading hours

        Example:
            >>> checker = TradingHoursChecker(dt_time(9, 30), dt_time(16, 0))
            >>> if checker.is_within_hours():
            ...     print("Market is open")
        """
        # Get current time in market timezone
        now_market = datetime.now(self.market_tz).time()

        # Handle midnight crossing (e.g., 22:00-02:00)
        if self.start_time <= self.end_time:
            return self.start_time <= now_market <= self.end_time
        else:
            return now_market >= self.start_time or now_market <= self.end_time

    def seconds_until_active(self) -> float:
        """
        Calculate seconds until next active period starts.

        Returns:
            Seconds until active period (0.0 if currently active)

        Example:
            >>> checker = TradingHoursChecker(dt_time(9, 30), dt_time(16, 0))
            >>> seconds = checker.seconds_until_active()
            >>> if seconds > 0:
            ...     print(f"Market opens in {seconds/3600:.1f} hours")
        """
        if self.is_within_hours():
            return 0.0

        # Get current time in market timezone
        now_market = datetime.now(self.market_tz)
        today_start = now_market.replace(
            hour=self.start_time.hour,
            minute=self.start_time.minute,
            second=0,
            microsecond=0,
        )

        if now_market.time() < self.start_time:
            # Start time is later today
            return (today_start - now_market).total_seconds()
        else:
            # Start time is tomorrow
            tomorrow_start = today_start + timedelta(days=1)
            return (tomorrow_start - now_market).total_seconds()

    def next_active_time(self) -> datetime:
        """
        Get the next time trading hours become active.

        Returns:
            Datetime in system timezone when trading hours start

        Example:
            >>> checker = TradingHoursChecker(dt_time(9, 30), dt_time(16, 0))
            >>> next_open = checker.next_active_time()
            >>> print(f"Market opens at {next_open.strftime('%Y-%m-%d %H:%M %Z')}")
        """
        seconds = self.seconds_until_active()
        now_system = datetime.now(self.system_tz)
        return now_system + timedelta(seconds=seconds)

    def _calculate_local_hours(self) -> str:
        """
        Calculate trading hours in system timezone.

        Returns:
            Human-readable string showing local hours

        Example:
            For US/Eastern 09:30-16:00 when system is Europe/Berlin:
            Returns: "15:30-22:00 CET" or "16:30-23:00 CEST" (depending on DST)
        """
        # Create datetime objects for today in market timezone
        now_market = datetime.now(self.market_tz)
        market_open = now_market.replace(
            hour=self.start_time.hour,
            minute=self.start_time.minute,
            second=0,
            microsecond=0,
        )
        market_close = now_market.replace(
            hour=self.end_time.hour,
            minute=self.end_time.minute,
            second=0,
            microsecond=0,
        )

        # Handle midnight crossing for close time
        if self.end_time < self.start_time:
            market_close += timedelta(days=1)

        # Convert to system timezone
        local_open = market_open.astimezone(self.system_tz)
        local_close = market_close.astimezone(self.system_tz)

        # Format with timezone abbreviation
        open_str = local_open.strftime("%H:%M")
        close_str = local_close.strftime("%H:%M")
        tz_str = local_open.strftime("%Z")

        # Handle day boundary crossing
        if local_open.date() != local_close.date():
            return (
                f"{open_str} {tz_str} {local_open.strftime('%b %d')} - "
                f"{close_str} {tz_str} {local_close.strftime('%b %d')}"
            )
        else:
            return f"{open_str}-{close_str} {tz_str}"

    @staticmethod
    def parse_active_hours(hours_str: str) -> tuple[dt_time, dt_time]:
        """
        Parse active hours string to time objects.

        Args:
            hours_str: Hours string in format "HH:MM-HH:MM" (e.g., "09:30-16:00")

        Returns:
            Tuple of (start_time, end_time)

        Raises:
            ValueError: If format is invalid

        Example:
            >>> start, end = TradingHoursChecker.parse_active_hours("09:30-16:00")
            >>> print(start)  # 09:30:00
            >>> print(end)    # 16:00:00
        """
        try:
            start_str, end_str = hours_str.split("-")
            start_hour, start_min = map(int, start_str.strip().split(":"))
            end_hour, end_min = map(int, end_str.strip().split(":"))
            return dt_time(start_hour, start_min), dt_time(end_hour, end_min)
        except (ValueError, AttributeError) as e:
            raise ValueError(
                f"Invalid active hours format: '{hours_str}'. "
                "Expected format: 'HH:MM-HH:MM' (e.g., '09:30-16:00')"
            ) from e
