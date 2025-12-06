from __future__ import annotations

from typing import Callable, Protocol


class RateLimiter(Protocol):
    def enforce_limits(self) -> None: ...


class CallableRateLimiter:
    """Wrap a no-arg callable behind the RateLimiter interface."""

    def __init__(self, func: Callable[[], None]):
        self._func = func

    def enforce_limits(self) -> None:
        self._func()


def wrap_callable(func: Callable[[], None]) -> RateLimiter:
    return CallableRateLimiter(func)
