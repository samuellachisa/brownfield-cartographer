"""Retry and rate limiting utilities for LLM calls."""

from __future__ import annotations

import time
from functools import wraps
from typing import Callable, TypeVar

T = TypeVar("T")


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Exponential backoff retry decorator."""

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @wraps(fn)
        def wrapper(*args: object, **kwargs: object) -> T:
            delay = base_delay
            last_exc: Exception | None = None
            for attempt in range(max_retries + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt == max_retries:
                        raise
                    time.sleep(min(delay, max_delay))
                    delay *= 2
            raise last_exc or RuntimeError("retry exhausted")

        return wrapper

    return decorator


class RateLimiter:
    """Simple token-bucket style rate limiter."""

    def __init__(self, requests_per_minute: float = 60.0) -> None:
        self.interval = 60.0 / max(1, requests_per_minute)
        self.last_call = 0.0

    def wait(self) -> None:
        elapsed = time.monotonic() - self.last_call
        if elapsed < self.interval:
            time.sleep(self.interval - elapsed)
        self.last_call = time.monotonic()
