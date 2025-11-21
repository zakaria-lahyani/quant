"""
Utility functions for MT5 API Client.
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import Any, Callable, Dict, Optional, TypeVar, Union

import httpx

from .exceptions import MT5ConnectionError, MT5TimeoutError, MT5RateLimitError

T = TypeVar('T')

logger = logging.getLogger(__name__)


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Set up logging for the MT5 client."""
    logger = logging.getLogger('mt5_client')
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Parse datetime string in ISO format."""
    if not dt_str:
        return None

    try:
        # Handle different datetime formats
        if dt_str.endswith('Z'):
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        return datetime.fromisoformat(dt_str)
    except ValueError as e:
        logger.warning(f"Failed to parse datetime '{dt_str}': {e}")
        return None


def format_datetime(dt: Optional[Union[datetime, str]]) -> Optional[str]:
    """Format datetime or string to ISO string."""
    try:
        if isinstance(dt, datetime):
            return dt.isoformat()
        if isinstance(dt, str):
            return datetime.fromisoformat(dt.replace("Z", "+00:00")).isoformat()
        raise ValueError("Invalid datetime type")
    except ValueError as e:
        raise ValueError(f"Invalid datetime format: {dt}") from e


def validate_symbol(symbol: str) -> str:
    """
    Validate symbol name.

    NOTE: Does NOT force uppercase - preserves broker-specific case sensitivity.
    E.g., ACG broker may use 'XAUUSD.pro' while FTMO uses 'XAUUSD'.
    """
    if not symbol or not isinstance(symbol, str):
        raise ValueError("Symbol must be a non-empty string")
    # Only strip whitespace - preserve case for broker compatibility
    return symbol.strip()


def validate_volume(volume: float) -> float:
    """Validate trading volume."""
    if not isinstance(volume, (int, float)) or volume <= 0:
        raise ValueError("Volume must be a positive number")
    return float(volume)


def normalize_volume(volume: float, volume_step: float = 0.01) -> float:
    """
    Normalize trading volume to broker's volume step.

    Args:
        volume: Raw volume to normalize
        volume_step: Broker's minimum volume step (default: 0.01 for most brokers)

    Returns:
        Normalized volume rounded to the nearest volume step

    Example:
        >>> normalize_volume(0.09505657810293679, 0.01)
        0.10
        >>> normalize_volume(0.09505657810293679, 0.001)
        0.095
    """
    if volume_step <= 0:
        raise ValueError("volume_step must be positive")

    # Round to nearest volume step
    normalized = round(volume / volume_step) * volume_step

    # Ensure minimum volume
    if normalized < volume_step:
        normalized = volume_step

    # Round to avoid floating point precision issues
    # Determine decimal places from volume_step
    import math
    decimal_places = abs(int(math.floor(math.log10(volume_step))))

    return round(normalized, decimal_places)


def validate_ticket(ticket: int) -> int:
    """Validate ticket number."""
    if not isinstance(ticket, int) or ticket <= 0:
        raise ValueError("Ticket must be a positive integer")
    return ticket


class RetryConfig:
    """Configuration for retry logic."""

    def __init__(
            self,
            max_retries: int = 3,
            base_delay: float = 1.0,
            max_delay: float = 60.0,
            exponential_base: float = 2.0,
            jitter: bool = True,
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter


async def retry_async(
        func: Callable[..., T],
        config: RetryConfig,
        *args,
        **kwargs
) -> T:
    """Retry an async function with exponential backoff."""
    last_exception = None

    for attempt in range(config.max_retries + 1):
        try:
            return await func(*args, **kwargs)
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            last_exception = e
            if attempt == config.max_retries:
                break

            delay = min(
                config.base_delay * (config.exponential_base ** attempt),
                config.max_delay
            )

            if config.jitter:
                import random
                delay *= (0.5 + random.random() * 0.5)

            logger.warning(
                f"Request failed (attempt {attempt + 1}/{config.max_retries + 1}), "
                f"retrying in {delay:.2f}s: {e}"
            )
            await asyncio.sleep(delay)
        except httpx.HTTPStatusError as e:
            # Don't retry on HTTP errors (4xx, 5xx)
            if e.response.status_code == 429:  # Rate limit
                retry_after = e.response.headers.get('Retry-After')
                if retry_after and attempt < config.max_retries:
                    delay = min(int(retry_after), config.max_delay)
                    logger.warning(f"Rate limited, retrying in {delay}s")
                    await asyncio.sleep(delay)
                    continue
                else:
                    raise MT5RateLimitError(
                        "Rate limit exceeded",
                        retry_after=int(retry_after) if retry_after else None
                    )
            raise

    # Convert connection/timeout errors to custom exceptions
    if isinstance(last_exception, httpx.ConnectError):
        raise MT5ConnectionError(f"Failed to connect to MT5 API: {last_exception}")
    elif isinstance(last_exception, httpx.TimeoutException):
        raise MT5TimeoutError(f"Request to MT5 API timed out: {last_exception}")
    else:
        raise last_exception


def retry_sync(
        func: Callable[..., T],
        config: RetryConfig,
        *args,
        **kwargs
) -> T:
    """Retry a sync function with exponential backoff."""
    last_exception = None

    for attempt in range(config.max_retries + 1):
        try:
            return func(*args, **kwargs)
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            last_exception = e
            if attempt == config.max_retries:
                break

            delay = min(
                config.base_delay * (config.exponential_base ** attempt),
                config.max_delay
            )

            if config.jitter:
                import random
                delay *= (0.5 + random.random() * 0.5)

            logger.warning(
                f"Request failed (attempt {attempt + 1}/{config.max_retries + 1}), "
                f"retrying in {delay:.2f}s: {e}"
            )
            time.sleep(delay)
        except httpx.HTTPStatusError as e:
            # Don't retry on HTTP errors (4xx, 5xx)
            if e.response.status_code == 429:  # Rate limit
                retry_after = e.response.headers.get('Retry-After')
                if retry_after and attempt < config.max_retries:
                    delay = min(int(retry_after), config.max_delay)
                    logger.warning(f"Rate limited, retrying in {delay}s")
                    time.sleep(delay)
                    continue
                else:
                    raise MT5RateLimitError(
                        "Rate limit exceeded",
                        retry_after=int(retry_after) if retry_after else None
                    )
            raise

    # Convert connection/timeout errors to custom exceptions
    if isinstance(last_exception, httpx.ConnectError):
        raise MT5ConnectionError(f"Failed to connect to MT5 API: {last_exception}")
    elif isinstance(last_exception, httpx.TimeoutException):
        raise MT5TimeoutError(f"Request to MT5 API timed out: {last_exception}")
    else:
        raise last_exception


def build_url(base_url: str, path: str, **params) -> str:
    """Build URL with query parameters."""
    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"

    if params:
        query_params = []
        for key, value in params.items():
            if value is not None:
                query_params.append(f"{key}={value}")

        if query_params:
            url += "?" + "&".join(query_params)

    return url


def handle_response_errors(response: httpx.Response) -> None:
    """Handle HTTP response errors."""
    if response.status_code >= 400:
        try:
            error_data = response.json()
            message = error_data.get('message', f'HTTP {response.status_code} error')
            error_code = error_data.get('error_code')
        except Exception:
            message = f'HTTP {response.status_code} error'
            error_code = None

        from .exceptions import MT5APIError
        raise MT5APIError(
            message=message,
            status_code=response.status_code,
            error_code=error_code,
            details={'response': response.text}
        )
