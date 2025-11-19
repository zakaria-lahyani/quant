"""
Custom exceptions for MT5 API Client.
"""

from typing import Any, Dict, Optional


class MT5ClientError(Exception):
    """Base exception for all MT5 client errors."""

    def __init__(self, message: str, error_code: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}


class MT5APIError(MT5ClientError):
    """Exception raised when the MT5 API returns an error response."""

    def __init__(
            self,
            message: str,
            status_code: int,
            error_code: Optional[str] = None,
            details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message, error_code, details)
        self.status_code = status_code


class MT5ValidationError(MT5ClientError):
    """Exception raised when request validation fails."""

    def __init__(self, message: str, validation_errors: Optional[list] = None):
        super().__init__(message)
        self.validation_errors = validation_errors or []


class MT5ConnectionError(MT5ClientError):
    """Exception raised when connection to MT5 API fails."""
    pass


class MT5TimeoutError(MT5ClientError):
    """Exception raised when request to MT5 API times out."""
    pass


class MT5AuthenticationError(MT5ClientError):
    """Exception raised when authentication fails."""
    pass


class MT5RateLimitError(MT5ClientError):
    """Exception raised when rate limit is exceeded."""

    def __init__(self, message: str, retry_after: Optional[int] = None):
        super().__init__(message)
        self.retry_after = retry_after
