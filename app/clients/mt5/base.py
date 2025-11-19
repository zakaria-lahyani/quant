"""
Base client class for MT5 API Client.
"""

import logging
from typing import Any, Dict, Optional, Union

import httpx

from .exceptions import MT5APIError, MT5ValidationError
from .models.response import APIResponse
from .utils import RetryConfig, handle_response_errors, retry_sync

logger = logging.getLogger(__name__)


class BaseClient:
    """Base client class with common functionality."""

    def __init__(
        self,
        base_url: str,
        timeout: float = 30.0,
        retry_config: Optional[RetryConfig] = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.retry_config = retry_config or RetryConfig()
        self.headers = headers or {}

        # Default headers
        self.headers.setdefault('Content-Type', 'application/json')
        self.headers.setdefault('Accept', 'application/json')

        # HTTP clients (will be initialized when needed)
        self._sync_client: Optional[httpx.Client] = None
        self._async_client: Optional[httpx.AsyncClient] = None

    @property
    def sync_client(self) -> httpx.Client:
        """Get or create sync HTTP client."""
        if self._sync_client is None:
            self._sync_client = httpx.Client(
                timeout=self.timeout,
                headers=self.headers,
            )
        return self._sync_client


    def close(self) -> None:
        """Close sync HTTP client."""
        if self._sync_client:
            self._sync_client.close()
            self._sync_client = None


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


    def _build_url(self, path: str) -> str:
        """Build full URL from path."""
        return f"{self.base_url}/{path.lstrip('/')}"

    def _handle_response(self, response: httpx.Response) -> Any:
        """Handle HTTP response and extract data."""
        handle_response_errors(response)

        try:
            data = response.json()
        except Exception as e:
            logger.error(f"Failed to parse JSON response: {e}")
            raise MT5APIError(
                message="Invalid JSON response from server",
                status_code=response.status_code,
                details={'response': response.text}
            )

        # Handle API response format
        if isinstance(data, dict) and 'success' in data:
            api_response = APIResponse(**data)
            if not api_response.success:
                raise MT5APIError(
                    message=api_response.message or "API request failed",
                    status_code=response.status_code,
                    error_code=api_response.error_code,
                    details={'response_data': data}
                )
            return api_response.data

        return data

    def _request_sync(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Make synchronous HTTP request."""
        url = self._build_url(path)
        print(f"url : {url}")
        def make_request():
            response = self.sync_client.request(
                method=method,
                url=url,
                params=params,
                json=json_data,
            )
            return self._handle_response(response)

        return retry_sync(make_request, self.retry_config)


    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Make synchronous GET request."""
        print(f"path: {path}")
        return self._request_sync("GET", path, params=params)


    def post(
        self,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Make synchronous POST request."""
        return self._request_sync("POST", path, params=params, json_data=json_data)


    def put(
        self,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Make synchronous PUT request."""
        return self._request_sync("PUT", path, params=params, json_data=json_data)


    def delete(
        self,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Make synchronous DELETE request."""
        return self._request_sync("DELETE", path, params=params, json_data=json_data)

