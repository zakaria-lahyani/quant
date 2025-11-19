"""
Main MT5 API Client.
"""

import logging
from typing import Dict, Optional

from .base import BaseClient
from .api.account import AccountClient
from .api.data import DataClient
from .api.history import HistoryClient
from .api.orders import OrdersClient
from .api.positions import PositionsClient
from .api.symbols import SymbolsClient
from .utils import RetryConfig, setup_logging


class MT5Client(BaseClient):
    """
    Main MT5 API Client with all endpoint clients.

    This client provides access to all MT5 API functionality through
    specialized endpoint clients for positions, orders, symbols, data,
    account information, and historical data.

    Example:
        ```python
        # Synchronous usage
        client = MT5Client("http://localhost:8000")

        # Get account info
        account = client.account.get_account_info()

        # Get open positions
        positions = client.positions.get_open_positions()

        # Create a buy order
        result = client.orders.create_buy_order("EURUSD", 0.1)

        # Get historical data
        bars = client.data.get_latest_bars("EURUSD", "H1", 100)

        client.close()
        ```

        ```python
        # Asynchronous usage
        async with MT5Client("http://localhost:8000") as client:
            account = await client.account.aget_account_info()
            positions = await client.positions.aget_open_positions()
            result = await client.orders.acreate_buy_order("EURUSD", 0.1)
            bars = await client.data.aget_latest_bars("EURUSD", "H1", 100)
        ```
    """

    def __init__(
        self,
        base_url: str,
        timeout: float = 30.0,
        retry_config: Optional[RetryConfig] = None,
        headers: Optional[Dict[str, str]] = None,
        enable_logging: bool = True,
        log_level: int = logging.INFO,
    ):
        """
        Initialize MT5 API Client.

        Args:
            base_url: Base URL of the MT5 API server
            timeout: Request timeout in seconds (default: 30.0)
            retry_config: Retry configuration for failed requests
            headers: Additional HTTP headers to send with requests
            enable_logging: Whether to enable logging (default: True)
            log_level: Logging level (default: INFO)
        """
        super().__init__(base_url, timeout, retry_config, headers)

        if enable_logging:
            setup_logging(log_level)

        # Initialize endpoint clients
        self._positions = PositionsClient(base_url, timeout, retry_config, headers)
        self._orders = OrdersClient(base_url, timeout, retry_config, headers)
        self._symbols = SymbolsClient(base_url, timeout, retry_config, headers)
        self._data = DataClient(base_url, timeout, retry_config, headers)
        self._account = AccountClient(base_url, timeout, retry_config, headers)
        self._history = HistoryClient(base_url, timeout, retry_config, headers)

    @property
    def positions(self) -> PositionsClient:
        """Access to positions management endpoints."""
        return self._positions

    @property
    def orders(self) -> OrdersClient:
        """Access to orders management endpoints."""
        return self._orders

    @property
    def symbols(self) -> SymbolsClient:
        """Access to symbols and market data endpoints."""
        return self._symbols

    @property
    def data(self) -> DataClient:
        """Access to historical data endpoints."""
        return self._data

    @property
    def account(self) -> AccountClient:
        """Access to account information endpoints."""
        return self._account

    @property
    def history(self) -> HistoryClient:
        """Access to historical trade data endpoints."""
        return self._history

    def health_check(self) -> Dict[str, str]:
        """
        Perform health check on the MT5 API server.

        Returns:
            Health check response
        """
        return self.get("health")


    def close(self) -> None:
        """Close all HTTP clients."""
        super().close()
        self._positions.close()
        self._orders.close()
        self._symbols.close()
        self._data.close()
        self._account.close()
        self._history.close()


    def __repr__(self) -> str:
        return f"MT5Client(base_url='{self.base_url}')"

    def get_client_info(self) -> Dict[str, str]:
        """
        Get information about the client.

        Returns:
            Dictionary with client information
        """
        return {
            'client_name': 'MT5 API Client',
            'version': '1.0.0',
            'base_url': self.base_url,
            'timeout': str(self.timeout),
            'retry_max_attempts': str(self.retry_config.max_retries),
        }


# Convenience functions for quick client creation

def create_client(
    base_url: str,
    timeout: float = 30.0,
    **kwargs
) -> MT5Client:
    """
    Create a new MT5Client instance.

    Args:
        base_url: Base URL of the MT5 API server
        timeout: Request timeout in seconds
        **kwargs: Additional arguments passed to MT5Client

    Returns:
        Configured MT5Client instance
    """
    return MT5Client(base_url=base_url, timeout=timeout, **kwargs)


def create_client_with_retry(
    base_url: str,
    max_retries: int = 3,
    base_delay: float = 1.0,
    **kwargs
) -> MT5Client:
    """
    Create a new MT5Client instance with custom retry configuration.

    Args:
        base_url: Base URL of the MT5 API server
        max_retries: Maximum number of retry attempts
        base_delay: Base delay between retries in seconds
        **kwargs: Additional arguments passed to MT5Client

    Returns:
        Configured MT5Client instance with retry settings
    """
    retry_config = RetryConfig(
        max_retries=max_retries,
        base_delay=base_delay,
    )
    return MT5Client(base_url=base_url, retry_config=retry_config, **kwargs)
