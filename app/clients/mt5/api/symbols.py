"""
Symbols client for MT5 API.
"""

from typing import Any, Dict, List, Optional

from app.clients.mt5.base import BaseClient
from app.clients.mt5.models.response import (
    SymbolSelectRequest,
    SymbolSelectResponse,
    SymbolTradableResponse,
)
from app.clients.mt5.utils import validate_symbol



class SymbolsClient(BaseClient):
    """Client for managing MT5 symbols and market data."""

    def get_all_symbols(self) -> List[str]:
        """
        Get all available symbols.

        Returns:
            List of all available trading symbols.
        """
        data = self.get("symbols")
        return data if data else []

    def get_symbol_info(self, symbol: str) -> Dict[str, Any]:
        """
        Get detailed symbol information.

        Args:
            symbol: Symbol name to get information for

        Returns:
            Detailed symbol information including specifications and current prices

        Raises:
            MT5APIError: If symbol is not found
        """
        symbol = validate_symbol(symbol)
        return self.get(f"symbols/{symbol}/info")

    def get_symbol_tick(self, symbol: str) -> Dict[str, Any]:
        """
        Get latest tick data for the symbol.

        Args:
            symbol: Symbol name to get tick data for

        Returns:
            Latest tick information including bid, ask, and last prices

        Raises:
            MT5APIError: If tick data cannot be retrieved
        """
        symbol = validate_symbol(symbol)
        return self.get(f"symbols/{symbol}/tick")

    def is_symbol_tradable(self, symbol: str) -> SymbolTradableResponse:
        """
        Check if symbol is currently tradable.

        Args:
            symbol: Symbol name to check

        Returns:
            Object indicating whether the symbol is tradable
        """
        symbol = validate_symbol(symbol)
        data = self.get(f"symbols/{symbol}/tradable")
        return SymbolTradableResponse(**data)

    def select_symbol(self, symbol: str, enable: bool = True) -> SymbolSelectResponse:
        """
        Add or remove a symbol from Market Watch.

        Args:
            symbol: Symbol name
            enable: True to add to Market Watch, False to remove

        Returns:
            Result of the operation
        """
        symbol = validate_symbol(symbol)

        request_data = SymbolSelectRequest(symbol=symbol, enable=enable)
        data = self.post("symbols/select", json_data=request_data.model_dump())
        return SymbolSelectResponse(**data)

    def add_symbol_to_market_watch(self, symbol: str) -> SymbolSelectResponse:
        """
        Add a symbol to Market Watch.

        Args:
            symbol: Symbol name

        Returns:
            Result of the operation
        """
        return self.select_symbol(symbol, enable=True)

    def remove_symbol_from_market_watch(self, symbol: str) -> SymbolSelectResponse:
        """
        Remove a symbol from Market Watch.

        Args:
            symbol: Symbol name

        Returns:
            Result of the operation
        """
        return self.select_symbol(symbol, enable=False)

    def get_symbol_price(self, symbol: str) -> Dict[str, float]:
        """
        Get current bid/ask prices for a symbol.

        Args:
            symbol: Symbol name

        Returns:
            Dictionary with bid and ask prices
        """
        tick_data = self.get_symbol_tick(symbol)
        return {
            'bid': tick_data.get('bid', 0.0),
            'ask': tick_data.get('ask', 0.0),
            'last': tick_data.get('last', 0.0),
        }

    def get_symbol_spread(self, symbol: str) -> float:
        """
        Get current spread for a symbol.

        Args:
            symbol: Symbol name

        Returns:
            Current spread in points
        """
        prices = self.get_symbol_price(symbol)
        return prices['ask'] - prices['bid']

