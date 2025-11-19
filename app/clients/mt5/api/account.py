"""
Account client for MT5 API.
"""

from typing import Any, Dict

from app.clients.mt5.base import BaseClient


class AccountClient(BaseClient):
    """Client for retrieving MT5 account information."""

    def get_account_info(self) -> Dict[str, Any]:
        """
        Get full account information.

        Returns:
            Account information including login, balance, equity, margin, etc.

        Raises:
            MT5APIError: If account information cannot be retrieved
        """
        return self.get("account")

    def get_balance(self) -> float:
        """
        Get account balance.

        Returns:
            Account balance

        Raises:
            MT5APIError: If balance cannot be retrieved
        """
        account_balance = self.get("account/balance")
        return account_balance["balance"]

    def get_equity(self) -> Dict[str, float]:
        """
        Get account equity.

        Returns:
            Account equity

        Raises:
            MT5APIError: If equity cannot be retrieved
        """
        return self.get("account/equity")


    def get_margin_info(self) -> Dict[str, float]:
        """
        Get margin information.

        Returns:
            Margin information including margin, free margin, and margin level

        Raises:
            MT5APIError: If margin information cannot be retrieved
        """
        return self.get("account/margin")


    def get_leverage(self) -> Dict[str, int]:
        """
        Get account leverage.

        Returns:
            Account leverage

        Raises:
            MT5APIError: If leverage cannot be retrieved
        """
        return self.get("account/leverage")

    def get_account_summary(self) -> Dict[str, Any]:
        """
        Get a summary of key account metrics.

        Returns:
            Dictionary with balance, equity, margin, free margin, and margin level
        """
        account_info = self.get_account_info()

        return {
            'balance': account_info.get('balance', 0.0),
            'equity': account_info.get('equity', 0.0),
            'margin': account_info.get('margin', 0.0),
            'margin_free': account_info.get('margin_free', 0.0),
            'margin_level': account_info.get('margin_level', 0.0),
            'profit': account_info.get('profit', 0.0),
            'currency': account_info.get('currency', ''),
            'leverage': account_info.get('leverage', 1),
        }


    def get_free_margin(self) -> float:
        """
        Get available free margin.

        Returns:
            Free margin amount
        """
        margin_info = self.get_margin_info()
        return margin_info.get('margin_free', 0.0)

    def get_margin_level(self) -> float:
        """
        Get margin level percentage.

        Returns:
            Margin level as percentage
        """
        margin_info = self.get_margin_info()
        return margin_info.get('margin_level', 0.0)


    def get_profit(self) -> float:
        """
        Get current floating profit/loss.

        Returns:
            Current profit/loss
        """
        account_info = self.get_account_info()
        return account_info.get('profit', 0.0)

    def is_margin_call(self, margin_call_level: float = 100.0) -> bool:
        """
        Check if account is in margin call.

        Args:
            margin_call_level: Margin call level percentage (default: 100%)

        Returns:
            True if margin level is below margin call level
        """
        margin_level = self.get_margin_level()
        return margin_level < margin_call_level if margin_level > 0 else False


    def is_stop_out(self, stop_out_level: float = 50.0) -> bool:
        """
        Check if account is in stop out.

        Args:
            stop_out_level: Stop out level percentage (default: 50%)

        Returns:
            True if margin level is below stop out level
        """
        margin_level = self.get_margin_level()
        return margin_level < stop_out_level if margin_level > 0 else False

