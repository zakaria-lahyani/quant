"""
History client for MT5 API.
"""

from typing import Any, Dict, List, Optional

from app.clients.mt5.base import BaseClient
from app.clients.mt5.utils import format_datetime, parse_datetime, validate_ticket

class HistoryClient(BaseClient):
    """Client for retrieving MT5 historical trade data."""

    def get_closed_positions(
            self,
            start: Optional[str] = None,
            end: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get closed positions with optional date range filtering.

        If both start and end are not provided, returns all available history.
        If only start is provided, returns all history from that date to now.
        If only end is provided, returns all history up to that date.
        If both are provided, returns history within that range.

        Args:
            start: Optional start datetime
            end: Optional end datetime

        Returns:
            List of closed positions with details

        Raises:
            MT5APIError: If parameters are invalid or data fetching fails
        """
        params = {}

        if start:
            params['start'] = format_datetime(start)
        if end:
            params['end'] = format_datetime(end)

        data = self.get("history/closed_positions", params=params)

        # Parse datetime fields if they're strings
        if data:
            for position in data:
                for field in ['time_open', 'time_close', 'time']:
                    if field in position and isinstance(position[field], str):
                        position[field] = parse_datetime(position[field])

        return data if data else []

    def get_closed_position_by_ticket(self, ticket: int) -> Optional[Dict[str, Any]]:
        """
        Get a specific closed position by ticket.

        Args:
            ticket: Deal ticket number

        Returns:
            Closed position details, or None if not found

        Raises:
            MT5APIError: If ticket is invalid or position not found
        """
        ticket = validate_ticket(ticket)
        data = self.get(f"history/closed_positions/{ticket}")

        # Parse datetime fields if they're strings
        if data:
            for field in ['time_open', 'time_close', 'time']:
                if field in data and isinstance(data[field], str):
                    data[field] = parse_datetime(data[field])

        return data

    def get_all_closed_positions(self) -> List[Dict[str, Any]]:
        """
        Get all closed positions from account history.

        Returns:
            List of all closed positions
        """
        return self.get_closed_positions()

    def get_closed_positions_by_symbol(
            self,
            symbol: str,
            start: Optional[str] = None,
            end: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get closed positions filtered by symbol.

        Args:
            symbol: Trading symbol to filter by
            start: Optional start datetime
            end: Optional end datetime

        Returns:
            List of closed positions for the specified symbol
        """
        all_positions = self.get_closed_positions(start=start, end=end)
        return [pos for pos in all_positions if pos.get('symbol') == symbol.upper()]

    def get_profitable_positions(
            self,
            start: Optional[str] = None,
            end: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get only profitable closed positions.

        Args:
            start: Optional start datetime
            end: Optional end datetime

        Returns:
            List of profitable closed positions
        """
        all_positions = self.get_closed_positions(start=start, end=end)
        return [pos for pos in all_positions if pos.get('profit', 0) > 0]

    def get_losing_positions(
            self,
            start: Optional[str] = None,
            end: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get only losing closed positions.

        Args:
            start: Optional start datetime
            end: Optional end datetime

        Returns:
            List of losing closed positions
        """
        all_positions = self.get_closed_positions(start=start, end=end)
        return [pos for pos in all_positions if pos.get('profit', 0) < 0]

    def get_trading_statistics(
            self,
            start: Optional[str] = None,
            end: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get trading statistics for closed positions.

        Args:
            start: Optional start datetime
            end: Optional end datetime

        Returns:
            Dictionary with trading statistics
        """
        positions = self.get_closed_positions(start=start, end=end)

        if not positions:
            return {
                'total_trades': 0,
                'profitable_trades': 0,
                'losing_trades': 0,
                'win_rate': 0.0,
                'total_profit': 0.0,
                'total_loss': 0.0,
                'net_profit': 0.0,
                'average_profit': 0.0,
                'average_loss': 0.0,
                'profit_factor': 0.0,
            }

        profits = [pos.get('profit', 0) for pos in positions]
        profitable_trades = [p for p in profits if p > 0]
        losing_trades = [p for p in profits if p < 0]

        total_profit = sum(profitable_trades)
        total_loss = abs(sum(losing_trades))
        net_profit = sum(profits)

        return {
            'total_trades': len(positions),
            'profitable_trades': len(profitable_trades),
            'losing_trades': len(losing_trades),
            'win_rate': len(profitable_trades) / len(positions) * 100 if positions else 0,
            'total_profit': total_profit,
            'total_loss': total_loss,
            'net_profit': net_profit,
            'average_profit': total_profit / len(profitable_trades) if profitable_trades else 0,
            'average_loss': total_loss / len(losing_trades) if losing_trades else 0,
            'profit_factor': total_profit / total_loss if total_loss > 0 else float('inf') if total_profit > 0 else 0,
        }

