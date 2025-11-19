"""
Positions client for MT5 API.
"""

from typing import Any, Dict, List, Optional

from app.clients.mt5.base import BaseClient
from app.clients.mt5.models.response import (
    Position,
    PositionUpdateRequest,
    PositionCloseRequest,
    CloseAllPositionsRequest
)
from app.clients.mt5.utils import validate_symbol, validate_ticket


class PositionsClient(BaseClient):
    """Client for managing MT5 positions."""

    def get_open_positions(self) -> List[Position]:
        """
        Get all currently open trading positions.

        Returns:
            List of open positions.
        """
        data = self.get("positions")
        return [Position(**pos) for pos in data] if data else []

    def get_positions_by_symbol(self, symbol: str) -> List[Position]:
        """
        Get all open positions filtered by trading symbol.

        Args:
            symbol: Trading symbol (e.g., EURUSD)

        Returns:
            List of positions filtered by the symbol.
        """
        symbol = validate_symbol(symbol)
        data = self.get(f"positions/{symbol}")
        return [Position(**pos) for pos in data] if data else []

    def get_position_by_ticket(self, ticket: int) -> Optional[Position]:
        """
        Get a specific open position by its ticket number.

        Args:
            ticket: Unique ticket ID of the position.

        Returns:
            Position matching the ticket, or None if not found.
        """
        ticket = validate_ticket(ticket)
        data = self.get(f"position/{ticket}")
        if isinstance(data, list) and data:
            return Position(**data[0])  #  Take the first dict
        elif isinstance(data, dict):
            return Position(**data)
        return None

    def modify_position(
            self,
            symbol: str,
            ticket: int,
            stop_loss: Optional[float] = None,
            take_profit: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Update stop loss or take profit of a position.

        Args:
            symbol: Trading symbol of the position.
            ticket: Ticket ID of the position to modify.
            stop_loss: New stop loss level.
            take_profit: New take profit level.

        Returns:
            Response indicating success or validation failure.
        """
        symbol = validate_symbol(symbol)
        ticket = validate_ticket(ticket)

        # if stop_loss and take_profit are both none, raise error, at least 1 should be provided

        update_data = PositionUpdateRequest(
            stop_loss=stop_loss,
            take_profit=take_profit
        )

        return self.post(
            f"positions/{symbol}/update/{ticket}",
            json_data=update_data.model_dump()
        )

    def close_position(
            self,
            symbol: str,
            ticket: int,
            volume: float,
    ) -> Dict[str, Any]:
        """
        Close a specific open position.

        Args:
            symbol: Symbol of the position to close.
            ticket: Ticket ID of the position to close.
            volume: Volume to close.

        Returns:
            Response indicating whether the close operation succeeded.
        """
        symbol = validate_symbol(symbol)
        ticket = validate_ticket(ticket)

        close_data = PositionCloseRequest(volume=volume)

        return self.post(
            f"positions/{symbol}/close/{ticket}",
            json_data=close_data.model_dump()
        )

    def close_all_positions(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """
        Close all open positions, or all positions for a specific symbol if provided.

        Args:
            symbol: Optional symbol to close only positions for that symbol.

        Returns:
            Response indicating initiation of the close operation.
        """
        close_data = CloseAllPositionsRequest(symbol=symbol)

        return self.post(
            "positions/close_all",
            json_data=close_data.model_dump()
        )
