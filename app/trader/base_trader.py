from abc import ABC, abstractmethod
from typing import Optional

from app.clients.mt5.models.response import Position
from app.clients.mt5.models.order import PendingOrder
from app.clients.mt5.models.history import ClosedPosition
from app.trader.risk_manager.models import RiskEntryResult


class BaseTrader(ABC):

    @abstractmethod
    def get_current_price(self, symbol:str):
        pass

    """
    Orders Methodes
    """
    @abstractmethod
    def open_pending_order(self, trade:RiskEntryResult):
        pass

    @abstractmethod
    def get_pending_orders(self, symbol: Optional[str] = None)-> list[PendingOrder]:
        pass

    @abstractmethod
    def update_pending_orders(self, ticket: int, price: float = None, sl: float = None, tp: float = None, comment: str = ""):
        pass

    @abstractmethod
    def cancel_pending_orders(self, ticket: int):
        pass

    @abstractmethod
    def cancel_all_pending_orders(self, symbol: Optional[str] = None):
        pass

    """
    Positions Methodes
    """
    @abstractmethod
    def get_open_positions(self, symbol: Optional[str] = None) -> list[Position]:
        pass

    @abstractmethod
    def update_open_position(self, symbol: str, ticket: int, sl: Optional[float] = None, tp: Optional[float] = None):
        """Update position status in tracking system"""
        pass

    @abstractmethod
    def close_open_position(self, symbol: str, ticket: int, volume: float = None):
        """Close an open position"""
        pass

    @abstractmethod
    def close_all_open_position(self):
        """Close an open position"""
        pass

    """
    Closed Positions Methodes
    """
    @abstractmethod
    def get_closed_positions(self, start: str, end: str) -> list[ClosedPosition]:
        pass
