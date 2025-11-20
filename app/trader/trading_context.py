"""
Trading Context - Shared state and configuration for trading operations.
"""

from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime

from app.clients.mt5.models.history import ClosedPosition
from app.clients.mt5.models.order import PendingOrder
from app.clients.mt5.models.response import Position


@dataclass
class MarketState:
    """Current market state snapshot."""
    open_positions: List[Position]
    pending_orders: List[PendingOrder]
    closed_positions: List[ClosedPosition]
    timestamp: datetime
    
    @property
    def has_open_positions(self) -> bool:
        """Check if there are any open positions."""
        return len(self.open_positions) > 0
    
    @property
    def has_pending_orders(self) -> bool:
        """Check if there are any pending orders."""
        return len(self.pending_orders) > 0


@dataclass
class TradingContext:
    """
    Shared context for trading operations.
    Contains all state that needs to be shared between components.
    """
    # Trading state
    trade_authorized: bool = True
    risk_breached: bool = False
    
    # Market state
    market_state: Optional[MarketState] = None
    
    # Timing
    current_time: Optional[datetime] = None
    
    # Restrictions
    news_block_active: bool = False
    market_closing_soon: bool = False
    
    # Metrics
    daily_pnl: float = 0.0
    floating_pnl: float = 0.0
    total_pnl: float = 0.0
    
    def can_trade(self) -> bool:
        """Check if trading is allowed based on all conditions."""
        return self.trade_authorized and not self.risk_breached
    
    def update_market_state(self, market_state: MarketState) -> None:
        """Update the market state."""
        self.market_state = market_state
        
    def block_trading(self, reason: str) -> None:
        """Block trading with a specific reason."""
        self.trade_authorized = False
        
    def allow_trading(self) -> None:
        """Allow trading."""
        self.trade_authorized = True
        
    def set_risk_breach(self, breached: bool) -> None:
        """Set risk breach status."""
        self.risk_breached = breached