"""
Risk Monitor - Monitors risk limits and safety measures.
"""

import logging
from typing import List

from ..live_trader import LiveTrader
from ...clients.mt5.models.history import ClosedPosition
from ...clients.mt5.models.response import Position
from .pnl_calculator import PnLCalculator


class RiskMonitor:
    """
    Monitors risk limits and implements safety measures.
    Single responsibility: Risk monitoring and protection.
    """
    
    def __init__(
        self, 
        trader: LiveTrader, 
        daily_loss_limit: float,
        pnl_calculator: PnLCalculator = None,
        logger: logging.Logger = None
    ):
        self.trader = trader
        self.daily_loss_limit = daily_loss_limit
        self.pnl_calculator = pnl_calculator or PnLCalculator()
        self.logger = logger or logging.getLogger(self.__class__.__name__)
    
    def check_catastrophic_loss_limit(
        self, 
        open_positions: List[Position], 
        closed_positions: List[ClosedPosition]
    ) -> bool:
        """
        Check if catastrophic loss limit has been breached.
        
        Args:
            open_positions: Currently open positions
            closed_positions: Closed positions for the day
            
        Returns:
            True if limit breached and emergency actions taken, False otherwise
        """
        total_pnl = self.pnl_calculator.calculate_total_daily_pnl(
            closed_positions, open_positions
        )
        
        # Calculate ratio for logging, but use direct comparison for logic
        loss_ratio = total_pnl / self.daily_loss_limit if self.daily_loss_limit != 0 else 0
        
        self.logger.info(
            f"Risk Check: Daily PnL={total_pnl:.2f}, Limit={self.daily_loss_limit:.2f}, "
            f"Ratio={loss_ratio:.3f}"
        )

        if loss_ratio < -1:  # Loss exceeds limit
            self.logger.critical(
                f"CATASTROPHIC LOSS LIMIT BREACHED! "
                f"PnL={total_pnl:.2f} exceeds limit of {self.daily_loss_limit:.2f}"
            )
            
            self._execute_emergency_shutdown()
            return True
        
        return False
    
    def _execute_emergency_shutdown(self) -> None:
        """Execute emergency shutdown procedures."""
        try:
            self.logger.warning("Executing emergency shutdown...")
            
            # Close all open positions
            self.trader.close_all_open_position()
            self.logger.warning("All open positions closed")
            
            # Cancel all pending orders
            self.trader.cancel_all_pending_orders()
            self.logger.warning("All pending orders cancelled")
            
            self.logger.critical("Emergency shutdown completed")
            
        except Exception as e:
            self.logger.error(f"Error during emergency shutdown: {e}")
    
    def get_risk_metrics(
        self, 
        open_positions: List[Position], 
        closed_positions: List[ClosedPosition]
    ) -> dict:
        """
        Get current risk metrics.
        
        Returns:
            Dictionary with risk metrics
        """
        total_pnl = self.pnl_calculator.calculate_total_daily_pnl(
            closed_positions, open_positions
        )
        
        floating_pnl = self.pnl_calculator.calculate_floating_pnl(open_positions)
        closed_pnl = self.pnl_calculator.calculate_closed_pnl(closed_positions)
        
        return {
            'daily_pnl': closed_pnl,  # Daily realized PnL from closed positions
            'floating_pnl': floating_pnl,  # Unrealized PnL from open positions
            'total_pnl': total_pnl,  # Total PnL (daily + floating)
            'total_daily_pnl': total_pnl,  # For backward compatibility
            'daily_loss_limit': self.daily_loss_limit,
            'loss_ratio': total_pnl / self.daily_loss_limit if self.daily_loss_limit != 0 else 0,
            'open_positions_count': len(open_positions),
            'closed_pnl': closed_pnl  # For backward compatibility
        }