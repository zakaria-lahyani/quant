"""
PnL Calculator - Calculates profit and loss metrics.
"""

import logging
from typing import List

from ...clients.mt5.models.history import ClosedPosition
from ...clients.mt5.models.response import Position


class PnLCalculator:
    """
    Calculates various profit and loss metrics.
    Single responsibility: PnL calculations.
    """
    
    def __init__(self, logger: logging.Logger = None):
        self.logger = logger or logging.getLogger(self.__class__.__name__)
    
    def calculate_closed_pnl(self, closed_positions: List[ClosedPosition]) -> float:
        """
        Calculate total PnL from closed positions.
        
        Args:
            closed_positions: List of closed positions
            
        Returns:
            Total PnL including profit, commissions, and swaps
        """
        if not closed_positions:
            return 0.0
        
        profit = sum(pos.profit for pos in closed_positions)
        commissions = sum(pos.commission for pos in closed_positions)
        swaps = sum(pos.swap for pos in closed_positions)
        
        total_pnl = profit + commissions + swaps
        
        self.logger.debug(
            f"Closed PnL: profit={profit:.2f}, commissions={commissions:.2f}, "
            f"swaps={swaps:.2f}, total={total_pnl:.2f}"
        )
        
        return total_pnl
    
    def calculate_floating_pnl(self, open_positions: List[Position]) -> float:
        """
        Calculate floating PnL from open positions.
        
        Args:
            open_positions: List of open positions
            
        Returns:
            Total floating PnL including unrealized profit and swaps
        """
        if not open_positions:
            return 0.0
        
        profit = sum(pos.profit for pos in open_positions)
        swaps = sum(pos.swap for pos in open_positions)
        
        total_floating_pnl = profit + swaps
        
        self.logger.debug(
            f"Floating PnL: profit={profit:.2f}, swaps={swaps:.2f}, "
            f"total={total_floating_pnl:.2f}"
        )
        
        return total_floating_pnl
    
    def calculate_total_daily_pnl(
        self, 
        closed_positions: List[ClosedPosition], 
        open_positions: List[Position]
    ) -> float:
        """
        Calculate total daily PnL (closed + floating).
        
        Args:
            closed_positions: List of closed positions
            open_positions: List of open positions
            
        Returns:
            Total daily PnL
        """
        closed_pnl = self.calculate_closed_pnl(closed_positions)
        floating_pnl = self.calculate_floating_pnl(open_positions)
        
        total_pnl = closed_pnl + floating_pnl
        
        self.logger.info(
            f"Daily PnL Summary: closed={closed_pnl:.2f}, floating={floating_pnl:.2f}, "
            f"total={total_pnl:.2f}"
        )
        
        return total_pnl