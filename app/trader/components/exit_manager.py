"""
Exit Manager - Handles closing positions based on exit signals.
"""

import logging
from typing import List

from ..live_trader import LiveTrader
from ...clients.mt5.models.response import Position
from ...strategy_builder.data.dtos import ExitDecision


class ExitManager:
    """
    Handles closing positions based on exit signals.
    Single responsibility: Position exit management.
    """
    
    def __init__(self, trader: LiveTrader, logger: logging.Logger = None):
        self.trader = trader
        self.logger = logger or logging.getLogger(self.__class__.__name__)
    
    def process_exits(self, exits: List[ExitDecision], open_positions: List[Position]) -> None:
        """
        Process exit signals and close matching positions.
        
        Args:
            exits: List of exit decisions
            open_positions: List of currently open positions
        """
        if not exits:
            self.logger.debug("No exit signals to process")
            return
        
        for exit_trade in exits:
            self._process_single_exit(exit_trade, open_positions)
    
    def _process_single_exit(self, exit_trade: ExitDecision, open_positions: List[Position]) -> None:
        """Process a single exit signal."""
        exit_type = 0 if exit_trade.direction == "long" else 1
        magic = exit_trade.magic
        symbol = exit_trade.symbol
        
        matched_positions = []
        
        for position in open_positions:
            if (position.symbol == symbol and 
                position.magic == magic and 
                position.type == exit_type):
                matched_positions.append(position)
        
        if not matched_positions:
            self.logger.warning(
                f"No matching positions found for exit: {symbol} {exit_trade.direction} "
                f"(magic={magic})"
            )
            return
        
        # Close all matching positions
        for position in matched_positions:
            try:
                self.trader.close_open_position(symbol, position.ticket)
                self.logger.info(
                    f"Closed position: ticket={position.ticket}, symbol={symbol}, "
                    f"direction={exit_trade.direction}, magic={magic}"
                )
            except Exception as e:
                self.logger.error(
                    f"Failed to close position {position.ticket}: {e}"
                )