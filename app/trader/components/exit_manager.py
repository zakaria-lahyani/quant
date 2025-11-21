"""
Exit Manager - Handles closing positions based on exit signals.
"""

import logging
from typing import List, Optional

from ..live_trader import LiveTrader
from ...clients.mt5.models.response import Position
from ...strategy_builder.data.dtos import ExitDecision


class ExitManager:
    """
    Handles closing positions based on exit signals.
    Single responsibility: Position exit management.
    """

    def __init__(
        self,
        trader: LiveTrader,
        symbol: str,
        broker_symbol: Optional[str] = None,
        logger: logging.Logger = None
    ):
        self.trader = trader
        self.symbol = symbol  # Display symbol (e.g., XAUUSD)
        self.broker_symbol = broker_symbol or symbol  # Broker API symbol (e.g., XAUUSD.pro)
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
        close_percent = getattr(exit_trade, 'close_percent', 100.0) or 100.0
        is_manual = getattr(exit_trade, 'is_manual', False)

        # Use broker symbol for matching positions (positions use broker symbol)
        # Exit signal comes with display symbol, we match against broker symbol
        broker_symbol = self.broker_symbol

        matched_positions = []

        for position in open_positions:
            # For manual exits, match by symbol and direction only (ignore magic)
            if is_manual:
                if position.symbol == broker_symbol and position.type == exit_type:
                    matched_positions.append(position)
            else:
                # Standard exit: match by symbol, magic, and direction
                if (position.symbol == broker_symbol and
                    position.magic == magic and
                    position.type == exit_type):
                    matched_positions.append(position)

        if not matched_positions:
            self.logger.warning(
                f"No matching positions found for exit: {self.symbol} ({broker_symbol}) "
                f"{exit_trade.direction} (magic={magic}, is_manual={is_manual})"
            )
            return

        # Close matching positions (full or partial)
        for position in matched_positions:
            try:
                if close_percent >= 100.0:
                    # Full close - use position's symbol (broker symbol)
                    self.trader.close_open_position(position.symbol, position.ticket)
                    self.logger.info(
                        f"Closed position: ticket={position.ticket}, symbol={position.symbol}, "
                        f"direction={exit_trade.direction}, volume={position.volume}"
                    )
                else:
                    # Partial close - calculate volume to close
                    close_volume = round(position.volume * (close_percent / 100.0), 2)
                    if close_volume >= 0.01:  # Minimum lot size
                        self.trader.close_open_position(position.symbol, position.ticket, volume=close_volume)
                        self.logger.info(
                            f"Partially closed position: ticket={position.ticket}, symbol={position.symbol}, "
                            f"direction={exit_trade.direction}, closed={close_volume}/{position.volume} "
                            f"({close_percent}%)"
                        )
                    else:
                        self.logger.warning(
                            f"Partial close volume too small ({close_volume}), skipping position {position.ticket}"
                        )
            except Exception as e:
                self.logger.error(
                    f"Failed to close position {position.ticket}: {e}"
                )