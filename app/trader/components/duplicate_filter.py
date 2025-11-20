"""
Duplicate Filter - Prevents duplicate trade entries.
"""

import logging
from typing import List, Set, Tuple

from ...clients.mt5.models.response import Position, Order
from ...strategy_builder.data.dtos import EntryDecision


class DuplicateFilter:
    """
    Filters duplicate entry signals to prevent redundant trades.
    Single responsibility: Duplicate detection and filtering.
    """
    
    def __init__(self, logger: logging.Logger = None):
        self.logger = logger or logging.getLogger(self.__class__.__name__)
    
    def filter_entries(
        self, 
        entries: List[EntryDecision], 
        open_positions: List[Position], 
        pending_orders: List[Order]
    ) -> List[EntryDecision]:
        """
        Filter out entry signals that would create duplicate trades.
        
        Args:
            entries: List of entry decisions to filter
            open_positions: Currently open positions
            pending_orders: Currently pending orders
            
        Returns:
            Filtered list of entry decisions without duplicates
        """
        if not entries:
            return entries
        
        self.logger.info(f"=== DUPLICATE FILTER START ===")
        self.logger.info(f"Checking {len(entries)} entries for duplicates")
        self.logger.info(f"Current open positions: {len(open_positions)}")
        self.logger.info(f"Current pending orders: {len(pending_orders)}")
        
        existing_trades = self._get_existing_trades(open_positions, pending_orders)
        
        # Convert to direction-based sets for filtering
        existing_positions_directions = {
            (pos.magic, self._get_direction_from_type(pos.type)) 
            for pos in open_positions
        }
        existing_pending_directions = {
            (order.magic, self._get_direction_from_type(order.type)) 
            for order in pending_orders
        }
        
        filtered_entries = self._filter_against_existing(
            entries, existing_trades, existing_positions_directions, existing_pending_directions
        )
        
        initial_count = len(entries)
        filtered_count = len(filtered_entries)
        
        self.logger.info(
            f"=== FILTER RESULT: {filtered_count}/{initial_count} entries passed "
            f"({initial_count - filtered_count} duplicates removed) ==="
        )
        
        return filtered_entries
    
    def _get_existing_trades(
        self, 
        open_positions: List[Position], 
        pending_orders: List[Order]
    ) -> Set[Tuple[int, str]]:
        """Get set of existing trades as (magic, direction) tuples."""
        # Debug: Show raw order types before conversion
        if pending_orders:
            sample_orders = pending_orders[:3]  # Show first 3 for debugging
            for i, order in enumerate(sample_orders):
                self.logger.info(f"DEBUG Order {i+1}: type={repr(order.type)} (type: {type(order.type).__name__})")
        
        existing_positions = {
            (pos.magic, self._get_direction_from_type(pos.type)) 
            for pos in open_positions
        }
        
        existing_pending = {
            (order.magic, self._get_direction_from_type(order.type)) 
            for order in pending_orders
        }
        
        # Debug: Show existing trades
        if existing_positions:
            self.logger.info(f"Existing positions (magic, direction): {existing_positions}")
        if existing_pending:
            self.logger.info(f"Existing pending (magic, direction): {existing_pending}")
        
        return existing_positions | existing_pending
    
    def _filter_against_existing(
        self, 
        entries: List[EntryDecision], 
        existing_trades: Set[Tuple[int, str]],
        existing_positions: Set[Tuple[int, str]],
        existing_pending: Set[Tuple[int, str]]
    ) -> List[EntryDecision]:
        """Filter entries against existing trades."""
        filtered_entries = []
        
        for entry in entries:
            entry_direction = self._get_direction_from_signal(entry.entry_signals)
            
            if entry_direction is None:
                self.logger.error(f"Unknown entry signal direction: {entry.entry_signals}")
                continue
                
            trade_key = (entry.magic, entry_direction)
            
            self.logger.info(f"Checking entry: magic={entry.magic}, direction={entry_direction}, signal={entry.entry_signals}")
            
            if trade_key not in existing_trades:
                filtered_entries.append(entry)
                self.logger.info(
                    f" Entry ALLOWED: {entry.strategy_name} {entry.direction} "
                    f"(magic={entry.magic}, direction={entry_direction})"
                )
            else:
                source = "positions" if trade_key in existing_positions else "pending orders"
                self.logger.warning(
                    f" Entry BLOCKED (duplicate): {entry.strategy_name} {entry.direction} "
                    f"(magic={entry.magic}, direction={entry_direction}) - Already exists in {source}"
                )
        
        return filtered_entries
    
    def _get_order_type(self, entry_signal: str) -> int:
        """Convert entry signal to MT5 order type."""
        type_map = {
            'BUY': 0,
            'SELL': 1,
            'BUY_LIMIT': 2,
            'SELL_LIMIT': 3
        }
        return type_map.get(entry_signal, -1)
    
    def _get_direction_from_type(self, order_type) -> str:
        """Convert MT5 order type to trading direction (long/short)."""
        # Handle both integer and string types
        if isinstance(order_type, str):
            # String types from API response validator
            if order_type in ['BUY', 'BUY_LIMIT', 'BUY_STOP', 'BUY_STOP_LIMIT']:
                return "long"
            elif order_type in ['SELL', 'SELL_LIMIT', 'SELL_STOP', 'SELL_STOP_LIMIT']:
                return "short"
            else:
                return "unknown"
        elif isinstance(order_type, int):
            # Integer types: 0=BUY, 1=SELL, 2=BUY_LIMIT, 3=SELL_LIMIT, etc.
            if order_type in [0, 2, 4, 6]:  # BUY, BUY_LIMIT, BUY_STOP, BUY_STOP_LIMIT
                return "long"
            elif order_type in [1, 3, 5, 7]:  # SELL, SELL_LIMIT, SELL_STOP, SELL_STOP_LIMIT
                return "short"
            else:
                return "unknown"
        else:
            return "unknown"
    
    def _get_direction_from_signal(self, entry_signal: str) -> str:
        """Convert entry signal to trading direction (long/short)."""
        signal_upper = entry_signal.upper()
        if signal_upper in ['BUY', 'BUY_LIMIT', 'BUY_STOP', 'BUY_STOP_LIMIT']:
            return "long"
        elif signal_upper in ['SELL', 'SELL_LIMIT', 'SELL_STOP', 'SELL_STOP_LIMIT']:
            return "short"
        else:
            return None