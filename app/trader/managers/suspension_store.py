"""
Suspension Store - Manages suspended trading items during news and market restrictions.
"""

from typing import List, Literal, TypedDict
import logging


class SuspendedItem(TypedDict):
    """Represents an item suspended during trading restrictions."""
    ticket: int
    kind: Literal["pending_order", "position_sl_tp"]
    original_sl: float | None
    original_tp: float | None
    symbol: str
    # Additional fields for recreating pending orders
    order_type: str | None
    volume: float | None
    price: float | None
    magic: int | None


class SuspensionStore:
    """
    In-memory store for suspended trading items.
    Manages items that are temporarily removed during news events or market restrictions.
    """
    
    def __init__(self, logger: logging.Logger = None):
        self._items: List[SuspendedItem] = []
        self.logger = logger or logging.getLogger(self.__class__.__name__)
    
    def add(self, item: SuspendedItem) -> None:
        """
        Add a suspended item to the store.
        
        Args:
            item: The suspended item to store
        """
        if self.has_ticket(item['ticket']):
            self.logger.warning(f"Ticket {item['ticket']} already exists in suspension store")
            return
        
        self._items.append(item)
        self.logger.info(
            f"Added {item['kind']} ticket {item['ticket']} to suspension store "
            f"(symbol: {item['symbol']})"
        )
    
    def all(self) -> List[SuspendedItem]:
        """
        Get all suspended items.
        
        Returns:
            List of all suspended items
        """
        return self._items.copy()
    
    def clear(self) -> None:
        """Clear all suspended items from the store."""
        count = len(self._items)
        self._items.clear()
        self.logger.info(f"Cleared {count} items from suspension store")
    
    def has_ticket(self, ticket: int) -> bool:
        """
        Check if a ticket exists in the suspension store.
        
        Args:
            ticket: The ticket number to check
            
        Returns:
            True if ticket exists, False otherwise
        """
        return any(item['ticket'] == ticket for item in self._items)
    
    def get_by_kind(self, kind: Literal["pending_order", "position_sl_tp"]) -> List[SuspendedItem]:
        """
        Get all suspended items of a specific kind.
        
        Args:
            kind: The kind of items to retrieve
            
        Returns:
            List of items matching the specified kind
        """
        return [item for item in self._items if item['kind'] == kind]
    
    def get_by_symbol(self, symbol: str) -> List[SuspendedItem]:
        """
        Get all suspended items for a specific symbol.
        
        Args:
            symbol: The symbol to filter by
            
        Returns:
            List of items for the specified symbol
        """
        return [item for item in self._items if item['symbol'] == symbol]
    
    def remove_ticket(self, ticket: int) -> bool:
        """
        Remove a specific ticket from the suspension store.
        
        Args:
            ticket: The ticket number to remove
            
        Returns:
            True if ticket was removed, False if not found
        """
        original_count = len(self._items)
        self._items = [item for item in self._items if item['ticket'] != ticket]
        
        if len(self._items) < original_count:
            self.logger.info(f"Removed ticket {ticket} from suspension store")
            return True
        
        return False
    
    def is_empty(self) -> bool:
        """
        Check if the suspension store is empty.
        
        Returns:
            True if store is empty, False otherwise
        """
        return len(self._items) == 0
    
    def count(self) -> int:
        """
        Get the number of items in the suspension store.
        
        Returns:
            Number of suspended items
        """
        return len(self._items)
    
    def get_summary(self) -> dict:
        """
        Get a summary of suspended items by kind.
        
        Returns:
            Dictionary with counts by kind
        """
        summary = {
            "pending_order": 0,
            "position_sl_tp": 0,
            "total": len(self._items)
        }
        
        for item in self._items:
            summary[item['kind']] += 1
        
        return summary