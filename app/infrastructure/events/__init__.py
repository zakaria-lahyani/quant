"""
Event management infrastructure for the trading system.

This module provides event bus implementations for managing trading events,
including Redis-based distributed event handling.
"""

from .redis_event_bus import RedisEventBus
from .event_types import TradeEvent, TradeEventType
from .event_bus_factory import EventBusFactory

__all__ = [
    'RedisEventBus',
    'TradeEvent',
    'TradeEventType',
    'EventBusFactory',
]
