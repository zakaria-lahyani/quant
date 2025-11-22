"""
Event management infrastructure for the trading system.

This module provides event bus implementations for managing trading events,
including Redis-based distributed event handling and manual signal processing.
"""

from .redis_event_bus import RedisEventBus
from .event_types import TradeEvent, TradeEventType
from .event_bus_factory import EventBusFactory
from .indicator_redis_store import IndicatorRedisStore
from .manual_signal_store import ManualSignalStore, ManualSignal
from .runtime_control_store import RuntimeControlStore

__all__ = [
    'RedisEventBus',
    'TradeEvent',
    'TradeEventType',
    'EventBusFactory',
    'IndicatorRedisStore',
    'ManualSignalStore',
    'ManualSignal',
    'RuntimeControlStore',
]
