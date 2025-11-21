"""
Event type definitions for the trading system.
"""

from enum import Enum
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
import json


class TradeEventType(str, Enum):
    """Types of trade events that can be published."""

    # Order events
    ORDER_CREATED = "order.created"
    ORDER_FILLED = "order.filled"
    ORDER_PARTIALLY_FILLED = "order.partially_filled"
    ORDER_CANCELLED = "order.cancelled"
    ORDER_REJECTED = "order.rejected"
    ORDER_MODIFIED = "order.modified"

    # Position events
    POSITION_OPENED = "position.opened"
    POSITION_CLOSED = "position.closed"
    POSITION_MODIFIED = "position.modified"
    POSITION_PARTIALLY_CLOSED = "position.partially_closed"

    # Trade execution events
    TRADE_EXECUTED = "trade.executed"
    TRADE_FAILED = "trade.failed"

    # Risk events
    RISK_LIMIT_BREACHED = "risk.limit_breached"
    STOP_LOSS_HIT = "risk.stop_loss_hit"
    TAKE_PROFIT_HIT = "risk.take_profit_hit"

    # Strategy events
    SIGNAL_GENERATED = "strategy.signal_generated"
    SIGNAL_VALIDATED = "strategy.signal_validated"
    SIGNAL_REJECTED = "strategy.signal_rejected"


@dataclass
class TradeEvent:
    """
    Base trade event structure.

    Attributes:
        event_type: Type of the event
        symbol: Trading symbol (e.g., "XAUUSD")
        timestamp: Event timestamp (ISO format)
        data: Event-specific data
        correlation_id: Optional correlation ID for tracking related events
        source: Source service/component that generated the event
        app_tag: Application/broker tag for multi-instance identification
    """
    event_type: TradeEventType
    symbol: str
    timestamp: str
    data: Dict[str, Any]
    correlation_id: Optional[str] = None
    source: Optional[str] = None
    app_tag: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary."""
        return {
            'event_type': self.event_type.value,
            'symbol': self.symbol,
            'timestamp': self.timestamp,
            'data': self.data,
            'correlation_id': self.correlation_id,
            'source': self.source,
            'app_tag': self.app_tag
        }

    def to_json(self) -> str:
        """Convert event to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TradeEvent':
        """Create event from dictionary."""
        return cls(
            event_type=TradeEventType(data['event_type']),
            symbol=data['symbol'],
            timestamp=data['timestamp'],
            data=data['data'],
            correlation_id=data.get('correlation_id'),
            source=data.get('source')
        )

    @classmethod
    def from_json(cls, json_str: str) -> 'TradeEvent':
        """Create event from JSON string."""
        return cls.from_dict(json.loads(json_str))

    @staticmethod
    def create_order_created(
        symbol: str,
        order_id: str,
        order_type: str,
        price: float,
        volume: float,
        **kwargs
    ) -> 'TradeEvent':
        """Helper to create ORDER_CREATED event."""
        return TradeEvent(
            event_type=TradeEventType.ORDER_CREATED,
            symbol=symbol,
            timestamp=datetime.utcnow().isoformat(),
            data={
                'order_id': order_id,
                'order_type': order_type,
                'price': price,
                'volume': volume,
                **kwargs
            }
        )

    @staticmethod
    def create_position_opened(
        symbol: str,
        position_id: str,
        entry_price: float,
        volume: float,
        direction: str,
        **kwargs
    ) -> 'TradeEvent':
        """Helper to create POSITION_OPENED event."""
        return TradeEvent(
            event_type=TradeEventType.POSITION_OPENED,
            symbol=symbol,
            timestamp=datetime.utcnow().isoformat(),
            data={
                'position_id': position_id,
                'entry_price': entry_price,
                'volume': volume,
                'direction': direction,
                **kwargs
            }
        )

    @staticmethod
    def create_position_closed(
        symbol: str,
        position_id: str,
        exit_price: float,
        pnl: float,
        **kwargs
    ) -> 'TradeEvent':
        """Helper to create POSITION_CLOSED event."""
        return TradeEvent(
            event_type=TradeEventType.POSITION_CLOSED,
            symbol=symbol,
            timestamp=datetime.utcnow().isoformat(),
            data={
                'position_id': position_id,
                'exit_price': exit_price,
                'pnl': pnl,
                **kwargs
            }
        )

    @staticmethod
    def create_signal_generated(
        symbol: str,
        strategy_name: str,
        signal_type: str,
        confidence: float,
        **kwargs
    ) -> 'TradeEvent':
        """Helper to create SIGNAL_GENERATED event."""
        return TradeEvent(
            event_type=TradeEventType.SIGNAL_GENERATED,
            symbol=symbol,
            timestamp=datetime.utcnow().isoformat(),
            data={
                'strategy_name': strategy_name,
                'signal_type': signal_type,
                'confidence': confidence,
                **kwargs
            }
        )
