"""
Event definitions for orchestrator service coordination.

These events are used within the symbol orchestrator to coordinate
between services (DataFetching, IndicatorCalculation, Strategy, Execution).
"""

from enum import Enum
from typing import Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import json


class OrchestratorEventType(str, Enum):
    """Event types for orchestrator service coordination."""

    # Data events
    NEW_CANDLE = "data.new_candle"
    DATA_FETCH_FAILED = "data.fetch_failed"

    # Indicator events
    INDICATORS_UPDATED = "indicators.updated"
    REGIME_CHANGED = "indicators.regime_changed"
    INDICATOR_CALCULATION_FAILED = "indicators.calculation_failed"

    # Strategy events
    SIGNAL_GENERATED = "strategy.signal_generated"
    SIGNAL_VALIDATED = "strategy.signal_validated"
    SIGNAL_REJECTED = "strategy.signal_rejected"
    STRATEGY_EVALUATION_FAILED = "strategy.evaluation_failed"

    # Execution events
    TRADE_EXECUTED = "execution.trade_executed"
    TRADE_FAILED = "execution.trade_failed"
    POSITION_UPDATED = "execution.position_updated"

    # Service lifecycle events
    SERVICE_STARTED = "service.started"
    SERVICE_STOPPED = "service.stopped"
    SERVICE_ERROR = "service.error"
    SERVICE_RECOVERING = "service.recovering"

    # Orchestrator events
    ORCHESTRATOR_STARTED = "orchestrator.started"
    ORCHESTRATOR_STOPPED = "orchestrator.stopped"
    ORCHESTRATOR_PAUSED = "orchestrator.paused"
    ORCHESTRATOR_RESUMED = "orchestrator.resumed"


@dataclass
class OrchestratorEvent:
    """
    Base event for orchestrator service coordination.

    Attributes:
        event_type: Type of the event
        symbol: Trading symbol (e.g., "XAUUSD")
        timestamp: Event timestamp (ISO format)
        data: Event-specific data
        correlation_id: Optional correlation ID for tracking related events
        source_service: Source service that generated the event
    """
    event_type: OrchestratorEventType
    symbol: str
    timestamp: str
    data: Dict[str, Any]
    correlation_id: Optional[str] = None
    source_service: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary."""
        return {
            'event_type': self.event_type.value,
            'symbol': self.symbol,
            'timestamp': self.timestamp,
            'data': self.data,
            'correlation_id': self.correlation_id,
            'source_service': self.source_service
        }

    def to_json(self) -> str:
        """Convert event to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OrchestratorEvent':
        """Create event from dictionary."""
        return cls(
            event_type=OrchestratorEventType(data['event_type']),
            symbol=data['symbol'],
            timestamp=data['timestamp'],
            data=data['data'],
            correlation_id=data.get('correlation_id'),
            source_service=data.get('source_service')
        )


@dataclass
class NewCandleEvent(OrchestratorEvent):
    """
    Event published when new candle data is available.

    Published by: DataFetchingService
    Consumed by: IndicatorCalculationService
    """

    def __init__(
        self,
        symbol: str,
        timeframe: str,
        candle_data: Dict[str, Any],
        correlation_id: Optional[str] = None
    ):
        super().__init__(
            event_type=OrchestratorEventType.NEW_CANDLE,
            symbol=symbol,
            timestamp=datetime.utcnow().isoformat(),
            data={
                'timeframe': timeframe,
                'candle': candle_data
            },
            correlation_id=correlation_id,
            source_service='DataFetchingService'
        )


@dataclass
class IndicatorsUpdatedEvent(OrchestratorEvent):
    """
    Event published when indicators are calculated/updated.

    Published by: IndicatorCalculationService
    Consumed by: StrategyEvaluationService
    """

    def __init__(
        self,
        symbol: str,
        timeframe: str,
        indicators: Dict[str, Any],
        regime_data: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None
    ):
        super().__init__(
            event_type=OrchestratorEventType.INDICATORS_UPDATED,
            symbol=symbol,
            timestamp=datetime.utcnow().isoformat(),
            data={
                'timeframe': timeframe,
                'indicators': indicators,
                'regime': regime_data
            },
            correlation_id=correlation_id,
            source_service='IndicatorCalculationService'
        )


@dataclass
class SignalGeneratedEvent(OrchestratorEvent):
    """
    Event published when a trading signal is generated.

    Published by: StrategyEvaluationService
    Consumed by: TradeExecutionService
    """

    def __init__(
        self,
        symbol: str,
        strategy_name: str,
        signal_type: str,
        confidence: float,
        entry_price: float,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None
    ):
        super().__init__(
            event_type=OrchestratorEventType.SIGNAL_GENERATED,
            symbol=symbol,
            timestamp=datetime.utcnow().isoformat(),
            data={
                'strategy_name': strategy_name,
                'signal_type': signal_type,
                'confidence': confidence,
                'entry_price': entry_price,
                'stop_loss': stop_loss,
                'take_profit': take_profit,
                'metadata': metadata or {}
            },
            correlation_id=correlation_id,
            source_service='StrategyEvaluationService'
        )


@dataclass
class TradeExecutedEvent(OrchestratorEvent):
    """
    Event published when a trade is executed.

    Published by: TradeExecutionService
    Consumed by: Monitoring, Logging, Risk Management
    """

    def __init__(
        self,
        symbol: str,
        trade_id: str,
        order_type: str,
        entry_price: float,
        volume: float,
        status: str,
        error_message: Optional[str] = None,
        correlation_id: Optional[str] = None
    ):
        super().__init__(
            event_type=OrchestratorEventType.TRADE_EXECUTED,
            symbol=symbol,
            timestamp=datetime.utcnow().isoformat(),
            data={
                'trade_id': trade_id,
                'order_type': order_type,
                'entry_price': entry_price,
                'volume': volume,
                'status': status,
                'error_message': error_message
            },
            correlation_id=correlation_id,
            source_service='TradeExecutionService'
        )


@dataclass
class RegimeChangedEvent(OrchestratorEvent):
    """
    Event published when market regime changes.

    Published by: IndicatorCalculationService
    Consumed by: StrategyEvaluationService, Risk Management
    """

    def __init__(
        self,
        symbol: str,
        timeframe: str,
        old_regime: str,
        new_regime: str,
        regime_data: Dict[str, Any],
        correlation_id: Optional[str] = None
    ):
        super().__init__(
            event_type=OrchestratorEventType.REGIME_CHANGED,
            symbol=symbol,
            timestamp=datetime.utcnow().isoformat(),
            data={
                'timeframe': timeframe,
                'old_regime': old_regime,
                'new_regime': new_regime,
                'regime_data': regime_data
            },
            correlation_id=correlation_id,
            source_service='IndicatorCalculationService'
        )
