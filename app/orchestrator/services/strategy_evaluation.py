"""
Strategy Evaluation Service - Evaluates strategies and generates signals.
"""

import logging
from typing import Optional, Dict, Any

from app.orchestrator.services.base import TradingService, ServiceState
from app.orchestrator.services.event_bus import LocalEventBus
from app.orchestrator.events import (
    OrchestratorEvent,
    OrchestratorEventType,
    IndicatorsUpdatedEvent,
    SignalGeneratedEvent
)


class StrategyEvaluationService(TradingService):
    """
    Service responsible for evaluating strategies and generating signals.

    Responsibilities:
    - Subscribe to INDICATORS_UPDATED events
    - Evaluate strategies via StrategyEngine
    - Validate signals via EntryManager
    - Publish SIGNAL_GENERATED events for valid signals

    Events Subscribed:
    - INDICATORS_UPDATED: Triggered when indicators are calculated

    Events Published:
    - SIGNAL_GENERATED: When a valid trading signal is generated
    - SIGNAL_REJECTED: When a signal is rejected by entry manager
    - STRATEGY_EVALUATION_FAILED: When evaluation fails
    """

    def __init__(
        self,
        symbol: str,
        strategy_engine: Any,
        entry_manager: Any,
        indicator_processor: Any,
        historicals: Dict[str, Any],
        event_bus: LocalEventBus,
        config: Dict[str, Any],
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize strategy evaluation service.

        Args:
            symbol: Trading symbol
            strategy_engine: StrategyEngine instance
            entry_manager: EntryManager instance
            indicator_processor: IndicatorProcessor instance
            historicals: Historical data for all timeframes
            event_bus: Local event bus
            config: Service configuration
            logger: Optional logger
        """
        super().__init__(symbol, event_bus, logger)

        self.strategy_engine = strategy_engine
        self.entry_manager = entry_manager
        self.indicator_processor = indicator_processor
        self.historicals = historicals
        self.config = config

        # Configuration
        self.evaluation_mode = config.get('evaluation_mode', 'on_new_candle')
        self.min_rows_required = config.get('min_rows_required', 3)

    def start(self) -> None:
        """Start the strategy evaluation service."""
        self.logger.info(f"Starting StrategyEvaluationService for {self.symbol}...")
        self._set_state(ServiceState.STARTING)

        try:
            # Subscribe to INDICATORS_UPDATED events
            self.event_bus.subscribe(
                OrchestratorEventType.INDICATORS_UPDATED,
                self._handle_indicators_updated,
                service_name=self.service_name
            )

            self._set_state(ServiceState.RUNNING)
            self.logger.info(f"StrategyEvaluationService started for {self.symbol}")

            # Publish service started event
            self.event_bus.publish(
                self.event_bus.create_event(
                    event_type=OrchestratorEventType.SERVICE_STARTED,
                    data={'service': self.service_name},
                    source_service=self.service_name
                )
            )

        except Exception as e:
            self.handle_error(e)
            raise

    def stop(self) -> None:
        """Stop the strategy evaluation service."""
        self.logger.info(f"Stopping StrategyEvaluationService for {self.symbol}...")
        self._set_state(ServiceState.STOPPING)

        # Unsubscribe from events
        self.event_bus.unsubscribe(
            OrchestratorEventType.INDICATORS_UPDATED,
            self._handle_indicators_updated
        )

        self._set_state(ServiceState.STOPPED)
        self.logger.info(f"StrategyEvaluationService stopped for {self.symbol}")

        # Publish service stopped event
        self.event_bus.publish(
            self.event_bus.create_event(
                event_type=OrchestratorEventType.SERVICE_STOPPED,
                data={'service': self.service_name},
                source_service=self.service_name
            )
        )

    def _handle_indicators_updated(self, event: IndicatorsUpdatedEvent) -> None:
        """
        Handle INDICATORS_UPDATED event.

        Args:
            event: IndicatorsUpdatedEvent containing updated indicators
        """
        if not self.is_running:
            return

        try:
            timeframe = event.data['timeframe']
            indicators = event.data['indicators']
            regime_data = event.data.get('regime')

            self.logger.debug(
                f"Evaluating strategies for {self.symbol} {timeframe}"
            )

            # Check if we have enough data
            if not self._has_sufficient_data(timeframe):
                self.logger.debug(
                    f"Insufficient data for {self.symbol} {timeframe}, "
                    f"need at least {self.min_rows_required} rows"
                )
                return

            # Evaluate strategies
            signals = self._evaluate_strategies(timeframe, indicators, regime_data)

            # Process each signal
            for signal in signals:
                self._process_signal(signal, event.correlation_id)

        except Exception as e:
            self.logger.error(
                f"Error evaluating strategies for {self.symbol}: {e}",
                exc_info=True
            )
            self._publish_evaluation_failed_event(event, str(e))

    def _has_sufficient_data(self, timeframe: str) -> bool:
        """
        Check if we have enough historical data to evaluate strategies.

        Args:
            timeframe: Timeframe to check

        Returns:
            True if sufficient data available
        """
        if timeframe not in self.historicals:
            return False

        historical = self.historicals[timeframe]
        if historical is None:
            return False

        return len(historical) >= self.min_rows_required

    def _evaluate_strategies(
        self,
        timeframe: str,
        indicators: Dict[str, Any],
        regime_data: Optional[Dict[str, Any]]
    ) -> list:
        """
        Evaluate all strategies for the given timeframe.

        Args:
            timeframe: Timeframe to evaluate
            indicators: Calculated indicators
            regime_data: Optional regime information

        Returns:
            List of generated signals
        """
        signals = []

        try:
            # Get historical data for this timeframe
            historical = self.historicals.get(timeframe)
            if historical is None or len(historical) < self.min_rows_required:
                return signals

            # Get recent rows from indicator processor (required by StrategyEngine)
            # The IndicatorProcessor.get_recent_rows() returns Dict[str, deque[pd.Series]]
            recent_rows = self.indicator_processor.get_recent_rows()

            if not recent_rows:
                self.logger.warning("No recent rows available from indicator processor")
                return signals

            # Evaluate all strategies using the strategy engine
            evaluation_result = self.strategy_engine.evaluate(recent_rows)

            # Convert evaluation results to signals format
            for strategy_name, result in evaluation_result.strategies.items():
                # Serialize evaluation result
                # Try model_dump() first, then fall back to __dict__
                if hasattr(result, 'model_dump'):
                    eval_dict = result.model_dump()
                elif hasattr(result, '__dict__'):
                    # Convert object attributes to dict recursively
                    def obj_to_dict(obj):
                        if hasattr(obj, 'model_dump'):
                            return obj.model_dump()
                        elif hasattr(obj, '__dict__'):
                            return {k: obj_to_dict(v) for k, v in obj.__dict__.items()}
                        else:
                            return obj
                    eval_dict = obj_to_dict(result)
                else:
                    self.logger.warning(f"Cannot serialize result for {strategy_name}, using empty dict")
                    eval_dict = {}

                # Check for entry signals
                if result.entry.long:
                    signals.append({
                        'strategy_name': strategy_name,
                        'type': 'BUY',
                        'timeframe': timeframe,
                        'confidence': 1.0,  # Default confidence
                        'entry_price': 0.0,  # Will be filled by execution service
                        'metadata': {
                            'evaluation_result': eval_dict,
                            'market_data': recent_rows  # Pass market data for risk calculations
                        }
                    })

                if result.entry.short:
                    signals.append({
                        'strategy_name': strategy_name,
                        'type': 'SELL',
                        'timeframe': timeframe,
                        'confidence': 1.0,
                        'entry_price': 0.0,
                        'metadata': {
                            'evaluation_result': eval_dict,
                            'market_data': recent_rows  # Pass market data for risk calculations
                        }
                    })

        except Exception as e:
            self.logger.error(f"Error in strategy evaluation: {e}", exc_info=True)

        return signals

    def _process_signal(self, signal: Dict[str, Any], correlation_id: Optional[str]) -> None:
        """
        Process and validate a signal.

        Args:
            signal: Signal data from strategy
            correlation_id: Correlation ID from parent event
        """
        try:
            # Validate signal with entry manager
            is_valid, rejection_reason = self._validate_signal(signal)

            if is_valid:
                # Publish SIGNAL_GENERATED event
                signal_event = SignalGeneratedEvent(
                    symbol=self.symbol,
                    strategy_name=signal['strategy_name'],
                    signal_type=signal.get('type', 'BUY'),
                    confidence=signal.get('confidence', 1.0),
                    entry_price=signal.get('entry_price', 0.0),
                    stop_loss=signal.get('stop_loss'),
                    take_profit=signal.get('take_profit'),
                    metadata=signal.get('metadata', {}),
                    correlation_id=correlation_id
                )
                self.event_bus.publish(signal_event)

                self.logger.info(
                    f"Published SIGNAL_GENERATED event for {self.symbol}: "
                    f"{signal['strategy_name']} {signal.get('type')}"
                )

            else:
                # Publish SIGNAL_REJECTED event
                self.event_bus.publish(
                    self.event_bus.create_event(
                        event_type=OrchestratorEventType.SIGNAL_REJECTED,
                        data={
                            'signal': signal,
                            'rejection_reason': rejection_reason
                        },
                        source_service=self.service_name,
                        correlation_id=correlation_id
                    )
                )

                self.logger.debug(
                    f"Signal rejected for {self.symbol}: {rejection_reason}"
                )

        except Exception as e:
            self.logger.error(f"Error processing signal: {e}")

    def _validate_signal(self, signal: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Validate signal using entry manager.

        Args:
            signal: Signal to validate

        Returns:
            Tuple of (is_valid, rejection_reason)
        """
        try:
            # For now, always allow signals to pass through
            # The EntryManager will do full validation when the trade is executed
            # TODO: Implement proper pre-validation if needed
            return True, None

        except Exception as e:
            self.logger.error(f"Error validating signal: {e}")
            return False, f"Validation error: {str(e)}"

    def _publish_evaluation_failed_event(
        self,
        original_event: OrchestratorEvent,
        error_message: str
    ) -> None:
        """Publish strategy evaluation failed event."""
        self.event_bus.publish(
            self.event_bus.create_event(
                event_type=OrchestratorEventType.STRATEGY_EVALUATION_FAILED,
                data={
                    'original_event': original_event.to_dict(),
                    'error': error_message
                },
                source_service=self.service_name,
                correlation_id=original_event.correlation_id
            )
        )

    def get_status(self) -> Dict[str, Any]:
        """Get service status with strategy-specific information."""
        status = super().get_status()

        available_strategies = []
        try:
            available_strategies = self.strategy_engine.list_available_strategies()
        except:
            pass

        status.update({
            'evaluation_mode': self.evaluation_mode,
            'min_rows_required': self.min_rows_required,
            'available_strategies': available_strategies
        })
        return status
