"""
Trade Execution Service - Executes trades and publishes events.
"""

import logging
from typing import Optional, Dict, Any

from app.orchestrator.services.base import TradingService, ServiceState
from app.orchestrator.services.event_bus import LocalEventBus
from app.orchestrator.events import (
    OrchestratorEvent,
    OrchestratorEventType,
    SignalGeneratedEvent,
    TradeExecutedEvent
)


class TradeExecutionService(TradingService):
    """
    Service responsible for executing trades based on generated signals.

    Responsibilities:
    - Subscribe to SIGNAL_GENERATED events
    - Validate signals (restrictions, risk limits)
    - Execute trades via TradeExecutor
    - Publish TRADE_EXECUTED events to both local and Redis event buses

    Events Subscribed:
    - SIGNAL_GENERATED: Triggered when a trading signal is generated

    Events Published:
    - TRADE_EXECUTED: When trade is executed (to local bus)
    - TRADE_FAILED: When trade execution fails (to local bus)
    - Plus external events via Redis (ORDER_CREATED, POSITION_OPENED, etc.)
    """

    def __init__(
        self,
        symbol: str,
        trade_executor: Any,
        entry_manager: Any,
        client: Any,
        date_helper: Any,
        redis_event_bus: Optional[Any],
        event_bus: LocalEventBus,
        config: Dict[str, Any],
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize trade execution service.

        Args:
            symbol: Trading symbol
            trade_executor: TradeExecutor instance
            entry_manager: EntryManager instance for risk management
            client: MT5 client for account data
            date_helper: DateHelper for time calculations
            redis_event_bus: Optional Redis event bus for external events
            event_bus: Local event bus for service coordination
            config: Service configuration
            logger: Optional logger
        """
        super().__init__(symbol, event_bus, logger)

        self.trade_executor = trade_executor
        self.entry_manager = entry_manager
        self.client = client
        self.date_helper = date_helper
        self.redis_event_bus = redis_event_bus
        self.config = config

        # Configuration
        self.execution_mode = config.get('execution_mode', 'immediate')
        self.batch_size = config.get('batch_size', 1)

        # Statistics
        self._trades_executed = 0
        self._trades_failed = 0
        self._orders_placed = 0
        self._orders_rejected = 0

    def start(self) -> None:
        """Start the trade execution service."""
        self.logger.info(f"Starting TradeExecutionService for {self.symbol}...")
        self._set_state(ServiceState.STARTING)

        try:
            # Inject Redis event bus into trade executor if available
            self._inject_redis_event_bus()

            # Subscribe to SIGNAL_GENERATED events
            self.event_bus.subscribe(
                OrchestratorEventType.SIGNAL_GENERATED,
                self._handle_signal_generated,
                service_name=self.service_name
            )

            self._set_state(ServiceState.RUNNING)
            self.logger.info(f"TradeExecutionService started for {self.symbol}")

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
        """Stop the trade execution service."""
        self.logger.info(f"Stopping TradeExecutionService for {self.symbol}...")
        self._set_state(ServiceState.STOPPING)

        # Unsubscribe from events
        self.event_bus.unsubscribe(
            OrchestratorEventType.SIGNAL_GENERATED,
            self._handle_signal_generated
        )

        self._set_state(ServiceState.STOPPED)
        self.logger.info(f"TradeExecutionService stopped for {self.symbol}")

        # Publish service stopped event
        self.event_bus.publish(
            self.event_bus.create_event(
                event_type=OrchestratorEventType.SERVICE_STOPPED,
                data={'service': self.service_name},
                source_service=self.service_name
            )
        )

    def _inject_redis_event_bus(self) -> None:
        """Inject Redis event bus into trade executor's OrderExecutor."""
        if self.redis_event_bus and hasattr(self.trade_executor, 'order_executor'):
            self.trade_executor.order_executor.event_bus = self.redis_event_bus
            self.logger.info(
                f"Injected Redis event bus into TradeExecutor for {self.symbol}"
            )

    def _handle_signal_generated(self, event: SignalGeneratedEvent) -> None:
        """
        Handle SIGNAL_GENERATED event.

        Args:
            event: SignalGeneratedEvent containing trading signal
        """
        if not self.is_running:
            return

        try:
            signal_data = event.data
            strategy_name = signal_data['strategy_name']
            signal_type = signal_data['signal_type']
            entry_price = signal_data['entry_price']
            stop_loss = signal_data.get('stop_loss')
            take_profit = signal_data.get('take_profit')
            confidence = signal_data.get('confidence', 1.0)

            self.logger.info(
                f"Processing signal for {self.symbol}: {strategy_name} {signal_type} "
                f"@ {entry_price} (confidence: {confidence:.2f})"
            )

            # Execute trade
            result = self._execute_trade(
                strategy_name=strategy_name,
                signal_type=signal_type,
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                metadata=signal_data.get('metadata', {})
            )

            # Publish trade result
            if result['success']:
                self._publish_trade_executed_event(result, event.correlation_id)
                self._trades_executed += 1
            else:
                self._publish_trade_failed_event(result, event.correlation_id)
                self._trades_failed += 1

        except Exception as e:
            self.logger.error(
                f"Error executing trade for {self.symbol}: {e}",
                exc_info=True
            )
            self._publish_trade_failed_event(
                {'error': str(e), 'signal': event.data},
                event.correlation_id
            )
            self._trades_failed += 1

    def _execute_trade(
        self,
        strategy_name: str,
        signal_type: str,
        entry_price: float,
        stop_loss: Optional[float],
        take_profit: Optional[float],
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute a trade via EntryManager and TradeExecutor.

        Args:
            strategy_name: Name of strategy generating signal
            signal_type: BUY or SELL
            entry_price: Entry price (will be recalculated by EntryManager)
            stop_loss: Optional stop loss (will be recalculated)
            take_profit: Optional take profit (will be recalculated)
            metadata: Additional signal metadata with evaluation_result

        Returns:
            Trade execution result dictionary
        """
        try:
            self.logger.info(
                f"Executing trade for {self.symbol}: {signal_type} "
                f"strategy={strategy_name}"
            )

            # Get evaluation result from metadata
            evaluation_result = metadata.get('evaluation_result', {})
            if not evaluation_result:
                self.logger.error(f"No evaluation result in metadata. Metadata keys: {list(metadata.keys())}")
                return {
                    'success': False,
                    'error': 'No evaluation result in metadata',
                    'strategy': strategy_name
                }

            # Get account balance for position sizing
            try:
                account_balance = self.client.account.get_balance()
            except Exception as e:
                self.logger.warning(f"Could not fetch account balance: {e}. Using None.")
                account_balance = None

            # Use EntryManager to calculate proper entry decision with risk management
            # Convert dict to object with attributes (EntryManager expects objects with .entry and .exit)
            def dict_to_obj(d):
                """Recursively convert dict to object with attributes."""
                if isinstance(d, dict):
                    return type('EvalResult', (), {k: dict_to_obj(v) for k, v in d.items()})()
                elif isinstance(d, list):
                    return [dict_to_obj(item) for item in d]
                else:
                    return d

            strategy_results = {
                strategy_name: dict_to_obj(evaluation_result)
            }

            # Get market data from indicator processor (passed via metadata or fetch fresh)
            market_data = metadata.get('market_data')
            if not market_data:
                # Market data not available, cannot proceed
                self.logger.warning("Market data not available for risk calculations")
                return {
                    'success': False,
                    'error': 'Market data required for risk calculations',
                    'strategy': strategy_name
                }

            # Calculate entry/exit decisions with full risk management
            trades = self.entry_manager.manage_trades(
                strategy_results=strategy_results,
                market_data=market_data,
                account_balance=account_balance
            )

            if not trades.entries and not trades.exits:
                self.logger.info(f"No valid trades generated for {strategy_name}")
                return {
                    'success': True,
                    'message': 'No valid trades after risk management',
                    'strategy': strategy_name
                }

            # Execute trades via TradeExecutor
            context = self.trade_executor.execute_trading_cycle(
                trades=trades,
                date_helper=self.date_helper
            )

            # Collect results
            result = {
                'success': True,
                'strategy': strategy_name,
                'entries_count': len(trades.entries),
                'exits_count': len(trades.exits),
                'trade_authorized': context.trade_authorized,
                'risk_breached': context.risk_breached,
                'daily_pnl': context.daily_pnl,
                'floating_pnl': context.floating_pnl,
                'message': f'Executed {len(trades.entries)} entries, {len(trades.exits)} exits'
            }

            # Update statistics
            if trades.entries:
                self._orders_placed += len(trades.entries)
            if trades.exits:
                self._orders_placed += len(trades.exits)

            self.logger.info(
                f"✓ Trade execution completed for {self.symbol}: "
                f"entries={len(trades.entries)}, exits={len(trades.exits)}, "
                f"authorized={context.trade_authorized}"
            )

            return result

        except Exception as e:
            self.logger.error(f"Error executing trade: {e}", exc_info=True)
            self._orders_rejected += 1
            return {
                'success': False,
                'error': str(e),
                'strategy': strategy_name
            }

    def _publish_trade_executed_event(
        self,
        result: Dict[str, Any],
        correlation_id: Optional[str]
    ) -> None:
        """
        Publish TRADE_EXECUTED event.

        Args:
            result: Trade execution result
            correlation_id: Correlation ID from signal event
        """
        trade_event = TradeExecutedEvent(
            symbol=self.symbol,
            trade_id=result.get('trade_id', 'unknown'),
            order_type=result.get('order_type', 'unknown'),
            entry_price=result.get('entry_price', 0.0),
            volume=result.get('volume', 0.0),
            status='success',
            correlation_id=correlation_id
        )

        # Publish to local event bus
        self.event_bus.publish(trade_event)

        self.logger.info(
            f"Trade executed successfully for {self.symbol}: {result.get('trade_id')}"
        )

        # Redis event bus will publish external events via OrderExecutor automatically

    def _publish_trade_failed_event(
        self,
        result: Dict[str, Any],
        correlation_id: Optional[str]
    ) -> None:
        """
        Publish TRADE_FAILED event.

        Args:
            result: Trade execution result with error
            correlation_id: Correlation ID from signal event
        """
        self.event_bus.publish(
            self.event_bus.create_event(
                event_type=OrchestratorEventType.TRADE_FAILED,
                data={
                    'error': result.get('error', 'Unknown error'),
                    'trade_params': result.get('trade_params', {}),
                    'signal': result.get('signal', {})
                },
                source_service=self.service_name,
                correlation_id=correlation_id
            )
        )

        self.logger.warning(
            f"Trade execution failed for {self.symbol}: {result.get('error')}"
        )

    def get_status(self) -> Dict[str, Any]:
        """Get service status with execution-specific information."""
        status = super().get_status()
        status.update({
            'execution_mode': self.execution_mode,
            'trades_executed': self._trades_executed,
            'trades_failed': self._trades_failed,
            'orders_placed': self._orders_placed,
            'orders_rejected': self._orders_rejected,
            'success_rate': (
                self._trades_executed / (self._trades_executed + self._trades_failed)
                if (self._trades_executed + self._trades_failed) > 0
                else 0.0
            ),
            'redis_connected': (
                self.redis_event_bus.is_connected
                if self.redis_event_bus
                else False
            )
        })
        return status
