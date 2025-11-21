"""
Manual Signal Service - Processes external/manual trading signals.

This service polls Redis for manual trading signals from external sources
(3rd party systems, manual trading interfaces) and routes them through
the standard risk management and trade execution pipeline.
"""

import logging
import threading
import time
from typing import Optional, Dict, Any
from datetime import datetime
import uuid

from app.orchestrator.services.base import TradingService, ServiceState
from app.orchestrator.services.event_bus import LocalEventBus
from app.orchestrator.events import (
    OrchestratorEventType,
    SignalGeneratedEvent
)
from app.infrastructure.events.manual_signal_store import (
    ManualSignalStore,
    ManualSignal
)


class ManualSignalService(TradingService):
    """
    Service that processes manual/external trading signals.

    This service:
    1. Polls Redis for manual signals from external sources
    2. Fetches current market data for risk calculations
    3. Creates SignalGeneratedEvent with proper evaluation_result structure
    4. Publishes to LocalEventBus for TradeExecutionService to process
    5. Full risk management (position sizing, SL/TP) happens automatically

    Signal Flow:
        External Source -> Redis Queue -> ManualSignalService
            -> SignalGeneratedEvent -> TradeExecutionService
            -> EntryManager (risk calc) -> TradeExecutor -> Broker

    Events Published:
        - SIGNAL_GENERATED: When manual signal is processed
    """

    def __init__(
        self,
        symbol: str,
        manual_signal_store: ManualSignalStore,
        indicator_processor: Any,
        event_bus: LocalEventBus,
        config: Dict[str, Any],
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize manual signal service.

        Args:
            symbol: Trading symbol this service handles
            manual_signal_store: ManualSignalStore instance for Redis queue
            indicator_processor: IndicatorProcessor for market data access
            event_bus: Local event bus for publishing signals
            config: Service configuration
            logger: Optional logger instance
        """
        super().__init__(symbol, event_bus, logger)

        self.manual_signal_store = manual_signal_store
        self.indicator_processor = indicator_processor
        self.config = config

        # Configuration
        self.poll_interval = config.get('poll_interval', 1.0)  # seconds
        self.enabled = config.get('enabled', True)
        self.primary_timeframe = config.get('primary_timeframe', '15')

        # Polling thread
        self._poll_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # Statistics
        self._signals_processed = 0
        self._signals_failed = 0
        self._last_signal_time: Optional[datetime] = None

    def start(self) -> None:
        """Start the manual signal service."""
        if not self.enabled:
            self.logger.info(f"ManualSignalService disabled for {self.symbol}")
            self._set_state(ServiceState.STOPPED)
            return

        self.logger.info(f"Starting ManualSignalService for {self.symbol}...")
        self._set_state(ServiceState.STARTING)

        try:
            # Clear stop event
            self._stop_event.clear()

            # Start polling thread
            self._poll_thread = threading.Thread(
                target=self._poll_loop,
                name=f"ManualSignalPoller-{self.symbol}",
                daemon=True
            )
            self._poll_thread.start()

            self._set_state(ServiceState.RUNNING)
            self.logger.info(
                f"ManualSignalService started for {self.symbol} "
                f"(poll interval: {self.poll_interval}s)"
            )

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
        """Stop the manual signal service."""
        self.logger.info(f"Stopping ManualSignalService for {self.symbol}...")
        self._set_state(ServiceState.STOPPING)

        # Signal poll thread to stop
        self._stop_event.set()

        # Wait for poll thread to finish
        if self._poll_thread and self._poll_thread.is_alive():
            self._poll_thread.join(timeout=5.0)

        self._set_state(ServiceState.STOPPED)
        self.logger.info(f"ManualSignalService stopped for {self.symbol}")

        # Publish service stopped event
        self.event_bus.publish(
            self.event_bus.create_event(
                event_type=OrchestratorEventType.SERVICE_STOPPED,
                data={'service': self.service_name},
                source_service=self.service_name
            )
        )

    def _poll_loop(self) -> None:
        """Main polling loop for manual signals."""
        self.logger.debug(f"Poll loop started for {self.symbol}")

        while not self._stop_event.is_set():
            try:
                # Check for pending signals
                signal = self.manual_signal_store.pop_signal()

                if signal:
                    self._process_signal(signal)

            except Exception as e:
                self.logger.error(
                    f"Error in poll loop for {self.symbol}: {e}",
                    exc_info=True
                )

            # Wait before next poll
            self._stop_event.wait(self.poll_interval)

        self.logger.debug(f"Poll loop stopped for {self.symbol}")

    def _process_signal(self, signal: ManualSignal) -> None:
        """
        Process a manual signal and publish to event bus.

        Args:
            signal: ManualSignal from Redis queue
        """
        try:
            # Validate signal is for this symbol
            if signal.symbol != self.symbol:
                self.logger.debug(
                    f"Ignoring signal for {signal.symbol} (this service handles {self.symbol})"
                )
                return

            action_str = signal.action if hasattr(signal, 'action') else 'ENTRY'
            self.logger.info(
                f"Processing manual signal: {signal.symbol} {action_str} {signal.direction} "
                f"from {signal.source} (id: {signal.signal_id})"
            )

            # Get current market data from indicator processor
            market_data = self._get_market_data()
            if not market_data:
                self.logger.error(
                    f"Cannot process signal - market data not available for {self.symbol}"
                )
                self._mark_signal_failed(signal, "Market data not available")
                return

            # Get current price
            current_price = signal.entry_price
            if current_price is None:
                current_price = self._get_current_price(market_data)
                if current_price is None:
                    self.logger.error("Cannot determine current price")
                    self._mark_signal_failed(signal, "Cannot determine current price")
                    return

            # Build evaluation_result structure expected by TradeExecutionService
            evaluation_result = self._build_evaluation_result(signal)

            # Create correlation ID for tracking
            correlation_id = f"manual_{signal.signal_id}"

            # Create and publish SignalGeneratedEvent
            # Uses "manual" strategy name to reference the manual.yaml risk configuration
            # The evaluation_result determines if it's an entry or exit
            signal_event = SignalGeneratedEvent(
                symbol=self.symbol,
                strategy_name="manual",  # References manual.yaml strategy for risk config
                signal_type=signal.direction,
                confidence=1.0,  # Manual signals have full confidence
                entry_price=current_price,
                stop_loss=None,  # Will be calculated by EntryManager using manual.yaml risk config
                take_profit=None,  # Will be calculated by EntryManager using manual.yaml risk config
                metadata={
                    'evaluation_result': evaluation_result,
                    'market_data': market_data,
                    'manual_signal_id': signal.signal_id,
                    'signal_source': signal.source,
                    'signal_action': action_str,
                    'is_manual': True,  # Tag for identification
                    'original_timestamp': signal.timestamp
                },
                correlation_id=correlation_id
            )

            # Publish to local event bus
            self.event_bus.publish(signal_event)

            # Mark signal as processed
            self.manual_signal_store.mark_processed(
                signal,
                {
                    'status': 'published',
                    'correlation_id': correlation_id,
                    'action': action_str,
                    'price': current_price
                }
            )

            # Update statistics
            self._signals_processed += 1
            self._last_signal_time = datetime.now()

            self.logger.info(
                f"Manual signal published successfully: {self.symbol} {action_str} {signal.direction} "
                f"@ {current_price} (correlation: {correlation_id})"
            )

        except Exception as e:
            self.logger.error(
                f"Error processing manual signal: {e}",
                exc_info=True
            )
            self._mark_signal_failed(signal, str(e))

    def _get_market_data(self) -> Optional[Dict[str, Any]]:
        """
        Get current market data from indicator processor.

        Returns:
            Dictionary with recent rows per timeframe (deques of pd.Series),
            or None if not available
        """
        try:
            # Get recent rows directly - returns Dict[str, deque[pd.Series]]
            # EntryManager expects this format for price extraction
            recent_rows = self.indicator_processor.get_recent_rows()

            if not recent_rows:
                return None

            # Return deques directly - EntryManager handles deque[pd.Series]
            return recent_rows if recent_rows else None

        except Exception as e:
            self.logger.error(f"Error getting market data: {e}")
            return None

    def _get_current_price(self, market_data: Dict[str, Any]) -> Optional[float]:
        """
        Get current price from market data.

        Args:
            market_data: Dictionary with recent rows per timeframe (deques of pd.Series)

        Returns:
            Current close price or None
        """
        try:
            from collections import deque
            import pandas as pd

            # Try primary timeframe first
            if self.primary_timeframe in market_data:
                rows = market_data[self.primary_timeframe]
                if rows:
                    # Handle deque of pd.Series (from IndicatorProcessor)
                    if isinstance(rows, deque) and rows:
                        latest = rows[-1]
                        if isinstance(latest, pd.Series) and 'close' in latest.index:
                            return float(latest['close'])
                    # Handle list
                    elif isinstance(rows, list) and rows:
                        latest = rows[-1]
                        if isinstance(latest, pd.Series) and 'close' in latest.index:
                            return float(latest['close'])
                        elif isinstance(latest, dict) and 'close' in latest:
                            return float(latest['close'])

            # Try any timeframe
            for tf, rows in market_data.items():
                if rows:
                    # Handle deque of pd.Series
                    if isinstance(rows, deque) and rows:
                        latest = rows[-1]
                        if isinstance(latest, pd.Series) and 'close' in latest.index:
                            return float(latest['close'])
                    # Handle list
                    elif isinstance(rows, list) and rows:
                        latest = rows[-1]
                        if isinstance(latest, pd.Series) and 'close' in latest.index:
                            return float(latest['close'])
                        elif isinstance(latest, dict) and 'close' in latest:
                            return float(latest['close'])

            return None

        except Exception as e:
            self.logger.error(f"Error getting current price: {e}")
            return None

    def _build_evaluation_result(self, signal: ManualSignal) -> Dict[str, Any]:
        """
        Build evaluation_result structure expected by TradeExecutionService.

        The EntryManager expects this structure to determine trade direction.
        Supports both ENTRY and EXIT actions.

        Args:
            signal: ManualSignal with direction and action

        Returns:
            Dictionary with entry/exit structure
        """
        is_buy = signal.direction == "BUY"
        is_sell = signal.direction == "SELL"

        # Check action type (default to ENTRY for backward compatibility)
        is_entry = getattr(signal, 'is_entry', True) if hasattr(signal, 'is_entry') else signal.action == "ENTRY" if hasattr(signal, 'action') else True
        is_exit = getattr(signal, 'is_exit', False) if hasattr(signal, 'is_exit') else signal.action == "EXIT" if hasattr(signal, 'action') else False

        if is_exit:
            # Exit signal: close existing position in the specified direction
            # EXIT BUY = close long positions, EXIT SELL = close short positions
            # close_percent: 1-100, None means 100% (close all)
            close_percent = getattr(signal, 'close_percent', None) or 100.0
            return {
                'entry': {
                    'long': False,
                    'short': False
                },
                'exit': {
                    'long': is_buy,   # EXIT BUY closes longs
                    'short': is_sell  # EXIT SELL closes shorts
                },
                'confidence': 1.0,
                'source': 'manual',
                'signal_id': signal.signal_id,
                'is_manual_exit': True,
                'close_percent': close_percent  # Percentage of position to close
            }
        else:
            # Entry signal: open new position
            return {
                'entry': {
                    'long': is_buy,
                    'short': is_sell
                },
                'exit': {
                    'long': False,
                    'short': False
                },
                'confidence': 1.0,
                'source': 'manual',
                'signal_id': signal.signal_id,
                'is_manual_entry': True
            }

    def _mark_signal_failed(self, signal: ManualSignal, error: str) -> None:
        """Mark a signal as failed."""
        self.manual_signal_store.mark_processed(
            signal,
            {
                'status': 'failed',
                'error': error
            }
        )
        self._signals_failed += 1

    def get_status(self) -> Dict[str, Any]:
        """Get service status with manual signal-specific information."""
        status = super().get_status()
        status.update({
            'enabled': self.enabled,
            'poll_interval': self.poll_interval,
            'signals_processed': self._signals_processed,
            'signals_failed': self._signals_failed,
            'queue_length': self.manual_signal_store.get_queue_length(),
            'last_signal_time': (
                self._last_signal_time.isoformat()
                if self._last_signal_time else None
            )
        })
        return status


class MultiSymbolManualSignalService:
    """
    Manager for multiple ManualSignalService instances.

    Creates and manages ManualSignalService for each configured symbol,
    allowing a single Redis queue to service multiple symbols.
    """

    def __init__(
        self,
        symbols: list,
        redis_client: Any,
        indicator_processors: Dict[str, Any],
        event_buses: Dict[str, LocalEventBus],
        account_name: str,
        config: Dict[str, Any],
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize multi-symbol manual signal service.

        Args:
            symbols: List of trading symbols
            redis_client: Redis client instance
            indicator_processors: Dict mapping symbol to IndicatorProcessor
            event_buses: Dict mapping symbol to LocalEventBus
            account_name: Account name for Redis key prefix
            config: Service configuration
            logger: Optional logger
        """
        self.symbols = symbols
        self.logger = logger or logging.getLogger(__name__)
        self.config = config

        # Create shared ManualSignalStore
        self.signal_store = ManualSignalStore(
            redis_client=redis_client,
            account_name=account_name,
            logger=self.logger
        )

        # Create service for each symbol
        self._services: Dict[str, ManualSignalService] = {}
        for symbol in symbols:
            if symbol in indicator_processors and symbol in event_buses:
                self._services[symbol] = ManualSignalService(
                    symbol=symbol,
                    manual_signal_store=self.signal_store,
                    indicator_processor=indicator_processors[symbol],
                    event_bus=event_buses[symbol],
                    config=config,
                    logger=self.logger
                )

    def start_all(self) -> None:
        """Start all symbol services."""
        for symbol, service in self._services.items():
            try:
                service.start()
            except Exception as e:
                self.logger.error(f"Failed to start service for {symbol}: {e}")

    def stop_all(self) -> None:
        """Stop all symbol services."""
        for symbol, service in self._services.items():
            try:
                service.stop()
            except Exception as e:
                self.logger.error(f"Failed to stop service for {symbol}: {e}")

    def get_status(self) -> Dict[str, Any]:
        """Get status of all services."""
        return {
            symbol: service.get_status()
            for symbol, service in self._services.items()
        }
