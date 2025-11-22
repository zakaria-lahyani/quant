"""
Data Fetching Service - Fetches new candle data and publishes events.
"""

import logging
import time
from typing import Optional, Dict, Any, List

from app.orchestrator.services.base import TradingService, ServiceState
from app.orchestrator.services.event_bus import LocalEventBus
from app.orchestrator.events import NewCandleEvent, OrchestratorEventType


class DataFetchingService(TradingService):
    """
    Service responsible for fetching new candle data.

    Responsibilities:
    - Periodically fetch new candle data for all timeframes
    - Publish NEW_CANDLE events when new data is available
    - Handle fetch errors and retry logic

    Events Published:
    - NEW_CANDLE: When new candle data is fetched
    - DATA_FETCH_FAILED: When fetch fails after retries
    """

    def __init__(
        self,
        symbol: str,
        timeframes: List[str],
        data_source: Any,
        event_bus: LocalEventBus,
        config: Dict[str, Any],
        display_symbol: Optional[str] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize data fetching service.

        Args:
            symbol: Broker-specific symbol for MT5 API calls (e.g., "XAUUSD.pro")
            timeframes: List of timeframes to fetch
            data_source: Data source manager for fetching data
            event_bus: Local event bus for publishing events
            config: Service configuration from system_config
            display_symbol: Display symbol for events (e.g., "XAUUSD"). Defaults to symbol if not provided.
            logger: Optional logger
        """
        # Store broker symbol for API calls, display symbol for events
        self.broker_symbol = symbol  # For MT5 API calls
        self.display_symbol = display_symbol or symbol  # For events

        # Initialize base class with display symbol (for event routing)
        super().__init__(self.display_symbol, event_bus, logger)

        self.timeframes = timeframes
        self.data_source = data_source
        self.config = config

        # Configuration
        self.fetch_interval = config.get('fetch_interval', 30)
        self.retry_attempts = config.get('retry_attempts', 3)
        self.candle_index = config.get('candle_index', 2)
        self.nbr_bars = config.get('nbr_bars', 2)

        # State
        self._running = False
        self._last_fetch_time: Dict[str, float] = {}

    def start(self) -> None:
        """Start the data fetching service."""
        self.logger.info(f"Starting DataFetchingService for {self.symbol}...")
        self._set_state(ServiceState.STARTING)

        try:
            self._running = True
            self._set_state(ServiceState.RUNNING)
            self.logger.info(f"DataFetchingService started for {self.symbol}")

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
        """Stop the data fetching service."""
        self.logger.info(f"Stopping DataFetchingService for {self.symbol}...")
        self._set_state(ServiceState.STOPPING)

        self._running = False

        self._set_state(ServiceState.STOPPED)
        self.logger.info(f"DataFetchingService stopped for {self.symbol}")

        # Publish service stopped event
        self.event_bus.publish(
            self.event_bus.create_event(
                event_type=OrchestratorEventType.SERVICE_STOPPED,
                data={'service': self.service_name},
                source_service=self.service_name
            )
        )

    def fetch_and_publish(self) -> None:
        """
        Fetch new candle data for all timeframes and publish events.

        This should be called periodically by an external scheduler.
        """
        if not self.is_running:
            return

        for timeframe in self.timeframes:
            try:
                # Check if enough time has passed since last fetch
                current_time = time.time()
                last_fetch = self._last_fetch_time.get(timeframe, 0)

                if current_time - last_fetch < self.fetch_interval:
                    continue

                # Fetch new candle data with retry logic
                candle_data = self._fetch_with_retry(timeframe)

                if candle_data is not None:
                    # Update last fetch time
                    self._last_fetch_time[timeframe] = current_time

                    # Publish NEW_CANDLE event
                    event = NewCandleEvent(
                        symbol=self.symbol,
                        timeframe=timeframe,
                        candle_data=candle_data
                    )
                    self.event_bus.publish(event)

                    self.logger.debug(
                        f"Published NEW_CANDLE event for {self.symbol} {timeframe}"
                    )

            except Exception as e:
                self.logger.error(
                    f"Error fetching data for {self.symbol} {timeframe}: {e}"
                )
                self._publish_fetch_failed_event(timeframe, str(e))

    def _fetch_with_retry(self, timeframe: str) -> Optional[Dict[str, Any]]:
        """
        Fetch data with retry logic.

        Args:
            timeframe: Timeframe to fetch

        Returns:
            Candle data or None if all retries failed
        """
        last_error = None

        for attempt in range(self.retry_attempts):
            try:
                # Fetch historical data using broker-specific symbol
                data = self.data_source.get_historical_data(
                    symbol=self.broker_symbol,  # Use broker symbol for MT5 API
                    timeframe=timeframe
                )

                if data is not None and len(data) > 0:
                    # Get the most recent closed candle
                    candle = data.iloc[-self.candle_index] if len(data) >= self.candle_index else data.iloc[-1]

                    # Convert to dictionary - use preserved original datetime
                    candle_dict = {
                        'time': candle.name,  # Index is timestamp
                        'candle_time': candle['candle_time_original'],  # Original candle datetime from MT5 API
                        'open': float(candle['open']),
                        'high': float(candle['high']),
                        'low': float(candle['low']),
                        'close': float(candle['close']),
                        'volume': float(candle['tick_volume']) if 'tick_volume' in candle else 0.0
                    }

                    return candle_dict

            except Exception as e:
                last_error = e
                self.logger.warning(
                    f"Fetch attempt {attempt + 1}/{self.retry_attempts} failed "
                    f"for {self.display_symbol} {timeframe}: {e}"
                )

                if attempt < self.retry_attempts - 1:
                    time.sleep(1)  # Wait before retry

        # All retries failed
        self.logger.error(
            f"All fetch attempts failed for {self.display_symbol} {timeframe}: {last_error}"
        )
        return None

    def _publish_fetch_failed_event(self, timeframe: str, error_message: str) -> None:
        """Publish data fetch failed event."""
        self.event_bus.publish(
            self.event_bus.create_event(
                event_type=OrchestratorEventType.DATA_FETCH_FAILED,
                data={
                    'timeframe': timeframe,
                    'error': error_message
                },
                source_service=self.service_name
            )
        )

    def should_fetch(self, timeframe: str) -> bool:
        """
        Check if enough time has passed to fetch this timeframe.

        Args:
            timeframe: Timeframe to check

        Returns:
            True if should fetch
        """
        current_time = time.time()
        last_fetch = self._last_fetch_time.get(timeframe, 0)
        return current_time - last_fetch >= self.fetch_interval

    def get_status(self) -> Dict[str, Any]:
        """Get service status with fetch-specific information."""
        status = super().get_status()
        status.update({
            'timeframes': self.timeframes,
            'fetch_interval': self.fetch_interval,
            'last_fetch_times': {
                tf: time.time() - last_time
                for tf, last_time in self._last_fetch_time.items()
            }
        })
        return status
