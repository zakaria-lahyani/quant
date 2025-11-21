"""
Indicator Calculation Service - Calculates indicators and publishes events.
"""

import logging
from typing import Optional, Dict, Any

from app.orchestrator.services.base import TradingService, ServiceState
from app.orchestrator.services.event_bus import LocalEventBus
from app.orchestrator.events import (
    OrchestratorEvent,
    OrchestratorEventType,
    NewCandleEvent,
    IndicatorsUpdatedEvent,
    RegimeChangedEvent
)
from app.infrastructure.events.indicator_redis_store import IndicatorRedisStore


class IndicatorCalculationService(TradingService):
    """
    Service responsible for calculating indicators when new candles arrive.

    Responsibilities:
    - Subscribe to NEW_CANDLE events
    - Update historical data with new candles
    - Calculate indicators via IndicatorProcessor
    - Check for regime changes via RegimeManager
    - Publish INDICATORS_UPDATED and REGIME_CHANGED events

    Events Subscribed:
    - NEW_CANDLE: Triggered when new candle data arrives

    Events Published:
    - INDICATORS_UPDATED: When indicators are calculated
    - REGIME_CHANGED: When market regime changes
    - INDICATOR_CALCULATION_FAILED: When calculation fails
    """

    def __init__(
        self,
        symbol: str,
        indicator_processor: Any,
        regime_manager: Any,
        historicals: Dict[str, Any],
        event_bus: LocalEventBus,
        config: Dict[str, Any],
        redis_client: Optional[Any] = None,
        account_name: Optional[str] = None,
        account_tag: Optional[str] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize indicator calculation service.

        Args:
            symbol: Trading symbol
            indicator_processor: IndicatorProcessor instance
            regime_manager: RegimeManager instance
            historicals: Historical data for all timeframes
            event_bus: Local event bus
            config: Service configuration
            redis_client: Optional Redis client for storing indicator data
            account_name: Account/broker name for Redis metadata (e.g., "ACG", "FTMO")
            account_tag: Account tag identifier
            logger: Optional logger
        """
        super().__init__(symbol, event_bus, logger)

        self.indicator_processor = indicator_processor
        self.regime_manager = regime_manager
        self.historicals = historicals
        self.config = config
        self.account_name = account_name
        self.account_tag = account_tag

        # Configuration
        self.recent_rows_limit = config.get('recent_rows_limit', 6)
        self.track_regime_changes = config.get('track_regime_changes', True)
        self.store_indicators_in_redis = config.get('store_indicators_in_redis', False)

        # Redis store for indicators
        self.redis_store: Optional[IndicatorRedisStore] = None
        if redis_client and self.store_indicators_in_redis:
            try:
                self.redis_store = IndicatorRedisStore(
                    redis_client=redis_client,
                    symbol=symbol,
                    max_recent_rows=config.get('redis_max_recent_rows', 50),
                    ttl_seconds=config.get('redis_ttl_seconds', 3600),
                    account_name=account_name,
                    account_tag=account_tag,
                    logger=logger
                )
                self.logger.info(
                    f"Redis indicator storage enabled for {symbol} "
                    f"[Account: {account_name or 'Unknown'}]"
                )
            except Exception as e:
                self.logger.warning(f"Failed to initialize Redis indicator store: {e}")

    def start(self) -> None:
        """Start the indicator calculation service."""
        self.logger.info(f"Starting IndicatorCalculationService for {self.symbol}...")
        self._set_state(ServiceState.STARTING)

        try:
            # Subscribe to NEW_CANDLE events
            self.event_bus.subscribe(
                OrchestratorEventType.NEW_CANDLE,
                self._handle_new_candle,
                service_name=self.service_name
            )

            self._set_state(ServiceState.RUNNING)
            self.logger.info(f"IndicatorCalculationService started for {self.symbol}")

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
        """Stop the indicator calculation service."""
        self.logger.info(f"Stopping IndicatorCalculationService for {self.symbol}...")
        self._set_state(ServiceState.STOPPING)

        # Unsubscribe from events
        self.event_bus.unsubscribe(
            OrchestratorEventType.NEW_CANDLE,
            self._handle_new_candle
        )

        self._set_state(ServiceState.STOPPED)
        self.logger.info(f"IndicatorCalculationService stopped for {self.symbol}")

        # Publish service stopped event
        self.event_bus.publish(
            self.event_bus.create_event(
                event_type=OrchestratorEventType.SERVICE_STOPPED,
                data={'service': self.service_name},
                source_service=self.service_name
            )
        )

    def _handle_new_candle(self, event: NewCandleEvent) -> None:
        """
        Handle NEW_CANDLE event.

        Args:
            event: NewCandleEvent containing new candle data
        """
        if not self.is_running:
            return

        try:
            import pandas as pd

            timeframe = event.data['timeframe']
            candle_data = event.data['candle']

            self.logger.debug(
                f"Processing new candle for {self.symbol} {timeframe}: {candle_data}"
            )

            # Update historical data
            self._update_historical_data(timeframe, candle_data)

            # Check for regime changes BEFORE processing indicators
            regime_data = None
            if self.track_regime_changes:
                regime_data = self._check_regime_changes(timeframe)

            # Process new candle through IndicatorProcessor
            # This will:
            # 1. Compute indicators for the new row
            # 2. Add regime data to the row
            # 3. Store the row in recent_rows with previous_* columns
            new_row_series = pd.Series(candle_data)
            new_row_series.name = candle_data['time']  # Set index to timestamp

            processed_row = self.indicator_processor.process_new_row(
                timeframe=timeframe,
                row=new_row_series,
                regime_data=regime_data
            )

            # Store in Redis if enabled
            if self.redis_store:
                try:
                    # Convert Series to dict for Redis storage
                    row_dict = processed_row.to_dict() if hasattr(processed_row, 'to_dict') else dict(processed_row)

                    self.redis_store.store_indicator_row(
                        timeframe=timeframe,
                        row=row_dict,
                        metadata={
                            'num_indicators': len(row_dict),
                            'has_regime': 'regime' in row_dict and row_dict['regime'] != 'unknown'
                        }
                    )
                    self.logger.info(
                        f"Stored indicators for {self.symbol} {timeframe} in Redis "
                        f"(regime: {row_dict.get('regime', 'N/A')})"
                    )
                except Exception as e:
                    self.logger.warning(f"Failed to store indicators in Redis: {e}")

            # Convert processed row to dict for event
            indicators = processed_row.to_dict() if hasattr(processed_row, 'to_dict') else dict(processed_row)

            # Publish INDICATORS_UPDATED event
            indicators_event = IndicatorsUpdatedEvent(
                symbol=self.symbol,
                timeframe=timeframe,
                indicators=indicators,
                regime_data=regime_data,
                correlation_id=event.correlation_id
            )
            self.event_bus.publish(indicators_event)

            self.logger.debug(
                f"Published INDICATORS_UPDATED event for {self.symbol} {timeframe}"
            )

        except Exception as e:
            self.logger.error(
                f"Error calculating indicators for {self.symbol}: {e}",
                exc_info=True
            )
            self._publish_calculation_failed_event(event, str(e))

    def _update_historical_data(self, timeframe: str, candle_data: Dict[str, Any]) -> None:
        """
        Update historical data with new candle.

        Args:
            timeframe: Timeframe to update
            candle_data: New candle data
        """
        import pandas as pd

        if timeframe not in self.historicals:
            self.logger.warning(f"Timeframe {timeframe} not found in historicals")
            return

        # Append to historical data
        historical = self.historicals[timeframe]
        if historical is not None:
            # Convert candle_data to DataFrame row
            new_row = pd.DataFrame([candle_data], index=[candle_data['time']])

            # Check if candle already exists (avoid duplicates)
            if candle_data['time'] not in historical.index:
                # Append new candle
                self.historicals[timeframe] = pd.concat([historical, new_row])
                self.logger.debug(
                    f"Updated historical data for {self.symbol} {timeframe}"
                )
            else:
                # Update existing candle by reconstructing DataFrame
                # This avoids dtype incompatibility warnings
                updated_historical = historical.drop(index=candle_data['time'])
                self.historicals[timeframe] = pd.concat([updated_historical, new_row]).sort_index()
                self.logger.debug(
                    f"Updated existing candle for {self.symbol} {timeframe}"
                )

    def _check_regime_changes(self, timeframe: str) -> Optional[Dict[str, Any]]:
        """
        Check for regime changes.

        Args:
            timeframe: Timeframe to check

        Returns:
            Regime data if changed, None otherwise
        """
        try:
            # Get current regime from regime manager
            current_regime = self.regime_manager.get_regime_data(timeframe)

            if current_regime:
                # Debug logging
                self.logger.debug(f"Raw regime data from manager: {current_regime}")

                regime_data = {
                    'regime': current_regime.get('regime'),
                    'confidence': current_regime.get('regime_confidence'),
                    'is_transition': current_regime.get('is_transition')
                }

                self.logger.debug(f"Processed regime data: {regime_data}")

                # Check if regime actually changed
                # (RegimeManager should handle this internally)
                return regime_data

        except Exception as e:
            self.logger.error(f"Error checking regime changes: {e}")

        return None

    def _publish_calculation_failed_event(
        self,
        original_event: OrchestratorEvent,
        error_message: str
    ) -> None:
        """Publish indicator calculation failed event."""
        self.event_bus.publish(
            self.event_bus.create_event(
                event_type=OrchestratorEventType.INDICATOR_CALCULATION_FAILED,
                data={
                    'original_event': original_event.to_dict(),
                    'error': error_message
                },
                source_service=self.service_name,
                correlation_id=original_event.correlation_id
            )
        )

    def get_status(self) -> Dict[str, Any]:
        """Get service status with indicator-specific information."""
        status = super().get_status()
        status.update({
            'track_regime_changes': self.track_regime_changes,
            'recent_rows_limit': self.recent_rows_limit
        })
        return status
