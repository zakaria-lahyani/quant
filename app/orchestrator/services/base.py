"""
Base classes for event-driven trading services.
"""

import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Dict, Any, Callable
from datetime import datetime


class ServiceState(str, Enum):
    """Service lifecycle states."""
    CREATED = "created"
    STARTING = "starting"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"
    RECOVERING = "recovering"


class TradingService(ABC):
    """
    Base class for all event-driven trading services.

    Services are independent, stateless workers that:
    - Subscribe to specific events
    - Process events asynchronously
    - Publish new events when done
    - Handle errors gracefully
    """

    def __init__(
        self,
        symbol: str,
        event_bus: 'LocalEventBus',
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize trading service.

        Args:
            symbol: Trading symbol this service handles
            event_bus: Event bus for pub/sub
            logger: Optional logger instance
        """
        self.symbol = symbol
        self.event_bus = event_bus
        self.logger = logger or logging.getLogger(self.__class__.__name__)

        self._state = ServiceState.CREATED
        self._error_count = 0
        self._last_error: Optional[str] = None
        self._start_time: Optional[datetime] = None

    @property
    def state(self) -> ServiceState:
        """Get current service state."""
        return self._state

    @property
    def service_name(self) -> str:
        """Get service name."""
        return self.__class__.__name__

    @property
    def is_running(self) -> bool:
        """Check if service is running."""
        return self._state == ServiceState.RUNNING

    @property
    def is_healthy(self) -> bool:
        """Check if service is healthy."""
        return self._state in (ServiceState.RUNNING, ServiceState.PAUSED)

    @abstractmethod
    def start(self) -> None:
        """
        Start the service.

        This should:
        1. Subscribe to relevant events
        2. Initialize service resources
        3. Transition to RUNNING state
        """
        pass

    @abstractmethod
    def stop(self) -> None:
        """
        Stop the service.

        This should:
        1. Unsubscribe from events
        2. Clean up resources
        3. Transition to STOPPED state
        """
        pass

    def pause(self) -> None:
        """Pause the service."""
        if self._state == ServiceState.RUNNING:
            self._state = ServiceState.PAUSED
            self.logger.info(f"{self.service_name} paused")

    def resume(self) -> None:
        """Resume the service."""
        if self._state == ServiceState.PAUSED:
            self._state = ServiceState.RUNNING
            self.logger.info(f"{self.service_name} resumed")

    def handle_error(self, error: Exception) -> None:
        """
        Handle service error.

        Args:
            error: Exception that occurred
        """
        self._error_count += 1
        self._last_error = str(error)
        self._state = ServiceState.ERROR

        self.logger.error(
            f"{self.service_name} error (count: {self._error_count}): {error}",
            exc_info=True
        )

        # Attempt recovery if error count is low
        if self._error_count < 3:
            self.recover()

    def recover(self) -> None:
        """Attempt to recover from error state."""
        self.logger.info(f"{self.service_name} attempting recovery...")
        self._state = ServiceState.RECOVERING

        try:
            # Stop and restart
            self.stop()
            self.start()
            self._state = ServiceState.RUNNING
            self._error_count = 0
            self.logger.info(f"{self.service_name} recovered successfully")

        except Exception as e:
            self.logger.error(f"{self.service_name} recovery failed: {e}")
            self._state = ServiceState.ERROR

    def get_status(self) -> Dict[str, Any]:
        """
        Get service status information.

        Returns:
            Status dictionary
        """
        return {
            'service_name': self.service_name,
            'symbol': self.symbol,
            'state': self._state.value,
            'is_healthy': self.is_healthy,
            'error_count': self._error_count,
            'last_error': self._last_error,
            'uptime': (
                (datetime.utcnow() - self._start_time).total_seconds()
                if self._start_time else 0
            )
        }

    def _set_state(self, state: ServiceState) -> None:
        """Set service state and log transition."""
        old_state = self._state
        self._state = state

        if state == ServiceState.RUNNING and not self._start_time:
            self._start_time = datetime.utcnow()

        self.logger.debug(
            f"{self.service_name} state transition: {old_state.value} -> {state.value}"
        )

    def __repr__(self) -> str:
        return f"<{self.service_name} symbol={self.symbol} state={self._state.value}>"
