"""
Local event bus for symbol-scoped service coordination.

This is different from the Redis event bus - it's used for coordinating
services within a single symbol orchestrator (in-process communication).
"""

import logging
from typing import Dict, List, Callable, Any, Optional
from collections import defaultdict
import uuid

from app.orchestrator.events import OrchestratorEvent, OrchestratorEventType


class LocalEventBus:
    """
    In-process event bus for service coordination within a symbol orchestrator.

    This is NOT the Redis event bus. This is for local service-to-service
    communication within the orchestrator (e.g., DataFetching -> Indicators).
    """

    def __init__(
        self,
        symbol: str,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize local event bus.

        Args:
            symbol: Trading symbol this bus serves
            logger: Optional logger instance
        """
        self.symbol = symbol
        self.logger = logger or logging.getLogger(__name__)

        # Event handlers: {event_type: [handler1, handler2, ...]}
        self._handlers: Dict[OrchestratorEventType, List[Callable]] = defaultdict(list)

        # Event history for debugging
        self._event_history: List[OrchestratorEvent] = []
        self._max_history = 1000

    def subscribe(
        self,
        event_type: OrchestratorEventType,
        handler: Callable[[OrchestratorEvent], None],
        service_name: Optional[str] = None
    ) -> None:
        """
        Subscribe to an event type.

        Args:
            event_type: Event type to subscribe to
            handler: Callback function to handle events
            service_name: Optional service name for logging
        """
        self._handlers[event_type].append(handler)

        self.logger.debug(
            f"[{self.symbol}] {service_name or 'Service'} subscribed to {event_type.value}"
        )

    def unsubscribe(
        self,
        event_type: OrchestratorEventType,
        handler: Callable[[OrchestratorEvent], None]
    ) -> None:
        """
        Unsubscribe from an event type.

        Args:
            event_type: Event type to unsubscribe from
            handler: Handler to remove
        """
        if event_type in self._handlers:
            try:
                self._handlers[event_type].remove(handler)
                self.logger.debug(
                    f"[{self.symbol}] Unsubscribed from {event_type.value}"
                )
            except ValueError:
                pass

    def publish(self, event: OrchestratorEvent) -> int:
        """
        Publish an event to all subscribers.

        Args:
            event: Event to publish

        Returns:
            Number of handlers that processed the event
        """
        # Validate symbol matches
        if event.symbol != self.symbol:
            self.logger.warning(
                f"Event symbol mismatch: expected {self.symbol}, got {event.symbol}"
            )
            return 0

        # Add to history
        self._add_to_history(event)

        # Get handlers for this event type
        handlers = self._handlers.get(event.event_type, [])

        if not handlers:
            self.logger.debug(
                f"[{self.symbol}] No handlers for {event.event_type.value}"
            )
            return 0

        # Call all handlers
        handled_count = 0
        for handler in handlers:
            try:
                handler(event)
                handled_count += 1
            except Exception as e:
                self.logger.error(
                    f"[{self.symbol}] Error in event handler for {event.event_type.value}: {e}",
                    exc_info=True
                )

        self.logger.debug(
            f"[{self.symbol}] Published {event.event_type.value} to {handled_count} handlers"
        )

        return handled_count

    def publish_sync(self, event: OrchestratorEvent) -> None:
        """
        Publish event synchronously (alias for publish).

        Args:
            event: Event to publish
        """
        self.publish(event)

    def create_event(
        self,
        event_type: OrchestratorEventType,
        data: Dict[str, Any],
        source_service: Optional[str] = None,
        correlation_id: Optional[str] = None
    ) -> OrchestratorEvent:
        """
        Helper to create an event.

        Args:
            event_type: Type of event
            data: Event data
            source_service: Service creating the event
            correlation_id: Optional correlation ID

        Returns:
            Created event
        """
        from datetime import datetime

        if correlation_id is None:
            correlation_id = str(uuid.uuid4())

        return OrchestratorEvent(
            event_type=event_type,
            symbol=self.symbol,
            timestamp=datetime.utcnow().isoformat(),
            data=data,
            correlation_id=correlation_id,
            source_service=source_service
        )

    def get_event_history(
        self,
        event_type: Optional[OrchestratorEventType] = None,
        limit: int = 100
    ) -> List[OrchestratorEvent]:
        """
        Get event history.

        Args:
            event_type: Optional event type filter
            limit: Maximum number of events to return

        Returns:
            List of events
        """
        history = self._event_history

        if event_type:
            history = [e for e in history if e.event_type == event_type]

        return history[-limit:]

    def clear_history(self) -> None:
        """Clear event history."""
        self._event_history.clear()
        self.logger.debug(f"[{self.symbol}] Event history cleared")

    def get_stats(self) -> Dict[str, Any]:
        """
        Get event bus statistics.

        Returns:
            Statistics dictionary
        """
        event_counts = defaultdict(int)
        for event in self._event_history:
            event_counts[event.event_type.value] += 1

        return {
            'symbol': self.symbol,
            'total_events': len(self._event_history),
            'event_counts': dict(event_counts),
            'subscriber_counts': {
                event_type.value: len(handlers)
                for event_type, handlers in self._handlers.items()
            }
        }

    def _add_to_history(self, event: OrchestratorEvent) -> None:
        """Add event to history, maintaining max size."""
        self._event_history.append(event)

        # Trim history if needed
        if len(self._event_history) > self._max_history:
            self._event_history = self._event_history[-self._max_history:]

    def __repr__(self) -> str:
        return f"<LocalEventBus symbol={self.symbol} subscribers={len(self._handlers)}>"
