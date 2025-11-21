"""
Redis-based event bus for distributed trade event management.

This module provides a client that connects to an external Redis Docker container
to publish and subscribe to trade events across the trading system.
"""

import logging
import json
from typing import Optional, Callable, Dict, Any, List
from datetime import datetime

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from .event_types import TradeEvent, TradeEventType


class RedisEventBus:
    """
    Redis-based event bus for publishing and subscribing to trade events.

    This client connects to an external Redis Docker container and uses
    pub/sub channels for real-time event distribution.

    The Redis container manages all operational configuration (streams, TTLs,
    consumer groups, etc.). This client only connects and publishes/subscribes.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        app_tag: str = "DEFAULT",
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize Redis event bus client.

        Args:
            host: Redis server hostname
            port: Redis server port
            db: Redis database number
            password: Optional Redis password
            app_tag: Application/broker tag for event identification
            logger: Optional logger instance

        Raises:
            ImportError: If redis-py is not installed
            redis.ConnectionError: If cannot connect to Redis
        """
        if not REDIS_AVAILABLE:
            raise ImportError(
                "redis-py is not installed. Install it with: pip install redis"
            )

        self.logger = logger or logging.getLogger(__name__)

        # Connection parameters
        self._host = host
        self._port = port
        self._db = db
        self._password = password
        self.app_tag = app_tag

        # Redis client
        self._client: Optional[redis.Redis] = None
        self._pubsub: Optional[redis.client.PubSub] = None

        # Event handlers
        self._handlers: Dict[TradeEventType, List[Callable]] = {}

        # Connect to Redis
        self._connect()

    def _connect(self) -> None:
        """Establish connection to Redis server."""
        try:
            self._client = redis.Redis(
                host=self._host,
                port=self._port,
                db=self._db,
                password=self._password,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )

            # Test connection
            self._client.ping()
            self.logger.info(
                f"Successfully connected to Redis at {self._host}:{self._port}"
            )

        except redis.ConnectionError as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise

    def publish(self, event: TradeEvent) -> int:
        """
        Publish a trade event to Redis.

        Events are published to a channel named: trades:{app_tag}:{symbol}:{event_type}
        For example: trades:BROKER_A:XAUUSD:order.created

        Args:
            event: TradeEvent to publish

        Returns:
            Number of subscribers that received the message

        Raises:
            redis.RedisError: If publish fails
        """
        if not self._client:
            raise RuntimeError("Redis client not initialized")

        # Automatically set app_tag if not already set
        if not event.app_tag:
            event.app_tag = self.app_tag

        # Construct channel name with app_tag for multi-instance support
        channel = f"trades:{event.app_tag}:{event.symbol}:{event.event_type.value}"

        try:
            # Publish event as JSON
            num_subscribers = self._client.publish(channel, event.to_json())

            self.logger.debug(
                f"Published {event.event_type.value} event for {event.symbol} "
                f"[{event.app_tag}] to {num_subscribers} subscribers"
            )

            return num_subscribers

        except redis.RedisError as e:
            self.logger.error(f"Failed to publish event: {e}")
            raise

    def subscribe(
        self,
        symbol: str,
        event_type: TradeEventType,
        handler: Callable[[TradeEvent], None]
    ) -> None:
        """
        Subscribe to specific trade events.

        Args:
            symbol: Trading symbol (e.g., "XAUUSD")
            event_type: Type of event to subscribe to
            handler: Callback function to handle events

        Example:
            >>> def handle_order(event: TradeEvent):
            ...     print(f"Order created: {event.data['order_id']}")
            >>>
            >>> bus.subscribe("XAUUSD", TradeEventType.ORDER_CREATED, handle_order)
        """
        if event_type not in self._handlers:
            self._handlers[event_type] = []

        self._handlers[event_type].append(handler)

        # Subscribe to channel
        channel = f"trades:{symbol}:{event_type.value}"
        self._ensure_pubsub()
        self._pubsub.subscribe(channel)

        self.logger.info(
            f"Subscribed to {event_type.value} events for {symbol}"
        )

    def subscribe_all(
        self,
        symbol: str,
        handler: Callable[[TradeEvent], None]
    ) -> None:
        """
        Subscribe to all trade events for a symbol.

        Args:
            symbol: Trading symbol (e.g., "XAUUSD")
            handler: Callback function to handle all events

        Example:
            >>> def handle_all_events(event: TradeEvent):
            ...     print(f"Event: {event.event_type.value}")
            >>>
            >>> bus.subscribe_all("XAUUSD", handle_all_events)
        """
        pattern = f"trades:{symbol}:*"
        self._ensure_pubsub()
        self._pubsub.psubscribe(pattern)

        # Register handler for all event types
        for event_type in TradeEventType:
            if event_type not in self._handlers:
                self._handlers[event_type] = []
            self._handlers[event_type].append(handler)

        self.logger.info(f"Subscribed to all events for {symbol}")

    def _ensure_pubsub(self) -> None:
        """Ensure pub/sub connection is initialized."""
        if self._pubsub is None:
            self._pubsub = self._client.pubsub()

    def listen(self, timeout: Optional[float] = None) -> None:
        """
        Start listening for events (blocking).

        This method blocks and processes incoming events. Call this in a
        separate thread or process if you need non-blocking behavior.

        Args:
            timeout: Optional timeout for get_message() in seconds

        Example:
            >>> # In a separate thread
            >>> bus.subscribe_all("XAUUSD", handler)
            >>> bus.listen()
        """
        if not self._pubsub:
            self.logger.warning("No subscriptions active")
            return

        self.logger.info("Starting event listener...")

        try:
            for message in self._pubsub.listen():
                if message['type'] in ('message', 'pmessage'):
                    self._handle_message(message)

        except KeyboardInterrupt:
            self.logger.info("Event listener stopped by user")
        except Exception as e:
            self.logger.error(f"Error in event listener: {e}")
            raise

    def _handle_message(self, message: Dict[str, Any]) -> None:
        """
        Handle incoming Redis message.

        Args:
            message: Redis message dictionary
        """
        try:
            # Parse event from JSON
            event = TradeEvent.from_json(message['data'])

            # Call registered handlers
            if event.event_type in self._handlers:
                for handler in self._handlers[event.event_type]:
                    try:
                        handler(event)
                    except Exception as e:
                        self.logger.error(
                            f"Error in event handler: {e}",
                            exc_info=True
                        )

        except Exception as e:
            self.logger.error(f"Failed to process message: {e}")

    def get_position(self, symbol: str, position_id: str) -> Optional[Dict[str, Any]]:
        """
        Get current position state from Redis.

        The Redis container maintains position state in hashes.
        Key format: position:{symbol}:{position_id}

        Args:
            symbol: Trading symbol
            position_id: Position identifier

        Returns:
            Position data dictionary or None if not found
        """
        if not self._client:
            raise RuntimeError("Redis client not initialized")

        key = f"position:{symbol}:{position_id}"
        data = self._client.hgetall(key)

        return data if data else None

    def get_order(self, symbol: str, order_id: str) -> Optional[Dict[str, Any]]:
        """
        Get current order state from Redis.

        The Redis container maintains order state in hashes.
        Key format: order:{symbol}:{order_id}

        Args:
            symbol: Trading symbol
            order_id: Order identifier

        Returns:
            Order data dictionary or None if not found
        """
        if not self._client:
            raise RuntimeError("Redis client not initialized")

        key = f"order:{symbol}:{order_id}"
        data = self._client.hgetall(key)

        return data if data else None

    def get_active_positions(self, symbol: str) -> List[Dict[str, Any]]:
        """
        Get all active positions for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            List of position dictionaries
        """
        if not self._client:
            raise RuntimeError("Redis client not initialized")

        # Scan for position keys
        pattern = f"position:{symbol}:*"
        positions = []

        for key in self._client.scan_iter(match=pattern):
            position = self._client.hgetall(key)
            if position.get('status') == 'active':
                positions.append(position)

        return positions

    def close(self) -> None:
        """Close Redis connections."""
        if self._pubsub:
            self._pubsub.close()
            self._pubsub = None

        if self._client:
            self._client.close()
            self._client = None

        self.logger.info("Redis event bus closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    @property
    def is_connected(self) -> bool:
        """Check if connected to Redis."""
        if not self._client:
            return False
        try:
            self._client.ping()
            return True
        except redis.RedisError:
            return False
