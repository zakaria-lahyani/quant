"""
Redis storage for manual/external trading signals.

This module provides a Redis-based store for receiving trading signals from
external sources (3rd party systems, manual trading interfaces, etc.) that
trigger the same risk management and execution flow as strategy-generated signals.

Signal Structure:
    {
        "symbol": "XAUUSD",
        "direction": "BUY" or "SELL",
        "action": "ENTRY" or "EXIT" (default: ENTRY),
        "source": "manual" or "external_system_name",
        "entry_price": 2650.50 (optional - will use market price if not provided),
        "close_percent": 50 (optional - percentage to close for EXIT, default: 100),
        "timestamp": "2024-01-15T10:30:00",
        "metadata": {} (optional additional data)
    }

Key Structure:
    - List: {account}:manual_signals - Queue of pending signals (FIFO)
    - Hash: {account}:manual_signals:processed - Recently processed signals

Trade Identification:
    - Manual trades use comment prefix "Manual_" for easy identification
    - Strategy name is set to "manual" for filtering
"""

import logging
import json
from typing import Optional, Dict, Any, List
from datetime import datetime
from dataclasses import dataclass, asdict

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


@dataclass
class ManualSignal:
    """
    Manual trading signal structure.

    Attributes:
        symbol: Trading symbol (e.g., "XAUUSD")
        direction: Trade direction ("BUY" or "SELL")
        action: Signal action ("ENTRY" or "EXIT", default: ENTRY)
        source: Signal source identifier
        entry_price: Optional entry price (uses market if not provided)
        close_percent: Percentage of position to close (1-100, only for EXIT, default: 100)
        timestamp: Signal timestamp (ISO format)
        signal_id: Unique signal identifier
        metadata: Additional signal data
    """
    symbol: str
    direction: str
    action: str = "ENTRY"  # ENTRY or EXIT
    source: str = "manual"
    entry_price: Optional[float] = None
    close_percent: Optional[float] = None  # 1-100, None means 100% (close all)
    timestamp: Optional[str] = None
    signal_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()
        if self.signal_id is None:
            self.signal_id = f"{self.source}_{self.symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        if self.metadata is None:
            self.metadata = {}
        # Normalize fields
        self.direction = self.direction.upper()
        self.symbol = self.symbol.upper()
        self.action = self.action.upper()

    @property
    def is_entry(self) -> bool:
        """Check if this is an entry signal."""
        return self.action == "ENTRY"

    @property
    def is_exit(self) -> bool:
        """Check if this is an exit signal."""
        return self.action == "EXIT"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'symbol': self.symbol,
            'direction': self.direction,
            'action': self.action,
            'source': self.source,
            'entry_price': self.entry_price,
            'close_percent': self.close_percent,
            'timestamp': self.timestamp,
            'signal_id': self.signal_id,
            'metadata': self.metadata
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ManualSignal':
        """Create from dictionary."""
        return cls(
            symbol=data['symbol'],
            direction=data['direction'],
            action=data.get('action', 'ENTRY'),
            source=data.get('source', 'manual'),
            entry_price=data.get('entry_price'),
            close_percent=data.get('close_percent'),
            timestamp=data.get('timestamp'),
            signal_id=data.get('signal_id'),
            metadata=data.get('metadata', {})
        )

    @classmethod
    def from_json(cls, json_str: str) -> 'ManualSignal':
        """Create from JSON string."""
        return cls.from_dict(json.loads(json_str))


class ManualSignalStore:
    """
    Redis store for manual/external trading signals.

    Signals are stored in a Redis list as a queue (FIFO).
    The service polls this queue and processes signals through
    the standard risk management and execution pipeline.

    Key Structure:
        - {account}:manual_signals - Pending signals queue
        - {account}:manual_signals:processed - Hash of processed signal IDs
        - {account}:manual_signals:history - Recent signal history (list)
    """

    def __init__(
        self,
        redis_client: 'redis.Redis',
        account_name: str = "default",
        max_history: int = 100,
        processed_ttl: int = 86400,  # 24 hours
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize manual signal store.

        Args:
            redis_client: Redis client instance
            account_name: Account name for key prefix
            max_history: Maximum signals to keep in history
            processed_ttl: TTL for processed signal tracking (seconds)
            logger: Optional logger instance
        """
        self.redis_client = redis_client
        self.account_name = account_name
        self._key_prefix = account_name
        self.max_history = max_history
        self.processed_ttl = processed_ttl
        self.logger = logger or logging.getLogger(__name__)

        # Keys
        self._queue_key = f"{self._key_prefix}:manual_signals"
        self._processed_key = f"{self._key_prefix}:manual_signals:processed"
        self._history_key = f"{self._key_prefix}:manual_signals:history"

    def push_signal(self, signal: ManualSignal) -> bool:
        """
        Push a new signal to the queue.

        Args:
            signal: ManualSignal to queue

        Returns:
            True if successfully queued
        """
        try:
            # Check if already processed (prevent duplicates)
            if self._is_processed(signal.signal_id):
                self.logger.warning(
                    f"Signal {signal.signal_id} already processed, skipping"
                )
                return False

            # Push to queue (right side - FIFO)
            self.redis_client.rpush(self._queue_key, signal.to_json())

            self.logger.info(
                f"Queued manual signal: {signal.symbol} {signal.direction} "
                f"from {signal.source} (id: {signal.signal_id})"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to push signal: {e}", exc_info=True)
            return False

    def pop_signal(self) -> Optional[ManualSignal]:
        """
        Pop the next signal from the queue (blocking).

        Returns:
            ManualSignal or None if queue is empty
        """
        try:
            # Pop from left side (oldest first - FIFO)
            signal_json = self.redis_client.lpop(self._queue_key)

            if not signal_json:
                return None

            # Handle bytes if decode_responses=False
            if isinstance(signal_json, bytes):
                signal_json = signal_json.decode('utf-8')

            signal = ManualSignal.from_json(signal_json)

            self.logger.debug(
                f"Popped signal from queue: {signal.symbol} {signal.direction}"
            )
            return signal

        except Exception as e:
            self.logger.error(f"Failed to pop signal: {e}", exc_info=True)
            return None

    def pop_signal_blocking(self, timeout: int = 1) -> Optional[ManualSignal]:
        """
        Pop signal with blocking wait.

        Args:
            timeout: Timeout in seconds (0 = wait forever)

        Returns:
            ManualSignal or None if timeout
        """
        try:
            result = self.redis_client.blpop(self._queue_key, timeout=timeout)

            if not result:
                return None

            _, signal_json = result

            # Handle bytes if decode_responses=False
            if isinstance(signal_json, bytes):
                signal_json = signal_json.decode('utf-8')

            return ManualSignal.from_json(signal_json)

        except Exception as e:
            self.logger.error(f"Failed to pop signal (blocking): {e}")
            return None

    def peek_signals(self, limit: int = 10) -> List[ManualSignal]:
        """
        Peek at pending signals without removing them.

        Args:
            limit: Maximum signals to return

        Returns:
            List of pending ManualSignal objects
        """
        try:
            signals_json = self.redis_client.lrange(self._queue_key, 0, limit - 1)

            signals = []
            for signal_json in signals_json:
                if isinstance(signal_json, bytes):
                    signal_json = signal_json.decode('utf-8')
                signals.append(ManualSignal.from_json(signal_json))

            return signals

        except Exception as e:
            self.logger.error(f"Failed to peek signals: {e}")
            return []

    def get_queue_length(self) -> int:
        """Get number of pending signals in queue."""
        try:
            return self.redis_client.llen(self._queue_key)
        except Exception:
            return 0

    def mark_processed(
        self,
        signal: ManualSignal,
        result: Dict[str, Any]
    ) -> None:
        """
        Mark a signal as processed and add to history.

        Args:
            signal: The processed signal
            result: Processing result (success/failure, trade details)
        """
        try:
            # Mark as processed (with TTL to auto-cleanup)
            self.redis_client.hset(
                self._processed_key,
                signal.signal_id,
                json.dumps({
                    'processed_at': datetime.now().isoformat(),
                    'result': result
                })
            )
            self.redis_client.expire(self._processed_key, self.processed_ttl)

            # Add to history (with metadata)
            history_entry = {
                **signal.to_dict(),
                'processed_at': datetime.now().isoformat(),
                'result': result
            }
            self.redis_client.lpush(self._history_key, json.dumps(history_entry))
            self.redis_client.ltrim(self._history_key, 0, self.max_history - 1)

            self.logger.debug(f"Marked signal {signal.signal_id} as processed")

        except Exception as e:
            self.logger.error(f"Failed to mark signal processed: {e}")

    def _is_processed(self, signal_id: str) -> bool:
        """Check if a signal has already been processed."""
        try:
            return self.redis_client.hexists(self._processed_key, signal_id)
        except Exception:
            return False

    def get_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Get recent signal processing history.

        Args:
            limit: Maximum entries to return

        Returns:
            List of history entries with signal and result data
        """
        try:
            history_json = self.redis_client.lrange(self._history_key, 0, limit - 1)

            history = []
            for entry_json in history_json:
                if isinstance(entry_json, bytes):
                    entry_json = entry_json.decode('utf-8')
                history.append(json.loads(entry_json))

            return history

        except Exception as e:
            self.logger.error(f"Failed to get history: {e}")
            return []

    def clear_queue(self) -> int:
        """
        Clear all pending signals from queue.

        Returns:
            Number of signals cleared
        """
        try:
            length = self.get_queue_length()
            self.redis_client.delete(self._queue_key)
            self.logger.info(f"Cleared {length} signals from queue")
            return length
        except Exception as e:
            self.logger.error(f"Failed to clear queue: {e}")
            return 0
