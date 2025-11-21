"""
Redis storage for indicator data and recent rows.

This module provides real-time storage of computed indicators and recent market data
in Redis for monitoring and debugging purposes.
"""

import logging
import json
from typing import Optional, Dict, Any, List
from datetime import datetime
import pandas as pd

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


class IndicatorRedisStore:
    """
    Stores indicator data and recent rows in Redis for real-time monitoring.

    Data Structure:
    - Hash: indicators:{symbol}:{timeframe}:latest - Latest indicator values
    - List: indicators:{symbol}:{timeframe}:recent - Recent rows (configurable limit)
    - Hash: indicators:{symbol}:metadata - Metadata about last update
    """

    # Key for storing account metadata in each DB
    ACCOUNT_META_KEY = "__account_meta__"

    def __init__(
        self,
        redis_client: redis.Redis,
        symbol: str,
        max_recent_rows: int = 50,
        ttl_seconds: int = 3600,
        account_name: Optional[str] = None,
        account_tag: Optional[str] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize indicator Redis store.

        Args:
            redis_client: Redis client instance
            symbol: Trading symbol (e.g., BTCUSD)
            max_recent_rows: Maximum number of recent rows to keep per timeframe
            ttl_seconds: Time-to-live for stored data in seconds (default: 1 hour)
            account_name: Account/broker name for this Redis DB (e.g., "ACG", "FTMO")
            account_tag: Account tag identifier
            logger: Optional logger instance
        """
        self.redis_client = redis_client
        self.symbol = symbol.upper()
        self.max_recent_rows = max_recent_rows
        self.ttl_seconds = ttl_seconds
        self.account_name = account_name
        self.account_tag = account_tag
        self.logger = logger or logging.getLogger(__name__)

        # Store account metadata in this DB if provided
        if account_name or account_tag:
            self._store_account_metadata()

    def store_indicator_row(
        self,
        timeframe: str,
        row: pd.Series,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Store a row with computed indicators in Redis.

        Args:
            timeframe: Timeframe (e.g., '1', '5', '15')
            row: Pandas Series containing indicator values
            metadata: Optional metadata about the computation
        """
        try:
            # Convert row to dictionary, handling numpy types
            row_dict = self._series_to_dict(row)

            # Add timestamp if not present
            if 'time' not in row_dict:
                row_dict['time'] = datetime.now().isoformat()

            # Store latest indicators in a hash
            latest_key = f"indicators:{self.symbol}:{timeframe}:latest"
            self.redis_client.hset(
                latest_key,
                mapping=self._flatten_dict(row_dict)
            )
            self.redis_client.expire(latest_key, self.ttl_seconds)

            # Store in recent rows list (FIFO with max length)
            recent_key = f"indicators:{self.symbol}:{timeframe}:recent"
            row_json = json.dumps(row_dict, default=str)
            self.redis_client.lpush(recent_key, row_json)
            self.redis_client.ltrim(recent_key, 0, self.max_recent_rows - 1)
            self.redis_client.expire(recent_key, self.ttl_seconds)

            # Update metadata
            if metadata:
                self._update_metadata(timeframe, metadata)

            self.logger.debug(
                f"Stored indicators for {self.symbol} {timeframe}: "
                f"{len(row_dict)} fields"
            )

        except Exception as e:
            self.logger.error(
                f"Failed to store indicators for {self.symbol} {timeframe}: {e}",
                exc_info=True
            )

    def store_recent_rows(
        self,
        timeframe: str,
        recent_rows: List[pd.Series]
    ) -> None:
        """
        Store multiple recent rows at once (useful for initialization).

        Args:
            timeframe: Timeframe (e.g., '1', '5', '15')
            recent_rows: List of recent rows with indicators
        """
        try:
            recent_key = f"indicators:{self.symbol}:{timeframe}:recent"

            # Clear existing data
            self.redis_client.delete(recent_key)

            # Store each row
            for row in recent_rows[-self.max_recent_rows:]:
                row_dict = self._series_to_dict(row)
                row_json = json.dumps(row_dict, default=str)
                self.redis_client.rpush(recent_key, row_json)

            self.redis_client.expire(recent_key, self.ttl_seconds)

            self.logger.info(
                f"Stored {len(recent_rows)} recent rows for {self.symbol} {timeframe}"
            )

        except Exception as e:
            self.logger.error(
                f"Failed to store recent rows for {self.symbol} {timeframe}: {e}",
                exc_info=True
            )

    def get_latest_indicators(self, timeframe: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve latest indicator values for a timeframe.

        Args:
            timeframe: Timeframe to retrieve

        Returns:
            Dictionary of indicator values or None if not found
        """
        try:
            latest_key = f"indicators:{self.symbol}:{timeframe}:latest"
            data = self.redis_client.hgetall(latest_key)

            if not data:
                return None

            # Convert bytes to strings and parse JSON values
            return {
                k.decode('utf-8'): self._parse_value(v.decode('utf-8'))
                for k, v in data.items()
            }

        except Exception as e:
            self.logger.error(
                f"Failed to get latest indicators for {self.symbol} {timeframe}: {e}"
            )
            return None

    def get_recent_rows(
        self,
        timeframe: str,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve recent rows with indicators.

        Args:
            timeframe: Timeframe to retrieve
            limit: Optional limit on number of rows (default: all available)

        Returns:
            List of dictionaries containing indicator values
        """
        try:
            recent_key = f"indicators:{self.symbol}:{timeframe}:recent"

            if limit is None:
                limit = self.max_recent_rows

            # Get recent rows (0 = oldest, -1 = newest)
            rows_json = self.redis_client.lrange(recent_key, 0, limit - 1)

            # Parse JSON strings
            rows = []
            for row_json in rows_json:
                try:
                    row_dict = json.loads(row_json.decode('utf-8'))
                    rows.append(row_dict)
                except json.JSONDecodeError as e:
                    self.logger.warning(f"Failed to parse row JSON: {e}")
                    continue

            return rows

        except Exception as e:
            self.logger.error(
                f"Failed to get recent rows for {self.symbol} {timeframe}: {e}"
            )
            return []

    def get_metadata(self, timeframe: Optional[str] = None) -> Dict[str, Any]:
        """
        Get metadata about stored indicators.

        Args:
            timeframe: Optional specific timeframe, or None for all

        Returns:
            Dictionary containing metadata
        """
        try:
            if timeframe:
                meta_key = f"indicators:{self.symbol}:{timeframe}:metadata"
                data = self.redis_client.hgetall(meta_key)
                return {
                    k.decode('utf-8'): v.decode('utf-8')
                    for k, v in data.items()
                }
            else:
                meta_key = f"indicators:{self.symbol}:metadata"
                data = self.redis_client.hgetall(meta_key)
                return {
                    k.decode('utf-8'): v.decode('utf-8')
                    for k, v in data.items()
                }

        except Exception as e:
            self.logger.error(f"Failed to get metadata: {e}")
            return {}

    def clear_data(self, timeframe: Optional[str] = None) -> None:
        """
        Clear stored indicator data.

        Args:
            timeframe: Optional specific timeframe, or None to clear all
        """
        try:
            if timeframe:
                # Clear specific timeframe
                keys = [
                    f"indicators:{self.symbol}:{timeframe}:latest",
                    f"indicators:{self.symbol}:{timeframe}:recent",
                    f"indicators:{self.symbol}:{timeframe}:metadata"
                ]
            else:
                # Clear all timeframes for this symbol
                pattern = f"indicators:{self.symbol}:*"
                keys = list(self.redis_client.scan_iter(match=pattern))

            if keys:
                self.redis_client.delete(*keys)
                self.logger.info(
                    f"Cleared {len(keys)} keys for {self.symbol}" +
                    (f" {timeframe}" if timeframe else "")
                )

        except Exception as e:
            self.logger.error(f"Failed to clear data: {e}")

    def _series_to_dict(self, row: pd.Series) -> Dict[str, Any]:
        """Convert pandas Series to dictionary with JSON-serializable values."""
        row_dict = {}
        for key, value in row.items():
            # Handle pandas/numpy types
            if pd.isna(value):
                row_dict[key] = None
            elif hasattr(value, 'item'):  # numpy types
                row_dict[key] = value.item()
            elif isinstance(value, (pd.Timestamp, datetime)):
                row_dict[key] = value.isoformat()
            else:
                row_dict[key] = value
        return row_dict

    def _flatten_dict(self, d: Dict[str, Any]) -> Dict[str, str]:
        """Flatten dictionary values to strings for Redis hash storage."""
        return {
            str(k): json.dumps(v, default=str)
            for k, v in d.items()
        }

    def _parse_value(self, value_str: str) -> Any:
        """Parse a JSON string value."""
        try:
            return json.loads(value_str)
        except json.JSONDecodeError:
            return value_str

    def _update_metadata(self, timeframe: str, metadata: Dict[str, Any]) -> None:
        """Update metadata for a timeframe."""
        try:
            meta_key = f"indicators:{self.symbol}:{timeframe}:metadata"
            metadata_flat = {
                'last_update': datetime.now().isoformat(),
                **{k: str(v) for k, v in metadata.items()}
            }
            self.redis_client.hset(meta_key, mapping=metadata_flat)
            self.redis_client.expire(meta_key, self.ttl_seconds)

            # Also update symbol-level metadata
            symbol_meta_key = f"indicators:{self.symbol}:metadata"
            self.redis_client.hset(
                symbol_meta_key,
                f"{timeframe}_last_update",
                datetime.now().isoformat()
            )
            self.redis_client.expire(symbol_meta_key, self.ttl_seconds)

        except Exception as e:
            self.logger.error(f"Failed to update metadata: {e}")

    def _store_account_metadata(self) -> None:
        """Store account metadata in this Redis DB for identification."""
        try:
            metadata = {
                'account_name': self.account_name or 'Unknown',
                'account_tag': self.account_tag or 'Unknown',
                'created_at': datetime.now().isoformat(),
                'last_active': datetime.now().isoformat()
            }
            self.redis_client.hset(self.ACCOUNT_META_KEY, mapping=metadata)
            # No TTL - account metadata should persist
            self.logger.debug(
                f"Stored account metadata: {self.account_name} ({self.account_tag})"
            )
        except Exception as e:
            self.logger.warning(f"Failed to store account metadata: {e}")

    @classmethod
    def get_account_info(cls, redis_client: redis.Redis) -> Dict[str, str]:
        """
        Get account information stored in this Redis DB.

        Args:
            redis_client: Redis client instance

        Returns:
            Dictionary with account_name, account_tag, etc.
        """
        try:
            data = redis_client.hgetall(cls.ACCOUNT_META_KEY)
            if data:
                return {
                    k.decode('utf-8'): v.decode('utf-8')
                    for k, v in data.items()
                }
        except Exception:
            pass
        return {'account_name': 'Unknown', 'account_tag': 'Unknown'}

    def update_last_active(self) -> None:
        """Update the last_active timestamp in account metadata."""
        try:
            self.redis_client.hset(
                self.ACCOUNT_META_KEY,
                'last_active',
                datetime.now().isoformat()
            )
        except Exception:
            pass
