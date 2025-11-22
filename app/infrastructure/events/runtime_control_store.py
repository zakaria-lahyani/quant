"""
Runtime Control Store - Redis-based runtime controls for trading system.

Provides runtime toggles for:
- Master trading kill switch (stops ALL trading)
- Auto-trading toggle (stops automatic strategies, allows manual)
- Per-strategy toggles
"""

import logging
from datetime import datetime
from typing import Optional, Dict, List
import redis


class RuntimeControlStore:
    """
    Redis-based store for runtime trading controls.

    Redis Keys:
        {account}:controls:trading_enabled      -> "true"/"false" (master kill switch)
        {account}:controls:auto_trading_enabled -> "true"/"false" (auto strategies only)
        {account}:controls:strategies:{name}    -> "true"/"false" (per-strategy)
        {account}:controls:last_updated         -> ISO timestamp
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        account_name: str,
        logger: Optional[logging.Logger] = None
    ):
        self.redis = redis_client
        self.account_name = account_name
        self.logger = logger or logging.getLogger(self.__class__.__name__)

        # Key prefixes
        self._base_key = f"{account_name}:controls"
        self._trading_key = f"{self._base_key}:trading_enabled"
        self._auto_trading_key = f"{self._base_key}:auto_trading_enabled"
        self._strategies_prefix = f"{self._base_key}:strategies"
        self._last_updated_key = f"{self._base_key}:last_updated"

    def _strategy_key(self, strategy_name: str) -> str:
        """Get Redis key for a specific strategy."""
        return f"{self._strategies_prefix}:{strategy_name}"

    def _update_timestamp(self) -> None:
        """Update the last_updated timestamp."""
        self.redis.set(self._last_updated_key, datetime.utcnow().isoformat())

    # ==================== Trading Controls ====================

    def is_trading_enabled(self) -> bool:
        """
        Check if trading is enabled (master kill switch).
        Returns True if key doesn't exist (default: enabled).
        """
        value = self.redis.get(self._trading_key)
        if value is None:
            return True  # Default: enabled
        return value.decode('utf-8').lower() == 'true'

    def set_trading_enabled(self, enabled: bool) -> None:
        """
        Set master trading switch.
        When disabled, ALL trading stops (including manual).
        """
        self.redis.set(self._trading_key, 'true' if enabled else 'false')
        self._update_timestamp()
        self.logger.info(f"Trading {'enabled' if enabled else 'disabled'} for {self.account_name}")

    def is_auto_trading_enabled(self) -> bool:
        """
        Check if automatic trading is enabled.
        Returns True if key doesn't exist (default: enabled).
        When disabled, only manual signals are processed.
        """
        value = self.redis.get(self._auto_trading_key)
        if value is None:
            return True  # Default: enabled
        return value.decode('utf-8').lower() == 'true'

    def set_auto_trading_enabled(self, enabled: bool) -> None:
        """
        Set automatic trading switch.
        When disabled, automatic strategies are blocked but manual signals work.
        """
        self.redis.set(self._auto_trading_key, 'true' if enabled else 'false')
        self._update_timestamp()
        self.logger.info(f"Auto-trading {'enabled' if enabled else 'disabled'} for {self.account_name}")

    # ==================== Strategy Controls ====================

    def is_strategy_enabled(self, strategy_name: str) -> bool:
        """
        Check if a specific strategy is enabled.
        Returns True if key doesn't exist (default: enabled).
        """
        value = self.redis.get(self._strategy_key(strategy_name))
        if value is None:
            return True  # Default: enabled
        return value.decode('utf-8').lower() == 'true'

    def set_strategy_enabled(self, strategy_name: str, enabled: bool) -> None:
        """Enable or disable a specific strategy."""
        self.redis.set(self._strategy_key(strategy_name), 'true' if enabled else 'false')
        self._update_timestamp()
        self.logger.info(f"Strategy '{strategy_name}' {'enabled' if enabled else 'disabled'} for {self.account_name}")

    def get_all_strategy_states(self) -> Dict[str, bool]:
        """Get enabled/disabled state for all configured strategies."""
        pattern = f"{self._strategies_prefix}:*"
        keys = self.redis.keys(pattern)

        states = {}
        for key in keys:
            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
            strategy_name = key_str.split(':')[-1]
            value = self.redis.get(key)
            states[strategy_name] = value.decode('utf-8').lower() == 'true' if value else True

        return states

    # ==================== Combined Check ====================

    def can_execute_trade(self, strategy_name: str, is_manual: bool = False) -> tuple[bool, str]:
        """
        Check if a trade can be executed based on all controls.

        Args:
            strategy_name: Name of the strategy
            is_manual: True if this is a manual signal

        Returns:
            Tuple of (can_execute, reason)
        """
        # Check master kill switch
        if not self.is_trading_enabled():
            return False, "Trading is disabled (master switch)"

        # For automatic strategies, check auto_trading_enabled
        if not is_manual and not self.is_auto_trading_enabled():
            return False, "Auto-trading is disabled (manual signals only)"

        # Check strategy-specific toggle
        if not self.is_strategy_enabled(strategy_name):
            return False, f"Strategy '{strategy_name}' is disabled"

        return True, "OK"

    # ==================== Status & Info ====================

    def get_status(self) -> Dict:
        """Get complete status of all controls."""
        last_updated = self.redis.get(self._last_updated_key)

        return {
            'account': self.account_name,
            'trading_enabled': self.is_trading_enabled(),
            'auto_trading_enabled': self.is_auto_trading_enabled(),
            'strategies': self.get_all_strategy_states(),
            'last_updated': last_updated.decode('utf-8') if last_updated else None
        }

    def initialize_defaults(self, strategy_names: List[str] = None) -> None:
        """
        Initialize controls with default values if they don't exist.
        Useful for first-time setup.
        """
        # Only set if not already set
        if self.redis.get(self._trading_key) is None:
            self.redis.set(self._trading_key, 'true')

        if self.redis.get(self._auto_trading_key) is None:
            self.redis.set(self._auto_trading_key, 'true')

        # Initialize strategy states
        if strategy_names:
            for name in strategy_names:
                if self.redis.get(self._strategy_key(name)) is None:
                    self.redis.set(self._strategy_key(name), 'true')

        self._update_timestamp()
        self.logger.info(f"Initialized default controls for {self.account_name}")

    def reset_all(self) -> None:
        """Reset all controls to enabled (default state)."""
        self.set_trading_enabled(True)
        self.set_auto_trading_enabled(True)

        # Reset all strategy toggles
        for strategy_name in self.get_all_strategy_states().keys():
            self.set_strategy_enabled(strategy_name, True)

        self.logger.info(f"Reset all controls to defaults for {self.account_name}")
