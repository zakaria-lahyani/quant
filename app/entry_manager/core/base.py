"""
Base classes for risk manager components.
"""

from abc import ABC
from typing import Optional, Dict, Any, Union
import logging

from .interfaces import PositionSizerInterface, StopLossCalculatorInterface, TakeProfitCalculatorInterface
from .exceptions import ValidationError, InvalidConfigurationError
from ...strategy_builder.core.domain.models import PositionSizing, StopLoss, TakeProfit


class BasePositionSizer(PositionSizerInterface, ABC):
    """Base class for position sizers with common validation."""
    
    def __init__(self, config: PositionSizing, logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate the position sizing configuration."""
        if not isinstance(self.config, PositionSizing):
            raise InvalidConfigurationError(
                "Invalid position sizing configuration type",
                config_type="PositionSizing"
            )
        
        if self.config.value <= 0:
            raise ValidationError(
                "Position sizing value must be positive",
                field_name="value"
            )
    
    def _validate_inputs(
        self,
        entry_price: float,
        account_balance: Optional[float] = None,
        volatility: Optional[float] = None
    ) -> None:
        """Validate common inputs for position sizing."""
        if entry_price <= 0:
            raise ValidationError("Entry price must be positive", field_name="entry_price")
        
        if account_balance is not None and account_balance <= 0:
            raise ValidationError("Account balance must be positive", field_name="account_balance")
        
        if volatility is not None and volatility < 0:
            raise ValidationError("Volatility cannot be negative", field_name="volatility")


class BaseStopLossCalculator(StopLossCalculatorInterface, ABC):
    """Base class for stop loss calculators with common validation."""
    
    def __init__(self, config: StopLoss, logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate the stop loss configuration."""
        if not isinstance(self.config, (dict, object)):
            raise InvalidConfigurationError(
                "Invalid stop loss configuration type",
                config_type="StopLoss"
            )
    
    def _validate_inputs(
        self,
        entry_price: float,
        is_long: bool,
        market_data: Optional[Dict[str, Any]] = None
    ) -> None:
        """Validate common inputs for stop loss calculation."""
        if entry_price <= 0:
            raise ValidationError("Entry price must be positive", field_name="entry_price")
        
        if not isinstance(is_long, bool):
            raise ValidationError("is_long must be a boolean", field_name="is_long")
    
    def _calculate_stop_level(
        self,
        entry_price: float,
        stop_distance: float,
        is_long: bool
    ) -> float:
        """Calculate stop loss level based on distance and direction."""
        if is_long:
            return entry_price - abs(stop_distance)
        else:
            return entry_price + abs(stop_distance)


class BaseTakeProfitCalculator(TakeProfitCalculatorInterface, ABC):
    """Base class for take profit calculators with common validation."""
    
    def __init__(self, config: TakeProfit, logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate the take profit configuration."""
        if not isinstance(self.config, (dict, object)):
            raise InvalidConfigurationError(
                "Invalid take profit configuration type",
                config_type="TakeProfit"
            )
    
    def _validate_inputs(
        self,
        entry_price: float,
        is_long: bool,
        market_data: Optional[Dict[str, Any]] = None
    ) -> None:
        """Validate common inputs for take profit calculation."""
        if entry_price <= 0:
            raise ValidationError("Entry price must be positive", field_name="entry_price")
        
        if not isinstance(is_long, bool):
            raise ValidationError("is_long must be a boolean", field_name="is_long")
    
    def _calculate_profit_level(
        self,
        entry_price: float,
        profit_distance: float,
        is_long: bool
    ) -> float:
        """Calculate take profit level based on distance and direction."""
        if is_long:
            return entry_price + abs(profit_distance)
        else:
            return entry_price - abs(profit_distance)


class BaseRiskCalculator:
    """Base class with common risk calculation utilities."""
    
    @staticmethod
    def calculate_percentage_distance(price: float, percentage: float) -> float:
        """Calculate distance based on percentage of price."""
        return price * (percentage / 100.0)
    
    @staticmethod
    def calculate_risk_reward_ratio(
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        is_long: bool
    ) -> float:
        """Calculate risk-reward ratio for a trade."""
        if is_long:
            risk = entry_price - stop_loss
            reward = take_profit - entry_price
        else:
            risk = stop_loss - entry_price
            reward = entry_price - take_profit
        
        if risk <= 0:
            return float('inf')
        
        return reward / risk
    
    @staticmethod
    def validate_price_levels(
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        is_long: bool
    ) -> bool:
        """Validate that price levels make sense for the trade direction."""
        if is_long:
            return stop_loss < entry_price < take_profit
        else:
            return take_profit < entry_price < stop_loss