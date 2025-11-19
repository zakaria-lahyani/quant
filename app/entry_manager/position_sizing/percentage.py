"""
Percentage-based position sizing implementation.
"""

from typing import Optional
import logging

from ..core.base import BasePositionSizer
from ..core.exceptions import ValidationError, CalculationError
from ...strategy_builder.core.domain.models import PositionSizing
from ...strategy_builder.core.domain.enums import PositionSizingTypeEnum


class PercentagePositionSizer(BasePositionSizer):
    """Position sizer that uses a percentage of account balance."""
    
    def __init__(self, config: PositionSizing, logger: Optional[logging.Logger] = None):
        super().__init__(config, logger)
        
        if config.type != PositionSizingTypeEnum.PERCENTAGE:
            raise ValidationError(
                f"Expected PERCENTAGE position sizing type, got {config.type}",
                field_name="type"
            )
        
        if config.value > 100:
            raise ValidationError(
                "Percentage value cannot exceed 100%",
                field_name="value"
            )
    
    def calculate_position_size(
        self,
        entry_price: float,
        account_balance: Optional[float] = None,
        volatility: Optional[float] = None,
        **kwargs
    ) -> float:
        """
        Calculate percentage-based position size.
        
        Args:
            entry_price: The entry price for the trade
            account_balance: Total account balance (required)
            volatility: Not used for percentage sizing
            **kwargs: Additional parameters (ignored)
            
        Returns:
            Position size in base currency units
            
        Raises:
            CalculationError: If account_balance is not provided
        """
        self._validate_inputs(entry_price, account_balance, volatility)
        
        if account_balance is None:
            raise CalculationError(
                "Account balance is required for percentage-based position sizing",
                calculation_type="percentage"
            )
        
        percentage_decimal = self.config.value / 100.0
        position_size = account_balance * percentage_decimal
        
        self.logger.debug(
            f"Percentage position sizing: balance={account_balance}, "
            f"percentage={self.config.value}%, size={position_size}"
        )
        
        return position_size
    
    def get_position_units(
        self,
        entry_price: float,
        account_balance: Optional[float] = None,
        **kwargs
    ) -> float:
        """
        Calculate the number of units/shares for the position.
        
        Args:
            entry_price: The entry price for the trade
            account_balance: Total account balance (required)
            **kwargs: Additional parameters
            
        Returns:
            Number of units/shares
        """
        position_size = self.calculate_position_size(
            entry_price, account_balance, **kwargs
        )
        units = position_size / entry_price
        
        self.logger.debug(
            f"Percentage position units: size={position_size}, "
            f"entry_price={entry_price}, units={units}"
        )
        
        return units
    
    def get_max_risk_amount(self, account_balance: float) -> float:
        """
        Get the maximum risk amount based on percentage of balance.
        
        Args:
            account_balance: Total account balance
            
        Returns:
            Maximum risk amount in base currency
        """
        return account_balance * (self.config.value / 100.0)