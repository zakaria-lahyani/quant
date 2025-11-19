"""
Fixed position sizing implementation.
"""

from typing import Optional
import logging

from ..core.base import BasePositionSizer
from ..core.exceptions import ValidationError
from ...strategy_builder.core.domain.models import PositionSizing
from ...strategy_builder.core.domain.enums import PositionSizingTypeEnum


class FixedPositionSizer(BasePositionSizer):
    """Position sizer that uses a fixed dollar amount."""
    
    def __init__(self, config: PositionSizing, logger: Optional[logging.Logger] = None):
        super().__init__(config, logger)
        
        if config.type != PositionSizingTypeEnum.FIXED:
            raise ValidationError(
                f"Expected FIXED position sizing type, got {config.type}",
                field_name="type"
            )
    
    def calculate_position_size(
        self,
        entry_price: float,
        account_balance: Optional[float] = None,
        volatility: Optional[float] = None,
        **kwargs
    ) -> float:
        """
        Calculate fixed position size.
        
        Args:
            entry_price: The entry price for the trade
            account_balance: Not used for fixed sizing
            volatility: Not used for fixed sizing
            **kwargs: Additional parameters (ignored)
            
        Returns:
            Fixed position size in base currency units
        """
        self._validate_inputs(entry_price, account_balance, volatility)
        
        position_size = self.config.value
        
        self.logger.debug(
            f"Fixed position sizing: size={position_size}, "
            f"entry_price={entry_price}"
        )
        
        return position_size
    
    def get_position_units(self, entry_price: float, **kwargs) -> float:
        """
        Calculate the number of units/shares for the position.
        
        Args:
            entry_price: The entry price for the trade
            **kwargs: Additional parameters
            
        Returns:
            Number of units/shares
        """
        # For fixed position sizing, the value already represents lots/units
        # No need to divide by entry price
        units = self.calculate_position_size(entry_price, **kwargs)
        
        self.logger.debug(
            f"Fixed position units: {units} lots, "
            f"entry_price={entry_price}"
        )
        
        return units