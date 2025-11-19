"""
Volatility-based position sizing implementation.
"""

from typing import Optional
import logging

from ..core.base import BasePositionSizer
from ..core.exceptions import ValidationError, CalculationError
from ...strategy_builder.core.domain.models import PositionSizing
from ...strategy_builder.core.domain.enums import PositionSizingTypeEnum


class VolatilityPositionSizer(BasePositionSizer):
    """Position sizer that adjusts size based on market volatility."""
    
    def __init__(self, config: PositionSizing, logger: Optional[logging.Logger] = None):
        super().__init__(config, logger)
        
        if config.type != PositionSizingTypeEnum.VOLATILITY:
            raise ValidationError(
                f"Expected VOLATILITY position sizing type, got {config.type}",
                field_name="type"
            )
    
    def calculate_position_size(
        self,
        entry_price: float,
        account_balance: Optional[float] = None,
        volatility: Optional[float] = None,
        risk_per_trade: Optional[float] = None,
        **kwargs
    ) -> float:
        """
        Calculate volatility-adjusted position size.
        
        The formula used is: Position Size = (Risk Amount) / (Volatility * Multiplier)
        Where:
        - Risk Amount = account_balance * (config.value / 100) if account_balance provided
        - Volatility = ATR or other volatility measure
        - Multiplier = volatility adjustment factor
        
        Args:
            entry_price: The entry price for the trade
            account_balance: Total account balance
            volatility: Market volatility (e.g., ATR) - required
            risk_per_trade: Fixed risk amount (alternative to account_balance)
            **kwargs: Additional parameters
            
        Returns:
            Position size in base currency units
            
        Raises:
            CalculationError: If volatility is not provided or invalid
        """
        self._validate_inputs(entry_price, account_balance, volatility)
        
        if volatility is None or volatility <= 0:
            raise CalculationError(
                "Valid volatility value is required for volatility-based position sizing",
                calculation_type="volatility"
            )
        
        # Determine risk amount
        if risk_per_trade is not None:
            risk_amount = risk_per_trade
        elif account_balance is not None:
            # Use config.value as percentage of account balance for risk
            risk_percentage = self.config.value / 100.0
            risk_amount = account_balance * risk_percentage
        else:
            raise CalculationError(
                "Either account_balance or risk_per_trade must be provided",
                calculation_type="volatility"
            )
        
        # Calculate position size based on volatility
        # Higher volatility = smaller position size
        volatility_multiplier = kwargs.get('volatility_multiplier', 2.0)
        position_size = risk_amount / (volatility * volatility_multiplier)
        
        # Apply minimum and maximum limits if specified
        min_size = kwargs.get('min_position_size', 0)
        max_size = kwargs.get('max_position_size', float('inf'))
        
        position_size = max(min_size, min(position_size, max_size))
        
        self.logger.debug(
            f"Volatility position sizing: risk_amount={risk_amount}, "
            f"volatility={volatility}, multiplier={volatility_multiplier}, "
            f"size={position_size}"
        )
        
        return position_size
    
    def get_position_units(
        self,
        entry_price: float,
        account_balance: Optional[float] = None,
        volatility: Optional[float] = None,
        **kwargs
    ) -> float:
        """
        Calculate the number of units/shares for the position.
        
        Args:
            entry_price: The entry price for the trade
            account_balance: Total account balance
            volatility: Market volatility (required)
            **kwargs: Additional parameters
            
        Returns:
            Number of units/shares
        """
        position_size = self.calculate_position_size(
            entry_price, account_balance, volatility, **kwargs
        )
        units = position_size / entry_price
        
        self.logger.debug(
            f"Volatility position units: size={position_size}, "
            f"entry_price={entry_price}, units={units}"
        )
        
        return units
    
    def calculate_kelly_criterion_size(
        self,
        win_probability: float,
        avg_win: float,
        avg_loss: float,
        account_balance: float,
        **kwargs
    ) -> float:
        """
        Calculate position size using Kelly Criterion for volatility-based sizing.
        
        Kelly % = (bp - q) / b
        Where:
        - b = odds received (avg_win / avg_loss)
        - p = probability of winning
        - q = probability of losing (1 - p)
        
        Args:
            win_probability: Probability of winning (0-1)
            avg_win: Average winning amount
            avg_loss: Average losing amount
            account_balance: Total account balance
            **kwargs: Additional parameters
            
        Returns:
            Kelly-optimized position size
        """
        if not (0 < win_probability < 1):
            raise ValidationError("Win probability must be between 0 and 1")
        
        if avg_win <= 0 or avg_loss <= 0:
            raise ValidationError("Average win and loss must be positive")
        
        # Calculate Kelly percentage
        b = avg_win / avg_loss  # odds
        p = win_probability
        q = 1 - p
        
        kelly_percentage = (b * p - q) / b
        
        # Apply Kelly fraction (typically 25-50% of full Kelly)
        kelly_fraction = kwargs.get('kelly_fraction', 0.25)
        adjusted_kelly = kelly_percentage * kelly_fraction
        
        # Ensure non-negative and reasonable limits
        adjusted_kelly = max(0, min(adjusted_kelly, 0.2))  # Max 20% of balance
        
        position_size = account_balance * adjusted_kelly
        
        self.logger.debug(
            f"Kelly criterion sizing: kelly%={kelly_percentage:.3f}, "
            f"adjusted={adjusted_kelly:.3f}, size={position_size}"
        )
        
        return position_size