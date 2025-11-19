"""
Core interfaces and base classes for the risk manager package.
"""

from .interfaces import (
    PositionSizerInterface,
    StopLossCalculatorInterface,
    TakeProfitCalculatorInterface,
    EntryManagerInterface
)

from .base import (
    BasePositionSizer,
    BaseStopLossCalculator,
    BaseTakeProfitCalculator
)

from .exceptions import (
    RiskManagerError,
    InvalidConfigurationError,
    CalculationError,
    ValidationError,
    InsufficientDataError,
    UnsupportedConfigurationError
)

__all__ = [
    # Interfaces
    "PositionSizerInterface",
    "StopLossCalculatorInterface", 
    "TakeProfitCalculatorInterface",
    "EntryManagerInterface",
    
    # Base classes
    "BasePositionSizer",
    "BaseStopLossCalculator",
    "BaseTakeProfitCalculator",
    
    # Exceptions
    "RiskManagerError",
    "InvalidConfigurationError",
    "CalculationError",
    "ValidationError"
]