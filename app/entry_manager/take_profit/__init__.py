"""
Take profit implementations for different strategies.
"""

from .fixed import FixedTakeProfitCalculator
from .multi_target import MultiTargetTakeProfitCalculator
from .factory import TakeProfitCalculatorFactory, create_take_profit_calculator

__all__ = [
    "FixedTakeProfitCalculator",
    "MultiTargetTakeProfitCalculator",
    "TakeProfitCalculatorFactory",
    "create_take_profit_calculator"
]