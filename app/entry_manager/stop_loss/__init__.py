"""
Stop loss implementations for different strategies.
"""

from .fixed import FixedStopLossCalculator
from .indicator import IndicatorStopLossCalculator
from .trailing import TrailingStopLossCalculator
from .factory import StopLossCalculatorFactory, create_stop_loss_calculator

__all__ = [
    "FixedStopLossCalculator",
    "IndicatorStopLossCalculator",
    "TrailingStopLossCalculator",
    "StopLossCalculatorFactory",
    "create_stop_loss_calculator"
]