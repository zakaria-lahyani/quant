"""
Factory for creating stop loss calculator instances.
"""

from typing import Optional, Dict, Type, Union
import logging

from ..core.interfaces import StopLossCalculatorInterface
from ..core.exceptions import UnsupportedConfigurationError
from ...strategy_builder.core.domain.models import FixedStopLoss, IndicatorBasedSlTp, TrailingStopLossOnly, MonetaryStopLoss

from .fixed import FixedStopLossCalculator
from .indicator import IndicatorStopLossCalculator
from .trailing import TrailingStopLossCalculator
from .monetary import MonetaryStopLossCalculator


class StopLossCalculatorFactory:
    """Factory for creating stop loss calculator instances based on configuration."""
    
    _CALCULATOR_REGISTRY: Dict[str, Type[StopLossCalculatorInterface]] = {
        "fixed": FixedStopLossCalculator,
        "indicator": IndicatorStopLossCalculator,
        "trailing": TrailingStopLossCalculator,
        "monetary": MonetaryStopLossCalculator,
    }
    
    @classmethod
    def create_calculator(
        cls,
        config: Union[FixedStopLoss, IndicatorBasedSlTp, TrailingStopLossOnly, MonetaryStopLoss],
        pip_value: float,
        logger: Optional[logging.Logger] = None
    ) -> StopLossCalculatorInterface:
        """
        Create a stop loss calculator based on the configuration type.
        
        Args:
            config: Stop loss configuration
            logger: Optional logger instance
            pip_value: Value used to convert pips to price distance (default: 10000.0 for forex)
            
        Returns:
            Appropriate stop loss calculator implementation
            
        Raises:
            UnsupportedConfigurationError: If the stop loss type is not supported
        """
        # Extract type from config
        if hasattr(config, 'type'):
            stop_type = config.type
        else:
            raise UnsupportedConfigurationError(
                "Stop loss configuration must have a 'type' field",
                config_type="unknown"
            )
        
        if stop_type not in cls._CALCULATOR_REGISTRY:
            raise UnsupportedConfigurationError(
                f"Unsupported stop loss type: {stop_type}",
                config_type=str(stop_type)
            )
        
        calculator_class = cls._CALCULATOR_REGISTRY[stop_type]
        return calculator_class(config, pip_value, logger)
    
    @classmethod
    def create_from_dict(
        cls,
        config_dict: Dict,
        pip_value: float,
        logger: Optional[logging.Logger] = None
    ) -> StopLossCalculatorInterface:
        """
        Create a stop loss calculator from a dictionary configuration.
        
        Args:
            config_dict: Dictionary containing stop loss configuration
            logger: Optional logger instance
            pip_value: Value used to convert pips to price distance (default: 10000.0 for forex)
            
        Returns:
            Appropriate stop loss calculator implementation
        """
        if 'type' not in config_dict:
            raise UnsupportedConfigurationError(
                "Stop loss configuration must have a 'type' field",
                config_type="unknown"
            )
        
        stop_type = config_dict['type']
        
        # Create appropriate model based on type
        if stop_type == "fixed":
            config = FixedStopLoss(**config_dict)
        elif stop_type == "indicator":
            config = IndicatorBasedSlTp(**config_dict)
        elif stop_type == "trailing":
            config = TrailingStopLossOnly(**config_dict)
        elif stop_type == "monetary":
            config = MonetaryStopLoss(**config_dict)
        else:
            raise UnsupportedConfigurationError(
                f"Unsupported stop loss type: {stop_type}",
                config_type=str(stop_type)
            )
        
        return cls.create_calculator(config, pip_value, logger)
    
    @classmethod
    def register_calculator(
        cls,
        stop_type: str,
        calculator_class: Type[StopLossCalculatorInterface]
    ) -> None:
        """
        Register a new stop loss calculator implementation.
        
        Args:
            stop_type: The stop loss type string
            calculator_class: The calculator implementation class
        """
        cls._CALCULATOR_REGISTRY[stop_type] = calculator_class
    
    @classmethod
    def get_supported_types(cls) -> list[str]:
        """
        Get list of supported stop loss types.
        
        Returns:
            List of supported stop loss types
        """
        return list(cls._CALCULATOR_REGISTRY.keys())
    
    @classmethod
    def is_supported(cls, stop_type: str) -> bool:
        """
        Check if a stop loss type is supported.
        
        Args:
            stop_type: The stop loss type to check
            
        Returns:
            True if supported, False otherwise
        """
        return stop_type in cls._CALCULATOR_REGISTRY


def create_stop_loss_calculator(
    config: Union[FixedStopLoss, IndicatorBasedSlTp, TrailingStopLossOnly, MonetaryStopLoss, Dict],
    pip_value: float,
    logger: Optional[logging.Logger] = None
) -> StopLossCalculatorInterface:
    """
    Convenience function to create a stop loss calculator.
    
    Args:
        config: Stop loss configuration (model or dict)
        logger: Optional logger instance
        pip_value: Value used to convert pips to price distance (default: 10000.0 for forex)
        
    Returns:
        Appropriate stop loss calculator implementation
    """
    if isinstance(config, dict):
        return StopLossCalculatorFactory.create_from_dict(config, pip_value, logger)
    else:
        return StopLossCalculatorFactory.create_calculator(config, pip_value, logger)