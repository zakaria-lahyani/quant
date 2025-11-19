"""
Factory for creating take profit calculator instances.
"""

from typing import Optional, Dict, Type, Union
import logging

from ..core.interfaces import TakeProfitCalculatorInterface
from ..core.exceptions import UnsupportedConfigurationError
from ...strategy_builder.core.domain.models import FixedTakeProfit, MultiTargetTakeProfit, IndicatorBasedSlTp

from .fixed import FixedTakeProfitCalculator
from .multi_target import MultiTargetTakeProfitCalculator


class TakeProfitCalculatorFactory:
    """Factory for creating take profit calculator instances based on configuration."""
    
    _CALCULATOR_REGISTRY: Dict[str, Type[TakeProfitCalculatorInterface]] = {
        "fixed": FixedTakeProfitCalculator,
        "multi_target": MultiTargetTakeProfitCalculator,
        # Note: indicator-based TP would use the same calculator as indicator-based SL
    }
    
    @classmethod
    def create_calculator(
        cls,
        config: Union[FixedTakeProfit, MultiTargetTakeProfit, IndicatorBasedSlTp],
        pip_value: float,
        logger: Optional[logging.Logger] = None
    ) -> TakeProfitCalculatorInterface:
        """
        Create a take profit calculator based on the configuration type.
        
        Args:
            config: Take profit configuration
            logger: Optional logger instance
            pip_value: Value used to convert pips to price distance (default: 10000.0 for forex)
            
        Returns:
            Appropriate take profit calculator implementation
            
        Raises:
            UnsupportedConfigurationError: If the take profit type is not supported
        """
        # Extract type from config
        if hasattr(config, 'type'):
            tp_type = config.type
        else:
            raise UnsupportedConfigurationError(
                "Take profit configuration must have a 'type' field",
                config_type="unknown"
            )
        
        if tp_type not in cls._CALCULATOR_REGISTRY:
            raise UnsupportedConfigurationError(
                f"Unsupported take profit type: {tp_type}",
                config_type=str(tp_type)
            )
        
        calculator_class = cls._CALCULATOR_REGISTRY[tp_type]
        return calculator_class(config, pip_value, logger)
    
    @classmethod
    def create_from_dict(
        cls,
        config_dict: Dict,
        pip_value: float,
        logger: Optional[logging.Logger] = None
    ) -> TakeProfitCalculatorInterface:
        """
        Create a take profit calculator from a dictionary configuration.
        
        Args:
            config_dict: Dictionary containing take profit configuration
            logger: Optional logger instance
            pip_value: Value used to convert pips to price distance (default: 10000.0 for forex)
            
        Returns:
            Appropriate take profit calculator implementation
        """
        if 'type' not in config_dict:
            raise UnsupportedConfigurationError(
                "Take profit configuration must have a 'type' field",
                config_type="unknown"
            )
        
        tp_type = config_dict['type']
        
        # Create appropriate model based on type
        if tp_type == "fixed":
            config = FixedTakeProfit(**config_dict)
        elif tp_type == "multi_target":
            config = MultiTargetTakeProfit(**config_dict)
        elif tp_type == "indicator":
            config = IndicatorBasedSlTp(**config_dict)
        else:
            raise UnsupportedConfigurationError(
                f"Unsupported take profit type: {tp_type}",
                config_type=str(tp_type)
            )
        
        return cls.create_calculator(config, pip_value, logger)
    
    @classmethod
    def register_calculator(
        cls,
        tp_type: str,
        calculator_class: Type[TakeProfitCalculatorInterface]
    ) -> None:
        """
        Register a new take profit calculator implementation.
        
        Args:
            tp_type: The take profit type string
            calculator_class: The calculator implementation class
        """
        cls._CALCULATOR_REGISTRY[tp_type] = calculator_class
    
    @classmethod
    def get_supported_types(cls) -> list[str]:
        """
        Get list of supported take profit types.
        
        Returns:
            List of supported take profit types
        """
        return list(cls._CALCULATOR_REGISTRY.keys())
    
    @classmethod
    def is_supported(cls, tp_type: str) -> bool:
        """
        Check if a take profit type is supported.
        
        Args:
            tp_type: The take profit type to check
            
        Returns:
            True if supported, False otherwise
        """
        return tp_type in cls._CALCULATOR_REGISTRY


def create_take_profit_calculator(
    config: Union[FixedTakeProfit, MultiTargetTakeProfit, IndicatorBasedSlTp, Dict],
    pip_value: float,
    logger: Optional[logging.Logger] = None
) -> TakeProfitCalculatorInterface:
    """
    Convenience function to create a take profit calculator.
    
    Args:
        config: Take profit configuration (model or dict)
        pip_value: Value used to convert pips to price distance (default: 10000.0 for forex)
        logger: Optional logger instance
        
    Returns:
        Appropriate take profit calculator implementation
    """
    if isinstance(config, dict):
        return TakeProfitCalculatorFactory.create_from_dict(config, pip_value, logger)
    else:
        return TakeProfitCalculatorFactory.create_calculator(config, pip_value, logger)