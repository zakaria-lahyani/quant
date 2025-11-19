"""
Factory for creating position sizer instances.
"""

from typing import Optional, Dict, Type
import logging

from ..core.interfaces import PositionSizerInterface
from ..core.exceptions import UnsupportedConfigurationError
from ...strategy_builder.core.domain.models import PositionSizing
from ...strategy_builder.core.domain.enums import PositionSizingTypeEnum

from .fixed import FixedPositionSizer
from .percentage import PercentagePositionSizer
from .volatility import VolatilityPositionSizer


class PositionSizerFactory:
    """Factory for creating position sizer instances based on configuration."""
    
    _SIZER_REGISTRY: Dict[PositionSizingTypeEnum, Type[PositionSizerInterface]] = {
        PositionSizingTypeEnum.FIXED: FixedPositionSizer,
        PositionSizingTypeEnum.PERCENTAGE: PercentagePositionSizer,
        PositionSizingTypeEnum.VOLATILITY: VolatilityPositionSizer,
    }
    
    @classmethod
    def create_sizer(
        self,
        config: PositionSizing,
        logger: Optional[logging.Logger] = None
    ) -> PositionSizerInterface:
        """
        Create a position sizer based on the configuration type.
        
        Args:
            config: Position sizing configuration
            logger: Optional logger instance
            
        Returns:
            Appropriate position sizer implementation
            
        Raises:
            UnsupportedConfigurationError: If the sizing type is not supported
        """
        if config.type not in self._SIZER_REGISTRY:
            raise UnsupportedConfigurationError(
                f"Unsupported position sizing type: {config.type}",
                config_type=str(config.type)
            )
        
        sizer_class = self._SIZER_REGISTRY[config.type]
        return sizer_class(config, logger)
    
    @classmethod
    def register_sizer(
        cls,
        sizing_type: PositionSizingTypeEnum,
        sizer_class: Type[PositionSizerInterface]
    ) -> None:
        """
        Register a new position sizer implementation.
        
        Args:
            sizing_type: The position sizing type enum
            sizer_class: The sizer implementation class
        """
        cls._SIZER_REGISTRY[sizing_type] = sizer_class
    
    @classmethod
    def get_supported_types(cls) -> list[PositionSizingTypeEnum]:
        """
        Get list of supported position sizing types.
        
        Returns:
            List of supported position sizing types
        """
        return list(cls._SIZER_REGISTRY.keys())
    
    @classmethod
    def is_supported(cls, sizing_type: PositionSizingTypeEnum) -> bool:
        """
        Check if a position sizing type is supported.
        
        Args:
            sizing_type: The position sizing type to check
            
        Returns:
            True if supported, False otherwise
        """
        return sizing_type in cls._SIZER_REGISTRY


def create_position_sizer(
    config: PositionSizing,
    logger: Optional[logging.Logger] = None
) -> PositionSizerInterface:
    """
    Convenience function to create a position sizer.
    
    Args:
        config: Position sizing configuration
        logger: Optional logger instance
        
    Returns:
        Appropriate position sizer implementation
    """
    return PositionSizerFactory.create_sizer(config, logger)