from app.indicators.indicator_handler import IndicatorHandler
from app.indicators.registry import INDICATOR_CLASSES, DEFAULT_PARAMETERS

from typing import Dict

class IndicatorFactory:
    """
    Factory class for creating multiple IndicatorHandler instances based on a configuration dictionary.

    Attributes:
        config (Dict[str, dict]): Dictionary mapping indicator names to parameter overrides.
    """

    def __init__(self, config: Dict[str, dict]):
        """
        Initialize the factory with configuration.

        Args:
            config (Dict[str, dict]): A dictionary like {'macd_1h': {'signal': 9}}.
        """
        self.config = config

    def create_handlers(self) -> Dict[str, IndicatorHandler]:
        """
        Creates indicator handlers using INDICATOR_CLASSES and DEFAULT_PARAMETERS.

        Returns:
            Dict[str, IndicatorHandler]: A dictionary mapping indicator names to their handlers.
        """
        handlers = {}
        for name, user_params in self.config.items():
            base = name.split('_')[0]
            cls = INDICATOR_CLASSES.get(base)
            if not cls:
                continue
            params = {**DEFAULT_PARAMETERS.get(base, {}), **user_params}
            handlers[name] = IndicatorHandler(name, cls(**params))
        return handlers
