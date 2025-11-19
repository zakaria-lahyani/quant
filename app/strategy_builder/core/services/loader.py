"""
Strategy loader with dependency injection.
"""

from typing import Dict, List

from app.strategy_builder.core.domain.protocols import StrategyLoaderInterface, Logger
from app.strategy_builder.core.domain.models import TradingStrategy
from app.strategy_builder.infrastructure.config import ConfigurationLoader


class StrategyLoader(StrategyLoaderInterface):
    """Loads and validates trading strategies from configuration files."""
    
    def __init__(
        self,
        schema_path: str,
        config_paths: List[str],
        config_loader: ConfigurationLoader,
        logger: Logger
    ):
        """
        Initialize strategy loader.
        
        Args:
            schema_path: Path to validation schema
            config_paths: List of strategy configuration file paths
            config_loader: Configuration loader with validation
            logger: Logger instance
        """
        self.schema_path = schema_path
        self.config_paths = config_paths
        self.config_loader = config_loader
        self.logger = logger
        self._strategies: Dict[str, TradingStrategy] = {}
    
    def load_strategies(self) -> Dict[str, TradingStrategy]:
        """
        Load and validate all trading strategies.
        
        Returns:
            Dictionary mapping strategy names to TradingStrategy instances
            
        Raises:
            ValueError: If any strategy fails validation
            FileNotFoundError: If configuration files are not found
        """
        if self._strategies:
            return self._strategies
        
        try:
            # Load and validate all configurations
            validated_configs = self.config_loader.load_and_validate_configs(
                self.schema_path,
                self.config_paths
            )
            
            # Convert to TradingStrategy instances
            for config_path, config_data in validated_configs.items():
                try:
                    strategy = TradingStrategy(**config_data)
                    strategy_name = strategy.name
                    
                    # Check for duplicate strategy names
                    if strategy_name in self._strategies:
                        self.logger.warning(
                            f"Duplicate strategy name '{strategy_name}' found in {config_path}. "
                            f"Overwriting previous definition."
                        )
                    
                    self._strategies[strategy_name] = strategy
                    self.logger.info(f"Successfully loaded strategy: {strategy_name}")
                    
                except Exception as e:
                    self.logger.error(f"Failed to create strategy from {config_path}: {e}")
                    raise ValueError(f"Failed to create strategy from {config_path}: {e}")
            
            self.logger.info(f"Successfully loaded {len(self._strategies)} strategies")
            return self._strategies
            
        except Exception as e:
            self.logger.error(f"Failed to load strategies: {e}")
            raise
    
    @property
    def strategies(self) -> Dict[str, TradingStrategy]:
        """
        Get loaded strategies (lazy loading).
        
        Returns:
            Dictionary of loaded strategies
        """
        if not self._strategies:
            self.load_strategies()
        return self._strategies
    
    def reload_strategies(self) -> Dict[str, TradingStrategy]:
        """
        Force reload of all strategies.
        
        Returns:
            Dictionary of reloaded strategies
        """
        self._strategies.clear()
        return self.load_strategies()
    
    def get_strategy(self, name: str) -> TradingStrategy:
        """
        Get a specific strategy by name.
        
        Args:
            name: Strategy name
            
        Returns:
            TradingStrategy instance
            
        Raises:
            KeyError: If strategy is not found
        """
        strategies = self.strategies
        if name not in strategies:
            raise KeyError(f"Strategy '{name}' not found. Available strategies: {list(strategies.keys())}")
        return strategies[name]
    
    def list_strategy_names(self) -> List[str]:
        """
        Get list of all loaded strategy names.
        
        Returns:
            List of strategy names
        """
        return list(self.strategies.keys())


def create_strategy_loader(
    schema_path: str,
    config_paths: List[str],
    config_loader: ConfigurationLoader,
    logger: Logger
) -> StrategyLoaderInterface:
    """
    Factory function to create strategy loader.
    
    Args:
        schema_path: Path to validation schema
        config_paths: List of configuration file paths
        config_loader: Configuration loader instance
        logger: Logger instance
        
    Returns:
        Strategy loader instance
    """
    return StrategyLoader(schema_path, config_paths, config_loader, logger)