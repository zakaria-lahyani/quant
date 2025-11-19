"""
High-level factory for creating strategy engine instances with all dependencies.
"""

import os
from typing import List, Optional

from importlib.resources import files

from app.strategy_builder.core.domain.protocols import ConfigurationManager, ValidationService, EvaluatorFactory, StrategyLoaderInterface
from app.strategy_builder.core.services import create_strategy_engine
from app.strategy_builder.core.services.loader import create_strategy_loader
from app.strategy_builder.core.evaluators import create_evaluator_factory
from app.strategy_builder.infrastructure import (
    create_logger,
    create_configuration_loader,
    create_config_manager,
    create_validation_service, ConfigurationLoader
)

def get_default_schema_path() -> str:
    """Get the path to the default schema file, handling both development and installed package scenarios."""
    try:
        # Try to use importlib.resources for installed packages
        schema_files = files("strategy_builder") / "config" / "strategy_schema.json"
        if hasattr(schema_files, 'read_text'):
            # For Python 3.9+ or when the file exists as a resource
            return str(schema_files)
        else:
            # Fallback to extracting the file to a temporary location
            import tempfile
            import json
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                f.write(schema_files.read_text())
                return f.name
    except (ImportError, FileNotFoundError, AttributeError):
        # Fallback to file system path for development
        fallback_path = os.path.join(os.path.dirname(__file__), "config", "strategy_schema.json")
        if os.path.exists(fallback_path):
            return fallback_path
        else:
            raise FileNotFoundError(
                "Default schema file not found. Please ensure the package is properly installed "
                "or provide an explicit schema_path."
            )


class StrategyEngineFactory:
    """High-level factory for creating fully configured strategy engines."""
    
    @staticmethod
    def create_engine(
        schema_path: Optional[str] = None,
        config_paths: List[str] = None,
        logger_name: str = "stratfactory"
    ):
        """
        Create a fully configured strategy engine with all dependencies.
        
        Args:
            schema_path: Path to validation schema file (uses default if None)
            config_paths: List of strategy configuration file paths
            logger_name: Name for the logger instance
            
        Returns:
            Configured StrategyEngine instance
            
        Example:
            >>> # Using default schema
            >>> engine = StrategyEngineFactory.create_engine(
            ...     config_paths=["strategy1.yaml", "strategy2.yaml"]
            ... )
            >>>
            >>> # Using custom schema
            >>> engine = StrategyEngineFactory.create_engine(
            ...     schema_path="custom_schema.json",
            ...     config_paths=["strategy1.yaml", "strategy2.yaml"]
            ... )
            >>> results = engine.evaluate(market_data)
        """
        # Use default schema if none provided
        if schema_path is None:
            schema_path = get_default_schema_path()
        
        # Validate inputs
        if config_paths is None:
            raise ValueError("config_paths must be provided")
        
        if not os.path.exists(schema_path):
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        # Create infrastructure dependencies
        logger = create_logger(logger_name)
        config_manager: ConfigurationManager = create_config_manager()
        validation_service: ValidationService = create_validation_service()
        config_loader: ConfigurationLoader = create_configuration_loader(config_manager, validation_service)
        
        # Create api dependencies
        evaluator_factory: EvaluatorFactory = create_evaluator_factory(logger)
        strategy_loader: StrategyLoaderInterface = create_strategy_loader(
            schema_path, config_paths, config_loader, logger
        )
        
        # Load strategies immediately to catch any configuration errors
        strategy_loader.load_strategies()
        
        # Create and return the engine
        return create_strategy_engine(strategy_loader, evaluator_factory, logger)
    
    @staticmethod
    def create_engine_for_testing(
        schema_path: Optional[str] = None,
        config_paths: List[str] = None
    ):
        """
        Create a strategy engine configured for testing (with null logger).
        
        Args:
            schema_path: Path to validation schema file (uses default if None)
            config_paths: List of strategy configuration file paths
            
        Returns:
            Configured StrategyEngine instance with null logger
        """
        # Use default schema if none provided
        if schema_path is None:
            schema_path = get_default_schema_path()
        
        # Validate inputs
        if config_paths is None:
            raise ValueError("config_paths must be provided")
        
        if not os.path.exists(schema_path):
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        from app.strategy_builder.infrastructure.logging import create_null_logger
        
        # Create infrastructure dependencies with null logger
        logger = create_null_logger()
        config_manager: ConfigurationManager = create_config_manager()
        validation_service: ValidationService = create_validation_service()
        config_loader: ConfigurationLoader = create_configuration_loader(config_manager, validation_service)
        
        # Create api dependencies
        evaluator_factory: EvaluatorFactory = create_evaluator_factory(logger)
        strategy_loader: StrategyLoaderInterface = create_strategy_loader(
            schema_path, config_paths, config_loader, logger
        )
        
        # Load strategies immediately to catch any configuration errors
        strategy_loader.load_strategies()
        
        # Create and return the engine
        return create_strategy_engine(strategy_loader, evaluator_factory, logger)


def create_strategy_engine_simple(
    schema_path: Optional[str] = None,
    config_paths: List[str] = None,
    logger_name: str = "stratfactory"
):
    """
    Simple factory function for creating strategy engines.
    
    Args:
        schema_path: Path to validation schema file (uses default if None)
        config_paths: List of strategy configuration file paths
        logger_name: Name for the logger instance
        
    Returns:
        Configured StrategyEngine instance
    """
    return StrategyEngineFactory.create_engine(schema_path, config_paths, logger_name)


# Convenience function for backward compatibility
def create_legacy_engine(schema_path: Optional[str] = None, config_paths: List[str] = None):
    """
    Create engine with the same interface as the original StrategyEngine.
    
    This function provides backward compatibility with the original API.
    
    Args:
        schema_path: Path to validation schema file (uses default if None)
        config_paths: List of strategy configuration file paths
        
    Returns:
        Configured StrategyEngine instance
    """
    return create_strategy_engine_simple(schema_path, config_paths)