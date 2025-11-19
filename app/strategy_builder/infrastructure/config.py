"""
Configuration management for the strategy engine.
"""

import yaml
from typing import Any, Dict, List

from app.strategy_builder.core.domain.protocols import ConfigurationManager, ValidationService
from app.utils.config import YamlConfigurationManager



class JsonSchemaValidationService:
    """JSON Schema-based validation service."""
    
    def __init__(self):
        """Initialize validation service."""
        try:
            from jsonschema.validators import Draft7Validator
            self.validator_class = Draft7Validator
        except ImportError:
            raise ImportError("jsonschema package is required for validation")
    
    def validate_config(self, config: Dict[str, Any], schema: Dict[str, Any]) -> List[str]:
        """
        Validate configuration against schema.
        
        Args:
            config: Configuration to validate
            schema: JSON schema to validate against
            
        Returns:
            List of validation error messages (empty if valid)
        """
        validator = self.validator_class(schema)
        errors = sorted(validator.iter_errors(config), key=lambda e: e.path)
        
        error_messages = []
        for error in errors:
            err_path = ".".join(map(str, error.absolute_path))
            error_messages.append(f"{err_path}: {error.message}")
        
        return error_messages


class ConfigurationLoader:
    """High-level configuration loader with validation."""
    
    def __init__(
        self,
        config_manager: ConfigurationManager,
        validation_service: ValidationService
    ):
        """Initialize with dependencies."""
        self.config_manager = config_manager
        self.validation_service = validation_service
    
    def load_and_validate_configs(
        self,
        schema_path: str,
        config_paths: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Load and validate multiple configuration files.
        
        Args:
            schema_path: Path to validation schema
            config_paths: List of configuration file paths
            
        Returns:
            Dictionary mapping config file paths to loaded configurations
            
        Raises:
            ValueError: If any configuration fails validation
        """
        schema = self.config_manager.load_schema(schema_path)
        validated_configs = {}
        
        for config_path in config_paths:
            config = self.config_manager.load_config(config_path)
            
            # Validate configuration
            errors = self.validation_service.validate_config(config, schema)
            if errors:
                error_msg = f"Validation failed for {config_path}:\n" + "\n".join(f"  - {err}" for err in errors)
                raise ValueError(error_msg)
            
            validated_configs[config_path] = config
        
        return validated_configs


def create_config_manager() -> ConfigurationManager:
    """Factory function to create configuration manager."""
    return YamlConfigurationManager()


def create_validation_service() -> ValidationService:
    """Factory function to create validation service."""
    return JsonSchemaValidationService()


def create_configuration_loader(
    config_manager: ConfigurationManager = None,
    validation_service: ValidationService = None
) -> ConfigurationLoader:
    """Factory function to create configuration loader with dependencies."""
    if config_manager is None:
        config_manager = create_config_manager()
    if validation_service is None:
        validation_service = create_validation_service()
    
    return ConfigurationLoader(config_manager, validation_service)