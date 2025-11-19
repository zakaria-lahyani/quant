import os
import logging
from typing import  Optional, Dict, Any
from pathlib import Path

import yaml

from app.infrastructure.configs.config_definitions import SystemConfig


class ConfigLoader:
    """Loader for system configuration from YAML files."""

    def __init__(self, config_dir: Optional[Path] = None):
        """
        Initialize the config loader.

        Args:
            config_dir: Directory containing configuration files.
                       Defaults to 'configs' relative to project root.
        """
        if config_dir is None:
            # Default to configs directory relative to this file
            self.config_dir = Path(__file__).parent.parent.parent.parent / "configs"
        else:
            self.config_dir = Path(config_dir)

        self.logger = logging.getLogger(__name__)

    def load_from_yaml(self, filename: str = "services.yaml") -> SystemConfig:
        """
        Load configuration from a YAML file.

        Args:
            filename: Name of the YAML file to load

        Returns:
            Validated SystemConfig object

        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config validation fails
        """
        config_path = self.config_dir / filename

        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        self.logger.info(f"Loading configuration from {config_path}")

        with open(config_path, "r") as f:
            raw_config = yaml.safe_load(f)

        # Apply environment variable overrides
        config_with_overrides = self._apply_env_overrides(raw_config)

        try:
            config = SystemConfig(**config_with_overrides)
            self.logger.info("Configuration loaded and validated successfully")
            return config
        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            raise ValueError(f"Invalid configuration: {e}")

    def _apply_env_overrides(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply environment variable overrides to configuration.

        Environment variables should be prefixed with 'TRADING_' and use
        double underscores to denote nested keys. For example:
        - TRADING_LOGGING_LEVEL=DEBUG
        - TRADING_SERVICES_DATA_FETCHING_ENABLED=false

        Args:
            config: Raw configuration dictionary

        Returns:
            Configuration with environment overrides applied
        """
        prefix = "TRADING_"

        for env_key, env_value in os.environ.items():
            if not env_key.startswith(prefix):
                continue

            # Remove prefix and split by double underscore
            config_path = env_key[len(prefix):].lower().split("__")

            # Navigate to the nested key and set the value
            current = config
            for key in config_path[:-1]:
                if key not in current:
                    current[key] = {}
                current = current[key]

            # Parse the value appropriately
            final_key = config_path[-1]
            parsed_value = self._parse_env_value(env_value)
            current[final_key] = parsed_value

            self.logger.debug(f"Applied env override: {env_key} = {parsed_value}")

        return config

    def _parse_env_value(self, value: str) -> Any:
        """
        Parse environment variable value to appropriate type.

        Args:
            value: String value from environment variable

        Returns:
            Parsed value (bool, int, float, or str)
        """
        # Boolean
        if value.lower() in ("true", "false"):
            return value.lower() == "true"

        # Integer
        try:
            return int(value)
        except ValueError:
            pass

        # Float
        try:
            return float(value)
        except ValueError:
            pass

        # String
        return value


def load_config(config_file: str = "services.yaml", config_dir: Optional[Path] = None) -> SystemConfig:
    """
    Convenience function to load configuration.

    Args:
        config_file: Name of the configuration file
        config_dir: Directory containing configuration files

    Returns:
        Validated SystemConfig object

    Example:
        >>> config = load_config("services.yaml")
        >>> print(config.logging.level)
        INFO
    """
    loader = ConfigLoader(config_dir)
    return loader.load_from_yaml(config_file)

