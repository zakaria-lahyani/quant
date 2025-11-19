

import os
import sys
import logging
from pathlib import Path

from app.infrastructure.config_loader import ConfigLoader, LoadEnvironmentVariables
from app.infrastructure.logging import LoggingManager


def initialize_logging(config_path: str = "configs/services.yaml") -> LoggingManager:
    """
    Initialize enhanced logging system with correlation IDs.

    Args:
        config_path: Path to configuration file

    Returns:
        Configured LoggingManager
    """
    try:
        # Try to load logging config from file
        system_config = ConfigLoader.load(config_path)
        logging_manager = LoggingManager.from_config(system_config)
    except FileNotFoundError:
        # Fallback to default configuration
        logging_manager = LoggingManager(
            level="INFO",
            format_type="text",
            include_correlation_ids=False,  # Disable correlation IDs for cleaner logs
            file_output=False
        )

    logging_manager.configure_root_logger()

    # Suppress noisy loggers for cleaner output
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('httpcore').setLevel(logging.WARNING)

    return logging_manager


def main():
    """
    Main entry point for multi-symbol live trading.

    This uses the new event-driven architecture with MultiSymbolTradingOrchestrator
    to manage services for multiple symbols concurrently.
    """
    # Setup paths
    ROOT_DIR = Path(__file__).parent.parent
    env_path = os.path.join(ROOT_DIR, ".env")

    env_config = LoadEnvironmentVariables(env_path)
    config_path = os.path.join(ROOT_DIR, env_config.CONF_FOLDER_PATH, "services.yaml")

    # Initialize logging first
    logging_manager = initialize_logging(config_path)
    logger = logging.getLogger(__name__)

    # Load system configuration
    logger.info(f"Loading system configuration ")
    try:
        system_config = ConfigLoader.load(config_path)
        logger.info("System configuration loaded")
    except FileNotFoundError:
        logger.warning(f"Configuration file not found: {config_path}")
        logger.info("Using default configuration...")

    print(system_config)


if __name__ == "__main__":
    main()
