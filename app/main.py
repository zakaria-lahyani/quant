

import os
import sys
import logging
from pathlib import Path

from app.clients.mt5.client import create_client_with_retry
from app.data.data_manger import DataSourceManager
from app.infrastructure.config_loader import AccountConfigLoader, LoadEnvironmentVariables
from app.infrastructure.logging import LoggingManager
from app.infrastructure.events import EventBusFactory
from app.infrastructure.events.runtime_control_store import RuntimeControlStore
from app.utils.date_helper import DateHelper
from app.utils.load_component import load_strategies_for_symbol, load_all_components_for_symbols


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
        system_config = AccountConfigLoader.load(config_path)
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
    config_path = os.path.join(ROOT_DIR, env_config.CONF_FOLDER_PATH)
    service_config_path = os.path.join(ROOT_DIR, env_config.CONF_FOLDER_PATH, env_config.CONF_ACCOUNT)

    # Initialize logging first
    logging_manager = initialize_logging(service_config_path)
    logger = logging.getLogger(__name__)

    # Load system configuration
    logger.info(f"Loading system configuration ")
    try:
        system_config = AccountConfigLoader.load(service_config_path)
        logger.info("System configuration loaded")

        # MT5 Client (shared)
        logger.info("Creating MT5 client...")
        client = create_client_with_retry(env_config.API_BASE_URL)

        # Data Source Manager (shared)
        logger.info("Initializing data source manager...")
        data_source = DataSourceManager(
            mode=env_config.TRADE_MODE,
            client=client,
            date_helper=DateHelper()
        )
        # Date Helper (shared)
        date_helper = DateHelper()

        # Get account balance
        account_balance = client.account.get_balance()
        logger.info(f"Account balance: {account_balance}")

        logger.info("Shared components initialized")

        # Initialize Redis Event Bus and Runtime Controls (optional - only if Redis is available)
        event_bus = None
        runtime_controls = None
        try:
            logger.info("Connecting to Redis event bus...")
            event_bus = EventBusFactory.create_from_env(env_config, logger)
            logger.info("Redis event bus connected")

            # Create runtime control store using the same Redis connection
            import redis
            redis_client = redis.Redis(
                host=env_config.REDIS_HOST,
                port=int(env_config.REDIS_PORT),
                db=int(env_config.REDIS_DB)
            )
            runtime_controls = RuntimeControlStore(
                redis_client=redis_client,
                account_name=env_config.APP_NAME,
                logger=logging.getLogger('runtime-controls')
            )
            logger.info(f"Runtime controls initialized for account: {env_config.APP_NAME}")
        except ImportError:
            logger.warning("redis-py not installed. Event bus disabled. Install with: pip install redis")
        except Exception as e:
            logger.warning(f"Failed to connect to Redis event bus: {e}")
            logger.warning("Continuing without event bus and runtime controls...")

        # Load components for all symbols
        symbol_components = load_all_components_for_symbols(
            config_path=config_path,
            env_config=env_config,
            system_config=system_config,
            client=client,
            data_source=data_source,
            logger=logger,
            runtime_controls=runtime_controls
        )

        logger.info(f"\n=== Components loaded for {len(symbol_components)} symbols ===\n")

        # Initialize runtime controls with discovered strategies
        if runtime_controls:
            # Collect all strategy names from loaded components
            all_strategy_names = set()
            for symbol, components in symbol_components.items():
                strategy_engine = components.get('strategy_engine')
                if strategy_engine:
                    all_strategy_names.update(strategy_engine.list_available_strategies())

            # Initialize defaults (only sets if not already set in Redis)
            runtime_controls.initialize_defaults(list(all_strategy_names))
            logger.info(f"Runtime controls initialized with {len(all_strategy_names)} strategies: {sorted(all_strategy_names)}")

        # Create and start multi-symbol orchestrator
        from app.orchestrator import MultiSymbolTradingOrchestrator

        logger.info("Creating multi-symbol trading orchestrator...")
        orchestrator = MultiSymbolTradingOrchestrator(
            system_config=system_config,
            symbol_components=symbol_components,
            client=client,
            data_source=data_source,
            redis_event_bus=event_bus,
            account_name=env_config.APP_NAME,
            account_tag=env_config.APP_TAG,
            logger=logger
        )

        # Run orchestrator with periodic health checks
        logger.info("Starting trading orchestrator...\n")
        orchestrator.run(check_interval=system_config.orchestrator.health_check_interval)

    except KeyboardInterrupt:
        logger.info("\nShutdown requested by user")
        if 'orchestrator' in locals():
            orchestrator.stop()
    except FileNotFoundError:
        logger.warning(f"Configuration file not found: {service_config_path}")
        logger.info("Using default configuration...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        if 'orchestrator' in locals():
            orchestrator.stop()
        raise


if __name__ == "__main__":
    main()
