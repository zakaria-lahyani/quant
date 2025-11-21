import os
import logging
from typing import Dict, List, Any
from pathlib import Path

from app.data.data_manger import DataSourceManager
from app.entry_manager.manager import EntryManager
from app.indicators.indicator_processor import IndicatorProcessor
from app.infrastructure.config_loader import YamlConfigurationManager, LoadEnvironmentVariables
from app.infrastructure.configs.config_definitions import SystemConfig
from app.regime.regime_manager import RegimeManager
from app.trader.executor_builder import ExecutorBuilder


def load_strategies_for_symbol(folder_path: str, symbol: str, path_symbol: str, logger: logging.Logger):
    """
    Load strategy engine for a specific symbol.

    Args:
        folder_path: Base configuration folder path
        symbol: Trading symbol for display (e.g., "XAUUSD")
        path_symbol: Symbol name for file paths (e.g., "xauusd")
        logger: Logger instance

    Returns:
        StrategyEngine for the symbol
    """
    from app.strategy_builder.factory import StrategyEngineFactory
    from app.utils.functions_helper import list_files_in_folder

    logger.info(f"  Loading strategies for {symbol}...")

    # Use path_symbol for folder lookup (e.g., "xauusd" for both "XAUUSD" and "XAUUSD.pro")
    strategy_folder = Path(folder_path) / "strategies" / path_symbol

    if not strategy_folder.exists():
        logger.warning(f"  No strategy folder found for {symbol} at {strategy_folder}")
        logger.warning(f"  Creating empty strategy engine for {symbol}")
        # Return empty engine or handle as needed
        return StrategyEngineFactory.create_engine(
            config_paths=[],
            logger_name=f"trading-engine-{path_symbol}"
        )

    strategy_paths = list_files_in_folder(str(strategy_folder))

    if not strategy_paths:
        logger.warning(f"  No strategy files found for {symbol}")

    logger.info(f"  Found {len(strategy_paths)} strategy files for {symbol}")

    engine = StrategyEngineFactory.create_engine(
        config_paths=strategy_paths,
        logger_name=f"trading-engine-{path_symbol}"
    )

    return engine


def load_indicators_for_symbol(folder_path: str, symbol: str, path_symbol: str, logger: logging.Logger) -> Dict:
    """
    Load indicator configuration for a specific symbol.

    Args:
        folder_path: Base configuration folder path
        symbol: Trading symbol for display (e.g., "XAUUSD")
        path_symbol: Symbol name for file paths (e.g., "xauusd")
        logger: Logger instance

    Returns:
        Dict mapping timeframe -> indicator config
    """
    from app.utils.functions_helper import list_files_in_folder
    from app.strategy_builder.core.domain.enums import TimeFrameEnum

    logger.info(f"  Loading indicators for {symbol}...")

    yaml_manager = YamlConfigurationManager()
    # Use path_symbol for folder lookup (e.g., "xauusd" for both "XAUUSD" and "XAUUSD.pro")
    indicator_folder = Path(folder_path) / "indicators" / path_symbol

    if not indicator_folder.exists():
        logger.warning(f"  No indicator folder found for {symbol} at {indicator_folder}")
        return {}

    indicator_paths = list_files_in_folder(str(indicator_folder))

    if not indicator_paths:
        logger.warning(f"  No indicator files found for {symbol}")
        return {}

    # Parse timeframe from filename (e.g., "xauusd_1.yaml" -> "1")
    files_by_tf = {
        Path(p).stem.split("_")[-1]: p
        for p in indicator_paths
    }

    logger.info(f"  Found {len(files_by_tf)} indicator configs for {symbol}: {list(files_by_tf.keys())}")

    configs = {}
    for tf in TimeFrameEnum:
        if tf.value in files_by_tf:
            configs[tf.value] = yaml_manager.load_config(files_by_tf[tf.value])

    return configs


def load_all_components_for_symbols(
    config_path: str,
    env_config: LoadEnvironmentVariables,
    system_config: SystemConfig,
    client: Any,
    data_source: DataSourceManager,
    logger: logging.Logger
) -> Dict[str, Dict[str, Any]]:
    """
    Load all components for all symbols.

    Args:
        config_path: Absolute path to the configs folder
        env_config: Environment configuration
        system_config: System configuration object
        client: MT5 client
        data_source: Data source manager
        logger: Logger instance

    Returns:
        Dictionary mapping symbol to component dictionary
    """
    logger.info("\n=== Loading Components for All Symbols ===")

    symbols = system_config.trading.symbols
    symbol_components = {}
    for symbolConfig in symbols:
        logger.info(f"\n--- Loading components for {symbolConfig} ---")

        # Load strategies
        strategy_engine = load_strategies_for_symbol(
            folder_path=config_path,
            symbol=symbolConfig.symbol,
            path_symbol=symbolConfig.path_symbol,
            logger=logger
        )
        # Create entry manager
        strategies = {
            name: strategy_engine.get_strategy_info(name)
            for name in strategy_engine.list_available_strategies()
        }
        logger.info(f"  Creating EntryManager for {symbolConfig.symbol}...")
        entry_manager = EntryManager(
            strategies=strategies,
            symbol=symbolConfig.symbol,
            pip_value=symbolConfig.pip_value,
            logger=logging.getLogger(f'entry-manager-{symbolConfig.symbol.lower()}')
        )
        # Use timeframes from symbol config (services.yaml)
        timeframes = symbolConfig.timeframes
        logger.info(f"  Timeframes for {symbolConfig.symbol}: {timeframes}")

        # Load indicator configuration (optional, for additional indicator settings)
        indicator_config = load_indicators_for_symbol(
            folder_path=config_path,
            symbol=symbolConfig.symbol,
            path_symbol=symbolConfig.path_symbol,
            logger=logger
        )

        # Create empty configs for timeframes that don't have indicator files
        if not indicator_config:
            logger.info(f"  No indicator configuration files found for {symbolConfig.symbol}, using empty configs")
            indicator_config = {tf: {} for tf in timeframes}
        else:
            # Fill in missing timeframes with empty configs
            for tf in timeframes:
                if tf not in indicator_config:
                    indicator_config[tf] = {}

        # Fetch historical data (use broker_symbol for API calls)
        logger.info(f"  Fetching historical data for {symbolConfig.symbol} (broker symbol: {symbolConfig.broker_symbol})...")
        historicals = {}
        for tf in timeframes:
            try:
                historicals[tf] = data_source.get_historical_data(
                    symbol=symbolConfig.broker_symbol,  # Use broker-specific symbol name for API
                    timeframe=tf
                )
                logger.info(f"   Loaded {len(historicals[tf])} bars for {symbolConfig.symbol} {tf}")
            except Exception as e:
                logger.error(f"  Failed to load historical data for {symbolConfig.symbol} {tf}: {e}")
                historicals[tf] = None

        # Create indicator processor
        logger.info(f"  Creating IndicatorProcessor for {symbolConfig.symbol}...")
        indicator_processor = IndicatorProcessor(
            configs=indicator_config,
            historicals=historicals,
            is_bulk=False
        )
        # Create regime manager
        logger.info(f"  Creating RegimeManager for {symbolConfig.symbol}...")
        regime_manager = RegimeManager(
            warmup_bars=500,
            persist_n=2,
            transition_bars=3,
            bb_threshold_len=200
        )
        regime_manager.setup(timeframes, historicals)

        # Create trade executor
        logger.info(f"  Creating TradeExecutor for {symbolConfig.symbol}...")

        class SymbolConfigForExecutor:
            """Configuration wrapper for TradeExecutor."""
            def __init__(self, env_config: LoadEnvironmentVariables, symbol_config):
                # Environment config (from .env)
                self.ACCOUNT_TYPE = env_config.ACCOUNT_TYPE
                self.NEWS_RESTRICTION_DURATION = env_config.NEWS_RESTRICTION_DURATION
                self.MARKET_CLOSE_RESTRICTION_DURATION = env_config.MARKET_CLOSE_RESTRICTION_DURATION

                # Symbol-specific config (from services.yaml)
                self.SYMBOL = symbol_config.symbol  # Display symbol for internal use
                self.BROKER_SYMBOL = symbol_config.broker_symbol  # Broker API symbol for orders
                self.PIP_VALUE = symbol_config.pip_value
                self.POSITION_SPLIT = symbol_config.position_split
                self.SCALING_TYPE = symbol_config.scaling_type
                self.ENTRY_SPACING = symbol_config.entry_spacing
                self.RISK_PER_GROUP = symbol_config.risk_per_group
                self.DEFAULT_CLOSE_TIME = symbol_config.default_close_time
                self.ORDER_DELAY_SECONDS = symbol_config.order_delay_seconds

                # Risk config (from services.yaml)
                self.DAILY_LOSS_LIMIT = system_config.risk.account_stop_loss.daily_loss_limit

                # Restriction folder path
                self.RESTRICTION_CONF_FOLDER_PATH = str(Path(config_path) / "restrictions")

        # Create the symbol config for executor
        executor_config = SymbolConfigForExecutor(env_config, symbolConfig)
        trade_executor = ExecutorBuilder.build_from_config(
            config=executor_config,
            client=client,
            event_bus=None,  # Will be injected later by orchestrator
            logger=logging.getLogger(f'trade-executor-{symbolConfig.symbol.lower()}')
        )

        # Store all components for this symbol
        symbol_components[symbolConfig.symbol] = {
            'indicator_processor': indicator_processor,
            'regime_manager': regime_manager,
            'strategy_engine': strategy_engine,
            'entry_manager': entry_manager,
            'trade_executor': trade_executor,
            'timeframes': timeframes,
            'historicals': historicals,
            'broker_symbol': symbolConfig.broker_symbol,  # For MT5 API calls
        }

        logger.info(f"  ✓ All components loaded for {symbolConfig.symbol}")

    logger.info(f"\n=== Components loaded for {len(symbol_components)} symbols ===")

    return symbol_components