"""
Executor Builder - Simplifies TradeExecutor initialization with proper dependency injection.
"""

import logging
from typing import Optional

from app.trader.risk_manager.models import ScalingConfig
from app.trader.risk_manager.risk_calculator import RiskCalculator

from .live_trader import LiveTrader
from .components.exit_manager import ExitManager
from .components.duplicate_filter import DuplicateFilter
from .components.pnl_calculator import PnLCalculator
from .components.risk_monitor import RiskMonitor
from .components.order_executor import OrderExecutor
from app.trader.managers.trade_restriction import TradeRestriction
from app.trader.managers.suspension_store import SuspensionStore
from app.trader.managers.restriction_manager import RestrictionManager
from .trade_executor import TradeExecutor


class ExecutorBuilder:
    """
    Builder for creating properly configured TradeExecutor instances.
    Handles all dependency injection and configuration.
    """
    
    @staticmethod
    def build_from_config(
        config,
        client,
        event_bus = None,
        logger: Optional[logging.Logger] = None
    ) -> TradeExecutor:
        """
        Build a TradeExecutor from configuration.
        
        Args:
            config: Environment configuration
            client: MT5 client
            logger: Optional logger
            
        Returns:
            Configured TradeExecutorV3 instance
        """
        if logger is None:
            logger = logging.getLogger('trade-executor')

        # Get order delay configuration (default to 0.5 seconds if not specified)
        order_delay = getattr(config, 'ORDER_DELAY_SECONDS', 0.5)

        # Create trader with order delay configuration
        trader = LiveTrader(client, logger, order_delay_seconds=order_delay)
        
        # Create scaling and risk configuration
        scaling_config = ScalingConfig(
            num_entries=config.POSITION_SPLIT,
            scaling_type=config.SCALING_TYPE,
            entry_spacing=config.ENTRY_SPACING,
            max_risk_per_group=config.RISK_PER_GROUP
        )
        risk_calculator = RiskCalculator(scaling_config)
        
        # Get broker_symbol from config (defaults to SYMBOL if not present)
        broker_symbol = getattr(config, 'BROKER_SYMBOL', config.SYMBOL)

        # Create components
        exit_manager = ExitManager(trader, config.SYMBOL, broker_symbol, logger)
        duplicate_filter = DuplicateFilter(logger)
        pnl_calculator = PnLCalculator(logger)
        risk_monitor = RiskMonitor(
            trader,
            config.DAILY_LOSS_LIMIT,
            pnl_calculator,
            logger
        )

        order_executor = OrderExecutor(
            trader,
            risk_calculator,
            config.SYMBOL,
            broker_symbol,
            event_bus,
            logger
        )
        
        # Create restriction components
        trade_restriction = TradeRestriction(
            restriction_path=config.RESTRICTION_CONF_FOLDER_PATH,
            default_close_time_str=config.DEFAULT_CLOSE_TIME,
            news_duration=config.NEWS_RESTRICTION_DURATION,
            market_close_duration=config.MARKET_CLOSE_RESTRICTION_DURATION
        )
        suspension_store = SuspensionStore(logger)
        restriction_manager = RestrictionManager(
            trader,
            suspension_store,
            trade_restriction,
            config.SYMBOL,
            config.ACCOUNT_TYPE,
            logger
        )
        
        # Create executor
        return TradeExecutor(
            trader=trader,
            exit_manager=exit_manager,
            duplicate_filter=duplicate_filter,
            risk_monitor=risk_monitor,
            order_executor=order_executor,
            restriction_manager=restriction_manager,
            symbol=config.SYMBOL,
            logger=logger
        )
    
    @staticmethod
    def build_with_components(
        trader: LiveTrader,
        exit_manager: ExitManager,
        duplicate_filter: DuplicateFilter,
        risk_monitor: RiskMonitor,
        order_executor: OrderExecutor,
        restriction_manager: RestrictionManager,
        symbol: str,
        logger: Optional[logging.Logger] = None
    ) -> TradeExecutor:
        """
        Build a TradeExecutor with pre-configured components.
        Useful for testing or custom configurations.
        
        Args:
            trader: Live trader instance
            exit_manager: Exit manager instance
            duplicate_filter: Duplicate filter instance
            risk_monitor: Risk monitor instance
            order_executor: Order executor instance
            restriction_manager: Restriction manager instance
            symbol: Trading symbol
            logger: Optional logger
            
        Returns:
            Configured TradeExecutorV3 instance
        """
        return TradeExecutor(
            trader=trader,
            exit_manager=exit_manager,
            duplicate_filter=duplicate_filter,
            risk_monitor=risk_monitor,
            order_executor=order_executor,
            restriction_manager=restriction_manager,
            symbol=symbol,
            logger=logger
        )