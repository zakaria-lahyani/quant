"""
Trade Executor Facade - Provides backward compatibility with the old TradeExecutor interface.
"""

import logging
from typing import Dict, Any

from app.strategy_builder.data.dtos import Trades
from app.utils.config import LoadEnvironmentVariables
from app.utils.date_helper import DateHelper

from app.trader.executor_builder import ExecutorBuilder
from app.trader.trade_executor import TradeExecutor


class TradeExecutor:
    """
    Facade for TradeExecutorV3 that maintains backward compatibility.
    
    This class provides the same interface as the old TradeExecutor
    but delegates to the new, cleaner implementation.
    """
    
    def __init__(self, mode: str, config: LoadEnvironmentVariables, **kwargs):
        """
        Initialize TradeExecutor with backward compatible interface.
        
        Args:
            mode: Trading mode ('live' only supported)
            config: Environment configuration
            **kwargs: Additional arguments (must include 'client' for live mode)
        """
        self.mode = mode
        self.config = config
        self.logger = logging.getLogger('trade-executor')
        
        # Validate mode
        if mode != 'live':
            raise ValueError(f"Unsupported mode: {mode}")
            
        # Get client
        if 'client' not in kwargs:
            raise ValueError("Live trading requires client")
            
        # Build the executor using the builder
        self._executor: TradeExecutor = ExecutorBuilder.build_from_config(
            config=config,
            client=kwargs['client'],
            logger=self.logger
        )
        
        # Expose some components for backward compatibility
        self.trader = self._executor.trader
        self.exit_manager = self._executor.exit_manager
        self.duplicate_filter = self._executor.duplicate_filter
        self.risk_monitor = self._executor.risk_monitor
        self.order_executor = self._executor.order_executor
        
        # Expose state for backward compatibility
        self._update_backward_compat_state()
        
    def manage(self, trades: Trades, date_helper: DateHelper) -> None:
        """
        Main entry point - maintains backward compatibility.
        
        Args:
            trades: Trade signals to process
            date_helper: Date helper for time calculations
        """
        # Execute trading cycle
        context = self._executor.execute_trading_cycle(trades, date_helper)
        
        # Update backward compatibility state
        self._update_backward_compat_state()
        
    def get_risk_metrics(self, date_helper: DateHelper) -> Dict[str, Any]:
        """
        Get current risk metrics.
        
        Args:
            date_helper: Date helper for time calculations
            
        Returns:
            Dictionary with risk metrics
        """
        # Update context first
        self._executor._update_context(date_helper)
        
        # Get metrics from context
        context = self._executor.get_context()
        
        return {
            'daily_pnl': context.daily_pnl,
            'floating_pnl': context.floating_pnl,
            'total_pnl': context.total_pnl,
            'risk_breached': context.risk_breached,
            'trade_authorized': context.trade_authorized,
            'daily_loss_limit': self.config.DAILY_LOSS_LIMIT
        }
        
    def _update_backward_compat_state(self) -> None:
        """Update state properties for backward compatibility."""
        context = self._executor.get_context()
        self.trade_authorized = context.trade_authorized
        
    # Additional methods for testing/debugging
    
    def get_context(self):
        """Get the trading context (for testing/debugging)."""
        return self._executor.get_context()
        
    def force_restriction_check(self, date_helper: DateHelper) -> None:
        """Force a restriction check (for testing)."""
        self._executor._update_context(date_helper)
        self._executor._apply_restrictions()
        self._update_backward_compat_state()