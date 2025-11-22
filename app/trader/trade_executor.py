"""
Trade Executor V3 - Simplified and focused on orchestration only.
"""

import logging
from datetime import datetime
from typing import Optional

from app.strategy_builder.data.dtos import Trades, EntryDecision
from app.utils.date_helper import DateHelper
from app.infrastructure.events.runtime_control_store import RuntimeControlStore

from .trading_context import TradingContext, MarketState
from .components.exit_manager import ExitManager
from .components.duplicate_filter import DuplicateFilter
from .components.risk_monitor import RiskMonitor
from .components.order_executor import OrderExecutor
from app.trader.managers.restriction_manager import RestrictionManager
from .live_trader import LiveTrader


class TradeExecutor:
    """
    Simplified Trade Executor - focuses only on orchestration.
    
    Responsibilities:
    - Orchestrate the trading workflow
    - Delegate to specialized components
    - Maintain trading context
    """
    
    def __init__(
        self,
        trader: LiveTrader,
        exit_manager: ExitManager,
        duplicate_filter: DuplicateFilter,
        risk_monitor: RiskMonitor,
        order_executor: OrderExecutor,
        restriction_manager: RestrictionManager,
        symbol: str,
        runtime_controls: Optional[RuntimeControlStore] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize with pre-configured components.

        Args:
            trader: Live trader for market data
            exit_manager: Handles position exits
            duplicate_filter: Filters duplicate trades
            risk_monitor: Monitors risk limits
            order_executor: Executes orders
            restriction_manager: Manages trading restrictions
            symbol: Trading symbol
            runtime_controls: Optional runtime control store for trading toggles
            logger: Optional logger
        """
        self.trader = trader
        self.exit_manager = exit_manager
        self.duplicate_filter = duplicate_filter
        self.risk_monitor = risk_monitor
        self.order_executor = order_executor
        self.restriction_manager = restriction_manager
        self.symbol = symbol
        self.runtime_controls = runtime_controls
        self.logger = logger or logging.getLogger('trade-executor-v3')

        # Trading context
        self.context = TradingContext()
        
    def execute_trading_cycle(self, trades: Trades, date_helper: DateHelper) -> TradingContext:
        """
        Execute a single trading cycle.
        
        This is the main orchestration method that coordinates all trading operations.
        
        Args:
            trades: Trade signals to process
            date_helper: Date helper for time calculations
            
        Returns:
            Updated trading context with results
        """
        self.logger.info("=== TRADING CYCLE START ===")
        
        try:
            # Step 1: Update context
            self._update_context(date_helper)
            
            # Step 2: Apply restrictions
            self._apply_restrictions()
            
            # Step 3: Process exits (always allowed)
            self._process_exits(trades)
            
            # Step 4: Check risk limits
            self._check_risk_limits()
            
            # Step 5: Process entries (if allowed)
            self._process_entries(trades)
            
        except Exception as e:
            self.logger.error(f"Error in trading cycle: {e}", exc_info=True)
            self.context.block_trading(f"error: {str(e)}")
            
        finally:
            self.logger.info(f"=== TRADING CYCLE END (authorized={self.context.trade_authorized}, risk_breached={self.context.risk_breached}) ===")
            
        return self.context
        
    def _update_context(self, date_helper: DateHelper) -> None:
        """Update trading context with current state."""
        self.logger.debug("Updating trading context")
        
        # Update time
        self.context.current_time = datetime.now()
        
        # Fetch market state
        market_state = self._fetch_market_state(date_helper)
        self.context.update_market_state(market_state)
        
    def _fetch_market_state(self, date_helper: DateHelper) -> MarketState:
        """Fetch current market state."""
        start_date = f"{date_helper.get_date_days_ago(0)}T00:00:00Z"
        end_date = f"{date_helper.get_date_days_ago(-1)}T00:00:00Z"
        
        return MarketState(
            open_positions=self.trader.get_open_positions(self.symbol),
            pending_orders=self.trader.get_pending_orders(self.symbol),
            closed_positions=self.trader.get_closed_positions(start_date, end_date),
            timestamp=self.context.current_time
        )
        
    def _apply_restrictions(self) -> None:
        """Apply trading restrictions."""
        self.logger.debug("Applying trading restrictions")
        self.restriction_manager.apply_restrictions(self.context)
        
    def _process_exits(self, trades: Trades) -> None:
        """Process exit signals."""
        if not trades.exits:
            self.logger.debug("No exit signals to process")
            return
            
        self.logger.info(f"Processing {len(trades.exits)} exit signals")
        self.exit_manager.process_exits(
            trades.exits,
            self.context.market_state.open_positions
        )
        
    def _check_risk_limits(self) -> None:
        """Check risk limits and update context."""
        self.logger.debug("Checking risk limits")
        
        risk_breached = self.risk_monitor.check_catastrophic_loss_limit(
            self.context.market_state.open_positions,
            self.context.market_state.closed_positions
        )
        
        self.context.set_risk_breach(risk_breached)
        
        # Update PnL metrics
        risk_metrics = self.risk_monitor.get_risk_metrics(
            self.context.market_state.open_positions,
            self.context.market_state.closed_positions
        )
        
        self.context.daily_pnl = risk_metrics['daily_pnl']
        self.context.floating_pnl = risk_metrics['floating_pnl']
        self.context.total_pnl = risk_metrics['total_pnl']
        
    def _process_entries(self, trades: Trades) -> None:
        """Process entry signals if trading is allowed."""
        if not trades.entries:
            self.logger.debug("No entry signals to process")
            return

        self.logger.info(f"Processing {len(trades.entries)} entry signals")

        # Check if trading is allowed
        if not self.context.can_trade():
            reasons = []
            if not self.context.trade_authorized:
                reasons.append("not authorized")
            if self.context.risk_breached:
                reasons.append("risk breach")
            self.logger.warning(f"Entry processing blocked: {', '.join(reasons)}")
            return

        # Filter duplicates
        filtered_entries = self.duplicate_filter.filter_entries(
            trades.entries,
            self.context.market_state.open_positions,
            self.context.market_state.pending_orders
        )

        if not filtered_entries:
            self.logger.info("All entries filtered out (duplicates)")
            return

        # Apply runtime controls if available
        if self.runtime_controls:
            filtered_entries = self._apply_runtime_controls(filtered_entries)

        if filtered_entries:
            self.logger.info(f"Executing {len(filtered_entries)} filtered entries")
            self.order_executor.execute_entries(filtered_entries)
        else:
            self.logger.info("All entries blocked by runtime controls")

    def _apply_runtime_controls(self, entries: list[EntryDecision]) -> list[EntryDecision]:
        """
        Apply runtime controls to filter entries.

        Checks:
        1. Master trading switch (blocks all)
        2. Auto-trading switch (blocks non-manual only)
        3. Per-strategy toggles

        Args:
            entries: List of entry decisions to filter

        Returns:
            Filtered list of entries that pass runtime controls
        """
        allowed_entries = []

        for entry in entries:
            is_manual = getattr(entry, 'is_manual', False)
            strategy_name = entry.strategy_name

            can_execute, reason = self.runtime_controls.can_execute_trade(
                strategy_name=strategy_name,
                is_manual=is_manual
            )

            if can_execute:
                allowed_entries.append(entry)
            else:
                self.logger.warning(
                    f"Entry blocked by runtime control: {strategy_name} "
                    f"{'(manual)' if is_manual else '(auto)'} - {reason}"
                )

        return allowed_entries
            
    def get_context(self) -> TradingContext:
        """Get current trading context."""
        return self.context