"""
Order Executor - Handles order execution via trader.
"""

import logging
from typing import List, Optional, Any

from ..live_trader import LiveTrader
from ..risk_manager.risk_calculator import RiskCalculator
from ..risk_manager.models import RiskEntryResult
from ...strategy_builder.data.dtos import EntryDecision


class OrderExecutor:
    """
    Executes orders through the trader interface.
    Single responsibility: Order execution.
    """
    
    def __init__(
        self,
        trader: LiveTrader,
        risk_calculator: RiskCalculator,
        symbol: str,
        broker_symbol: Optional[str] = None,
        event_bus: Optional[Any] = None,
        logger: logging.Logger = None
    ):
        self.trader = trader
        self.risk_calculator = risk_calculator
        self.symbol = symbol  # Display symbol for logging
        self.broker_symbol = broker_symbol or symbol  # Broker API symbol for orders
        self.event_bus = event_bus
        self.logger = logger or logging.getLogger(self.__class__.__name__)
    
    def execute_entries(self, entries: List[EntryDecision]) -> None:
        """
        Execute entry orders.

        Args:
            entries: List of entry decisions to execute
        """
        if not entries:
            self.logger.debug("No entry signals to process")
            return

        self.logger.info(f"🔍 Executing {len(entries)} entry decision(s)")

        # Use broker_symbol for API calls
        current_price = self.trader.get_current_price(self.broker_symbol)
        self.logger.info(f"🔍 Current {self.symbol} price: {current_price}")

        risk_entries = self.risk_calculator.process_entries(entries, current_price)
        self.logger.info(f"🔍 RiskCalculator created {len(risk_entries)} risk entry group(s)")

        for idx, risk_entry in enumerate(risk_entries):
            self.logger.info(
                f"🔍 Group {idx+1}: {len(risk_entry.limit_orders)} orders, "
                f"sizes={[round(s, 3) for s in risk_entry.scaled_sizes]}, "
                f"prices={[round(p, 2) for p in risk_entry.entry_prices]}"
            )
            self._execute_risk_entry(risk_entry)
    
    def _execute_risk_entry(self, risk_entry: RiskEntryResult) -> None:
        """Execute a single risk entry group."""
        self.logger.info(f"Processing risk entry for group {risk_entry.group_id[:8]}")
        self.logger.info(f"Creating {len(risk_entry.limit_orders)} orders")

        try:
            # Replace symbols in orders with broker_symbol for API calls
            self._replace_symbols_in_orders(risk_entry)

            # Open the orders and check results
            results = self.trader.open_pending_order(trade=risk_entry)
            
            # Process and log results
            self._process_execution_results(results, risk_entry)
            
        except Exception as e:
            self.logger.error(f"Failed to execute risk entry {risk_entry.group_id[:8]}: {e}")
    
    def _process_execution_results(
        self,
        results: List,
        risk_entry: RiskEntryResult
    ) -> None:
        """Process and log execution results."""
        success_count = 0
        successful_tickets = []

        for i, result in enumerate(results):
            if isinstance(result, dict):
                if 'error' in result:
                    self.logger.error(f"Order {i + 1} failed: {result['error']}")
                else:
                    self.logger.info(f"Order {i + 1} result: {result}")
                    if result.get('status') == 'success' or result.get('result'):
                        success_count += 1
                        # Extract ticket if available
                        ticket = result.get('ticket') or result.get('order')
                        if ticket:
                            successful_tickets.append(ticket)
            else:
                self.logger.info(f"Order {i + 1} response: {result}")
                if result:  # Assume non-None/non-empty responses are successful
                    success_count += 1

        total_orders = len(risk_entry.limit_orders)
        self.logger.info(
            f"Execution summary: {success_count}/{total_orders} orders successful "
            f"for group {risk_entry.group_id[:8]}"
        )

        # Publish TradesExecutedEvent if any orders succeeded
        if success_count > 0 and self.event_bus:
            self._publish_trades_executed_event(risk_entry, successful_tickets)

    def _publish_trades_executed_event(
        self,
        risk_entry: RiskEntryResult,
        tickets: List[int]
    ) -> None:
        """
        Publish TradesExecutedEvent for position monitoring.

        Args:
            risk_entry: Risk entry result with TP targets
            tickets: List of successful order tickets
        """
        try:
            from ...events.trade_events import TradesExecutedEvent

            # Extract TP targets from risk_entry
            tp_targets = []
            if risk_entry.take_profit and risk_entry.take_profit.type == 'multi_target':
                tp_targets = [
                    {
                        "level": target.level,
                        "percent": target.percent,
                        "move_stop": target.move_stop
                    }
                    for target in risk_entry.take_profit.targets
                ]

            # Calculate total volume
            total_volume = sum(order['volume'] for order in risk_entry.limit_orders)

            # Determine direction
            direction = risk_entry.direction.lower()  # "BUY" -> "buy" or "SELL" -> "sell"
            if direction == "buy":
                direction = "long"
            elif direction == "sell":
                direction = "short"

            # Publish event
            event = TradesExecutedEvent(
                symbol=risk_entry.symbol,
                direction=direction,
                total_volume=total_volume,
                order_count=len(risk_entry.limit_orders),
                strategy_name=risk_entry.strategy_name,
                metadata={
                    "tp_targets": tp_targets,
                    "tickets": tickets,
                    "group_id": risk_entry.group_id
                }
            )

            self.event_bus.publish(event)
            self.logger.info(
                f"📢 Published TradesExecutedEvent: {risk_entry.symbol} {direction} "
                f"{total_volume} lots, TP targets: {len(tp_targets)}"
            )

        except Exception as e:
            self.logger.error(f"Failed to publish TradesExecutedEvent: {e}", exc_info=True)

    def set_event_bus(self, event_bus: Any) -> None:
        """
        Set the event bus for publishing events.

        Args:
            event_bus: EventBus instance
        """
        self.event_bus = event_bus
        self.logger.debug("Event bus configured for OrderExecutor")

    def _replace_symbols_in_orders(self, risk_entry: RiskEntryResult) -> None:
        """
        Replace display symbols with broker symbols in all orders.

        This ensures that orders sent to the MT5 API use the correct broker-specific
        symbol names (e.g., 'XAUUSD.pro' for ACG broker instead of 'XAUUSD').

        Args:
            risk_entry: Risk entry result containing limit orders
        """
        for order in risk_entry.limit_orders:
            if 'symbol' in order and order['symbol'] == self.symbol:
                order['symbol'] = self.broker_symbol
                self.logger.debug(f"Replaced symbol '{self.symbol}' with broker symbol '{self.broker_symbol}' in order")