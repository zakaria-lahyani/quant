from typing import Optional, Dict, Any, List

from app.strategy_builder.data.dtos import EntryDecision
from app.trader.risk_manager.models import ScalingConfig, PositionGroup, ScaledPosition, PositionType, RiskEntryResult
from app.trader.risk_manager.stop_loss_calculator import MonetaryStopLossCalculator, PositionEntry
import logging
import uuid

from app.trader.risk_manager.stop_loss_planner import StopLossPlanner, build_scaled_position, build_limit_order


class RiskCalculator:
    """
    1. Takes entry decision and splits into multiple limit orders
    2. Sends all orders at once
    3. Manages position groups for stop loss coordination
    4. Updates positions each iteration
    """
    def __init__(
            self,
            scaling_config: ScalingConfig,
            group_stop_loss: bool = True,
            logger: Optional[logging.Logger] = None
    ):
        self.scaling_config = scaling_config
        self.group_stop_loss = group_stop_loss  # True = single group SL, False = individual SLs
        self.logger = logger or logging.getLogger(self.__class__.__name__)

        # Track position groups
        self.position_groups: Dict[str, PositionGroup] = {}
        self.active_tickets: Dict[str, str] = {}  # ticket_id -> group_id

    def _create_position_group(self, entry_decision: EntryDecision, group_id: str) -> PositionGroup:
        """Create position group from entry decision."""
        return PositionGroup(
            group_id=group_id,
            symbol=entry_decision.symbol,
            strategy_name=entry_decision.strategy_name,
            direction=entry_decision.direction,
            original_decision=entry_decision,
            total_target_size=entry_decision.position_size,
            num_entries=self.scaling_config.num_entries,
            scaling_strategy=self.scaling_config.scaling_type,
            total_risk_amount=self.scaling_config.max_risk_per_group
        )

    def _calculate_scaled_entry_prices(self, current_price: float, direction: str) -> List[
        float]:
        """Calculate entry prices for scaled positions."""
        entry_prices = []
        is_long = direction.lower() == 'long'

        # Use current price as base for spacing
        base_price = current_price
        spacing_amount = base_price * (self.scaling_config.entry_spacing / 100.0)

        for i in range(self.scaling_config.num_entries):
            if is_long:
                # For long: spread entries below current price (buy the dip)
                entry_price = base_price - (spacing_amount * i)
            else:
                # For short: spread entries above current price (sell the rally)
                entry_price = base_price + (spacing_amount * i)

            entry_prices.append(entry_price)

        return entry_prices

    def process_entry_signal(self,
                             entry_decision: "EntryDecision",
                             current_price: float) -> RiskEntryResult:
        """
        Orchestrates: group creation, scaling, SL planning, position & order building, result payload.
        """

        # 1) Create group
        group_id = str(uuid.uuid4())
        group = self._create_position_group(entry_decision, group_id)

        # 2) Compute scaling
        entry_prices = self._calculate_scaled_entry_prices(current_price, entry_decision.direction)
        size_ratios = self.scaling_config.get_size_ratios()

        # 3) Plan stop losses (delegated)
        sl_planner = StopLossPlanner(logger=self.logger,
                                     scaling_config=self.scaling_config,
                                     group_stop_loss=self.group_stop_loss)
        sl_plan = sl_planner.plan(entry_decision, entry_prices, size_ratios)

        # 4) Build positions & orders
        limit_orders: List[Dict[str, Any]] = []
        scaled_sizes = [entry_decision.position_size * r for r in size_ratios]

        for i in range(self.scaling_config.num_entries):
            pos = build_scaled_position(
                i=i,
                group_id=group_id,
                entry_price=entry_prices[i],
                position_size=scaled_sizes[i],
                stop_loss=sl_plan.adjusted_stops[i],
                entry_decision=entry_decision
            )
            # apply_take_profit_targets(pos, entry_decision)
            group.add_position(pos)

            order = build_limit_order(
                symbol=entry_decision.symbol,
                direction=entry_decision.direction,
                volume=scaled_sizes[i],
                price=entry_prices[i],
                group_sl_level=sl_plan.group_level,
                use_group_sl=(sl_plan.mode == "group"),
                strategy_name=entry_decision.strategy_name,
                magic=entry_decision.magic
            )
            limit_orders.append(order)
            
            # Debug logging
            if i == 0:  # Log only once
                self.logger.debug(f"Stop loss plan group_level: {sl_plan.group_level}")
                self.logger.debug(f"Order group_stop_loss: {order.get('group_stop_loss')}")

            # track ticket -> group mapping
            self.active_tickets[pos.position_id] = group_id

        # 5) Persist group-level SL & group
        if sl_plan.mode == "group":
            group.group_stop_loss = sl_plan.group_level
        self.position_groups[group_id] = group

        # 6) Shape result
        result = RiskEntryResult(
            group_id=group_id,
            limit_orders=limit_orders,
            total_orders=len(limit_orders),
            total_size=entry_decision.position_size,
            scaled_sizes=scaled_sizes,
            entry_prices=entry_prices,
            stop_losses=sl_plan.adjusted_stops,
            group_stop_loss=sl_plan.group_level if sl_plan.mode == "group" else None,
            stop_loss_mode=sl_plan.mode,
            original_risk=self.scaling_config.max_risk_per_group,
            take_profit=entry_decision.take_profit,
            calculated_risk=sl_plan.calculated_risk,
            weighted_avg_entry=sl_plan.details.get('weighted_avg_price'),
            stop_calculation_method=sl_plan.calculation_method,
            strategy_name=entry_decision.strategy_name,
            magic=entry_decision.magic
        )

        self.logger.info(
            f"[Orders] Created {len(limit_orders)} scaled limit orders for "
            f"{entry_decision.symbol} {entry_decision.direction} (Group: {group_id[:8]})"
        )
        return result

    def process_entries(self, entries: list[EntryDecision], current_price: float) -> List[RiskEntryResult]:
        processed_entries = []
        for entry in entries:
            processed_ = self.process_entry_signal(entry, current_price)
            processed_entries.append(processed_)

        return processed_entries