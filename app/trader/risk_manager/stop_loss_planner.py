from __future__ import annotations
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from app.strategy_builder.data.dtos import EntryDecision
from app.trader.risk_manager.models import ScaledPosition, PositionType
from app.trader.risk_manager.stop_loss_calculator import PositionEntry, MonetaryStopLossCalculator


# --- Small, focused data carriers ------------------------------------------------

@dataclass(frozen=True)
class StopLossPlan:
    mode: str  # 'group' or 'individual'
    calculation_method: str  # 'monetary' or 'price_level'
    group_level: Optional[float]          # None if individual
    adjusted_stops: List[Optional[float]] # per-position SLs; None means no individual SL
    details: Dict[str, Any]               # calculator-specific extra info (weighted avg, etc.)
    calculated_risk: float

# --- Helpers ---------------------------------------------------------------------

def build_position_entries(entry_prices: List[float],
                           total_size: float,
                           size_ratios: List[float]) -> List["PositionEntry"]:
    """Pure helper to create PositionEntry list used by the SL calculator."""
    return [
        PositionEntry(entry_price=entry_prices[i],
                      position_size=total_size * size_ratios[i])
        for i in range(len(entry_prices))
    ]

class StopLossPlanner:
    """
    Encapsulates the branching around group vs individual SLs and which method to use.
    Keeps all SL-related decisions and logging in one place.
    """
    def __init__(self, *, logger, scaling_config, group_stop_loss: bool):
        self.logger = logger
        self.scaling_config = scaling_config
        self.group_stop_loss = group_stop_loss

    def plan(self,
             entry_decision: "EntryDecision",
             entry_prices: List[float],
             size_ratios: List[float]) -> StopLossPlan:
        original_stop_level = entry_decision.stop_loss.level if entry_decision.stop_loss else None

        if not self.group_stop_loss:
            # Simple: all positions share the original stop level; no calculator
            adjusted = [original_stop_level] * len(entry_prices)
            return StopLossPlan(
                mode="individual",
                calculation_method="price_level" if (original_stop_level and entry_decision.stop_loss and entry_decision.stop_loss.type != 'monetary') else 'monetary',
                group_level=None,
                adjusted_stops=adjusted,
                details={},
                calculated_risk=self.scaling_config.max_risk_per_group
            )

        # Group SL with calculator
        calculator = MonetaryStopLossCalculator(entry_decision.symbol, self.logger)
        position_entries = build_position_entries(entry_prices,
                                                  entry_decision.position_size,
                                                  size_ratios)

        sl_details: Dict[str, Any] = {}

        if original_stop_level:
            if entry_decision.stop_loss.type == 'monetary':
                group_sl_level, sl_details = calculator.calculate_group_stop_loss(
                    position_entries,
                    self.scaling_config.max_risk_per_group,
                    entry_decision.direction
                )
                self.logger.info(f"[SL] Using monetary risk: ${self.scaling_config.max_risk_per_group}")
                method = 'monetary'
            else:
                group_sl_level, sl_details = calculator.calculate_group_stop_loss_from_price_level(
                    entries=position_entries,
                    original_entry_price=entry_decision.entry_price,
                    original_stop_price=original_stop_level,
                    original_position_size=entry_decision.position_size,
                    direction=entry_decision.direction
                )
                self.logger.info(f"[SL] Using price-level method: entry={entry_decision.entry_price}, stop={original_stop_level}")
                method = 'price_level'
        else:
            group_sl_level, sl_details = calculator.calculate_group_stop_loss(
                position_entries,
                self.scaling_config.max_risk_per_group,
                entry_decision.direction
            )
            self.logger.info(f"[SL] No original stop provided -> monetary risk ${self.scaling_config.max_risk_per_group}")
            method = 'monetary'

        self.logger.info(f"[SL] Details: {sl_details}")

        calculated_risk = sl_details.get('calculated_total_risk', self.scaling_config.max_risk_per_group)

        return StopLossPlan(
            mode="group",
            calculation_method=method,
            group_level=group_sl_level,
            adjusted_stops=[None] * len(entry_prices),
            details=sl_details,
            calculated_risk=calculated_risk
        )

def build_scaled_position(i: int,
                          group_id: str,
                          entry_price: float,
                          position_size: float,
                          stop_loss: Optional[float],
                          entry_decision: "EntryDecision") -> "ScaledPosition":
    return ScaledPosition(
        position_id=f"{group_id}_pos_{i+1}",
        group_id=group_id,
        symbol=entry_decision.symbol,
        direction=entry_decision.direction,
        entry_price=entry_price,
        position_size=position_size,
        stop_loss_level=stop_loss,
        position_type=PositionType.INITIAL if i == 0 else PositionType.SCALE_IN,
        strategy_name=entry_decision.strategy_name,
        magic_number=entry_decision.magic
    )


def build_limit_order(symbol: str,
                      direction: str,
                      volume: float,
                      price: float,
                      group_sl_level: Optional[float],
                      use_group_sl: bool,
                      strategy_name: Optional[str] = None,
                      magic: Optional[int] = None) -> Dict[str, Any]:
    return {
        'symbol': symbol,
        'order_type': 'BUY_LIMIT' if direction.lower() == 'long' else 'SELL_LIMIT',
        'volume': volume,
        'price': price,
        'group_stop_loss': group_sl_level if use_group_sl else None,
        'strategy_name': strategy_name,
        'magic': magic
    }
