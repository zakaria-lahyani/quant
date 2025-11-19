"""
Core domain models for trading strategies.
"""

from datetime import datetime
from typing import Annotated, Any, List, Literal, Optional, Union
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
    confloat,
    conint
)
import re

from app.strategy_builder.core.domain.enums import (
    TimeFrameEnum,
    DaysEnum,
    ConditionOperatorEnum,
    LogicModeEnum,
    SessionHandlingEnum,
    PositionSizingTypeEnum
)


# ---------------------------
# Core Condition Models
# ---------------------------

class Condition(BaseModel):
    """Represents a single trading condition."""
    model_config = ConfigDict(extra="forbid", validate_default=True)

    signal: str = Field(..., min_length=1)
    operator: ConditionOperatorEnum
    value: Union[float, str, bool, List[Any]]
    timeframe: TimeFrameEnum
    lookback: conint(ge=1) = 1
    required_confidence: confloat(ge=0, le=1) = 0.8

    @model_validator(mode="after")
    def validate_operator_value(self) -> "Condition":
        """Validate operator-value compatibility."""
        if self.operator in [ConditionOperatorEnum.IN, ConditionOperatorEnum.NOT_IN]:
            if not isinstance(self.value, list):
                raise ValueError(f"{self.operator.value} operator requires array value")
        elif isinstance(self.value, list):
            raise ValueError(f"Array value not allowed for {self.operator.value} operator")
        return self


class ConditionTree(BaseModel):
    """Represents a tree of conditions with logical operators."""
    model_config = ConfigDict(extra="forbid")

    operator: Literal["and", "or", "not"]
    conditions: List[Union[Condition, "ConditionTree"]]

    @model_validator(mode="after")
    def validate_structure(self) -> "ConditionTree":
        """Validate tree structure."""
        if self.operator == "not" and len(self.conditions) != 1:
            raise ValueError("NOT operator requires exactly one condition")
        if self.operator in ["and", "or"] and len(self.conditions) < 2:
            raise ValueError(f"{self.operator.upper()} operator requires at least two conditions")
        return self


# Enable forward references
ConditionTree.model_rebuild()


# ---------------------------
# Schedule and Activation Models
# ---------------------------

class Schedule(BaseModel):
    """Trading schedule configuration."""
    model_config = ConfigDict(extra="forbid")

    days: List[DaysEnum] = []
    hours: str
    session_handling: Optional[SessionHandlingEnum] = None

    @field_validator("hours")
    @classmethod
    def validate_hours_format(cls, v: str) -> str:
        """Validate hours format (HH:MM-HH:MM)."""
        if not re.match(r"^\d{2}:\d{2}-\d{2}:\d{2}$", v):
            raise ValueError("Invalid hours format. Use HH:MM-HH:MM")

        start, end = v.split("-")
        start_h, start_m = map(int, start.split(":"))
        end_h, end_m = map(int, end.split(":"))

        if not (0 <= start_h <= 23 and 0 <= start_m <= 59):
            raise ValueError("Invalid start time")
        if not (0 <= end_h <= 23 and 0 <= end_m <= 59):
            raise ValueError("Invalid end time")
        if start >= end:
            raise ValueError("Start time must be before end time")

        return v


class Activation(BaseModel):
    """Strategy activation configuration."""
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    schedule: Optional[Schedule] = None


# ---------------------------
# Risk Management Models
# ---------------------------

class PositionSizing(BaseModel):
    """Position sizing configuration."""
    model_config = ConfigDict(extra="forbid")

    type: PositionSizingTypeEnum
    value: confloat(ge=0)
    atr_distance: Optional[confloat(ge=0)] = None


class TrailingStopLoss(BaseModel):
    """Trailing stop loss configuration."""
    model_config = ConfigDict(extra="forbid")

    enabled: Optional[confloat(ge=0)] = None
    step: confloat(ge=0)


class FixedStopLoss(BaseModel):
    """Fixed stop loss configuration."""
    model_config = ConfigDict(extra="forbid")

    type: Literal["fixed"]
    value: confloat(ge=0)
    trailing: Optional[TrailingStopLoss] = None


class TrailingStopLossOnly(BaseModel):
    """Standalone trailing stop loss configuration."""
    model_config = ConfigDict(extra="forbid")

    type: Literal["trailing"]
    step: confloat(ge=0)
    activation_price: Optional[confloat(ge=0)] = None
    cap: Optional[confloat(ge=0)] = None


class IndicatorBasedSlTp(BaseModel):
    """Indicator-based stop loss/take profit configuration."""
    model_config = ConfigDict(extra="forbid")

    type: Literal["indicator"]
    source: str = Field(..., min_length=1)
    offset: confloat(ge=0) = 0
    timeframe: TimeFrameEnum
    trailing: Optional[TrailingStopLoss] = None


class MonetaryStopLoss(BaseModel):
    """Monetary stop loss configuration."""
    model_config = ConfigDict(extra="forbid")

    type: Literal["monetary"]
    value: confloat(gt=0)  # Dollar amount to risk
    trailing: Optional[bool] = False


StopLoss = Annotated[
    Union[FixedStopLoss, TrailingStopLossOnly, IndicatorBasedSlTp, MonetaryStopLoss],
    Field(discriminator="type")
]


class FixedTakeProfit(BaseModel):
    """Fixed take profit configuration."""
    model_config = ConfigDict(extra="forbid")

    type: Literal["fixed"]
    value: confloat(ge=0)


class TakeProfitTarget(BaseModel):
    """Individual take profit target."""
    model_config = ConfigDict(extra="forbid")

    value: confloat(ge=0)
    percent: confloat(ge=0, le=100)
    move_stop: bool = False


class MultiTargetTakeProfit(BaseModel):
    """Multi-target take profit configuration."""
    model_config = ConfigDict(extra="forbid")

    type: Literal["multi_target"]
    targets: List[TakeProfitTarget] = Field(..., min_length=1)


TakeProfit = Annotated[
    Union[FixedTakeProfit, MultiTargetTakeProfit, IndicatorBasedSlTp],
    Field(discriminator="type")
]


class RiskManagement(BaseModel):
    """Risk management configuration."""
    model_config = ConfigDict(extra="forbid")

    position_sizing: Optional[PositionSizing] = None
    sl: StopLoss
    tp: TakeProfit


# ---------------------------
# Entry/Exit Components
# ---------------------------

class ProfitGuard(BaseModel):
    """Profit guard configuration."""
    model_config = ConfigDict(extra="forbid")

    max_drawdown: Optional[confloat(ge=0)] = None
    trailing: Optional[confloat(ge=0)] = None


class TimeBasedExit(BaseModel):
    """Time-based exit configuration."""
    model_config = ConfigDict(extra="forbid")

    max_duration: Optional[str] = None
    min_duration: Optional[str] = None

    @field_validator("max_duration", "min_duration")
    @classmethod
    def validate_duration_format(cls, v: str) -> str:
        """Validate duration format (<number><m|h|d|w>)."""
        if not re.match(r"^\d+[mhdw]$", v):
            raise ValueError("Invalid duration format. Use <number><m|h|d|w>")
        return v


class BaseRuleSet(BaseModel):
    """Base class for rule sets."""
    model_config = ConfigDict(extra="forbid")

    mode: LogicModeEnum
    conditions: Optional[List[Condition]] = None
    tree: Optional[ConditionTree] = None

    @model_validator(mode="after")
    def validate_rule_structure(self) -> "BaseRuleSet":
        """Validate rule structure based on mode."""
        if self.mode == LogicModeEnum.COMPLEX:
            if not self.tree:
                raise ValueError("Condition tree required in complex mode")
            if self.conditions:
                raise ValueError("Cannot use conditions array in complex mode")
        else:
            # For simple modes, allow empty conditions for ExitRules with time_based or profit_guard
            if not self.conditions:
                # Check if this is an ExitRules instance with alternative exit mechanisms
                if hasattr(self, 'time_based') or hasattr(self, 'profit_guard'):
                    # Allow empty conditions if time_based or profit_guard are present
                    pass
                else:
                    raise ValueError("Conditions array required in simple modes")
            if self.tree:
                raise ValueError("Cannot use tree in simple modes")
        return self


class EntryRules(BaseRuleSet):
    """Entry rules configuration."""
    mode: LogicModeEnum = Field(default=LogicModeEnum.ALL)


class ExitRules(BaseRuleSet):
    """Exit rules configuration."""
    mode: LogicModeEnum = Field(default=LogicModeEnum.ANY)
    time_based: Optional[TimeBasedExit] = None
    profit_guard: Optional[ProfitGuard] = None


class EntryDirectionalRules(BaseModel):
    """Directional entry rules (long/short)."""
    long: Optional[EntryRules] = None
    short: Optional[EntryRules] = None


class ExitDirectionalRules(BaseModel):
    """Directional exit rules (long/short)."""
    long: Optional[ExitRules] = None
    short: Optional[ExitRules] = None


# ---------------------------
# Main Strategy Model
# ---------------------------

class StrategyMeta(BaseModel):
    """Strategy metadata."""
    version: Optional[str] = Field(None, pattern=r"^\d+\.\d+\.\d+$")
    description: Optional[str] = Field(None, max_length=500)
    author: Optional[str] = None
    created_at: Optional[datetime] = None


class StrategyConfig(BaseModel):
    """Reusable configuration for YAML anchors and references."""
    model_config = ConfigDict(extra="allow")

    # Allow any fields to be defined dynamically for maximum flexibility


class TradingStrategy(BaseModel):
    """Complete trading strategy configuration."""
    model_config = ConfigDict(extra="forbid", validate_default=True)

    # Required api fields
    name: str = Field(..., min_length=1, max_length=100)
    timeframes: List[TimeFrameEnum] = Field(..., min_length=1)
    entry: Optional[EntryDirectionalRules] = None
    exit: Optional[ExitDirectionalRules] = None
    risk: RiskManagement

    # Optional metadata and configuration
    meta: Optional[StrategyMeta] = None
    activation: Optional[Activation] = None
    config: Optional[StrategyConfig] = Field(None, description="Reusable configuration for YAML anchors")

    @field_validator("meta")
    @classmethod
    def validate_meta(cls, value: Optional[dict]) -> Optional[dict]:
        """Validate metadata fields."""
        if value:
            if "version" in value:
                if not re.match(r"^\d+\.\d+\.\d+$", value["version"]):
                    raise ValueError("Invalid semantic version format")
            if "created_at" in value:
                try:
                    datetime.fromisoformat(value["created_at"])
                except ValueError:
                    raise ValueError("Invalid ISO 8601 datetime format")
        return value

    @model_validator(mode="after")
    def validate_timeframe_consistency(self) -> "TradingStrategy":
        """Validate timeframe consistency across conditions."""
        strategy_timeframes = set(self.timeframes)

        def check_conditions(conditions: List[Condition]) -> None:
            for condition in conditions:
                if condition.timeframe not in strategy_timeframes:
                    raise ValueError(
                        f"Condition timeframe {condition.timeframe} "
                        f"not in strategy timeframes"
                    )

        def check_rule_set(rules: Union[EntryRules, ExitRules]) -> None:
            if rules.mode != LogicModeEnum.COMPLEX:
                if rules.conditions:
                    check_conditions(rules.conditions)
            if rules.tree:
                def traverse_tree(tree: ConditionTree):
                    for condition in tree.conditions:
                        if isinstance(condition, Condition):
                            if condition.timeframe not in strategy_timeframes:
                                raise ValueError(
                                    f"Tree condition timeframe {condition.timeframe} "
                                    f"not in strategy timeframes"
                                )
                        elif isinstance(condition, ConditionTree):
                            traverse_tree(condition)

                traverse_tree(rules.tree)

        # Validate entry rules
        if self.entry:
            if self.entry.long:
                check_rule_set(self.entry.long)
            if self.entry.short:
                check_rule_set(self.entry.short)

        # Validate exit rules
        if self.exit:
            if self.exit.long:
                check_rule_set(self.exit.long)
            if self.exit.short:
                check_rule_set(self.exit.short)

        return self

    @model_validator(mode="after")
    def validate_exit_profit_guard(self) -> "TradingStrategy":
        """Validate profit guard configuration."""

        def check_profit_guard(rules: ExitRules):
            if rules.profit_guard:
                if not any([rules.time_based, rules.conditions, rules.tree]):
                    raise ValueError("Profit guard requires at least one exit condition")

        if self.exit:
            if self.exit.long:
                check_profit_guard(self.exit.long)
            if self.exit.short:
                check_profit_guard(self.exit.short)
        return self

    @model_validator(mode="after")
    def validate_manual_strategy_requirements(self) -> "TradingStrategy":
        """
        Validate that automated strategies have entry/exit sections.
        Manual strategies (name='manual') can omit entry/exit sections
        since they are triggered via API signals, not automated conditions.
        """
        is_manual_strategy = self.name == "manual"

        if not is_manual_strategy and not self.entry:
            raise ValueError(
                "Entry section is required for automated strategies. "
                "Only strategies with name='manual' can omit entry/exit sections, "
                "as they are triggered via API signals rather than automated evaluation."
            )

        return self

    def model_dump_json(self, **kwargs):
        """Serialize to JSON with round-trip support."""
        return super().model_dump_json(round_trip=True, **kwargs)