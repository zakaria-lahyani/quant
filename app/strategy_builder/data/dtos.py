"""
Data Transfer Objects for the strategy engine.
"""

from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Optional, Literal


@dataclass
class SignalResult:
    """Result of signal evaluation."""
    long: Optional[bool] = None
    short: Optional[bool] = None


@dataclass
class StrategyEvaluationResult:
    """Result of strategy evaluation."""
    strategy_name: str
    entry: SignalResult
    exit: SignalResult


@dataclass
class AllStrategiesEvaluationResult:
    """Result of all strategies evaluation."""
    strategies: dict[str, StrategyEvaluationResult]


@dataclass
class TPLevel:
    """Take profit level configuration."""
    level: float
    value: float
    percent: float
    move_stop: Optional[float] = None


@dataclass
class TakeProfitResult:
    """Take profit execution result."""
    type: Literal["fixed", "multi_target", "indicator"]
    level: Optional[float] = None  # For fixed TP
    source: Optional[str] = None  # For indicator-based TP
    percent: Optional[float] = None  # For fixed TP
    targets: Optional[List[TPLevel]] = field(default=None)  # For multi-target


@dataclass
class StopLossResult:
    """Stop loss execution result."""
    type: Literal["fixed", "indicator"]

    # Common for fixed & indicator
    level: float

    # Trailing-specific
    step: Optional[float] = None
    trailing: Optional[bool] = None

    # Indicator-specific
    source: Optional[str] = None


@dataclass
class EntryDecision:
    """Entry decision data."""
    symbol: str
    strategy_name: str
    magic: int
    direction: str
    entry_signals: Literal['BUY', 'SELL', 'BUY_LIMIT', 'SELL_LIMIT']
    entry_price: float
    position_size: float
    stop_loss: StopLossResult
    take_profit: TakeProfitResult
    decision_time: datetime
    is_manual: bool = False  # True for manual signals, affects order comment prefix


@dataclass
class ExitDecision:
    """Exit decision data."""
    symbol: str
    strategy_name: str
    magic: int
    direction: Literal['long', 'short']
    decision_time: datetime
    close_percent: float = 100.0  # Percentage of position to close (1-100)
    is_manual: bool = False  # True for manual exit signals


@dataclass
class Trades:
    """Collection of trading decisions."""
    entries: List[EntryDecision]
    exits: List[ExitDecision]