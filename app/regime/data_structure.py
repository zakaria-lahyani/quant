from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Optional, NamedTuple

import numpy as np
import pandas as pd

# ================= Data Structures =================

@dataclass
class BarData:
    """Represents a single price bar."""
    timestamp: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    bar_index: int


@dataclass
class IndicatorValues:
    """Container for all indicator values at a point in time."""
    rsi: Optional[float] = None
    atr_ratio: Optional[float] = None
    bb_width: Optional[float] = None
    macd_hist: Optional[float] = None
    ema20: Optional[float] = None
    ema50: Optional[float] = None
    ema200: Optional[float] = None
    ema_slope: Optional[float] = None


@dataclass
class RegimeSnapshot:
    """Snapshot of regime state at a point in time."""
    timestamp: pd.Timestamp
    bar_index: int
    regime: str
    confidence: float
    indicators: IndicatorValues
    is_transition: bool = False
    htf_bias: str = "neutral"

    def to_dict(self) -> Dict:
        return {
            "timestamp": str(self.timestamp),
            "bar_index": self.bar_index,
            "regime": self.regime,
            "confidence": float(self.confidence),
            "indicators": {
                k: (float(v) if isinstance(v, (int, float, np.floating)) and v is not None else v)
                for k, v in self.indicators.__dict__.items()
            },
            "is_transition": bool(self.is_transition),
            "htf_bias": self.htf_bias,
        }


class ClassificationResult(NamedTuple):
    """Result of regime classification."""
    direction: str  # "bull", "bear", "neutral"
    volatility: str  # "expansion", "contraction"
    confidence: float
    dir_score: int


@dataclass
class IndicatorState:
    """Manages the state of all technical indicators."""

    # EMA states
    ema12: Optional[float] = None
    ema26: Optional[float] = None
    ema20: Optional[float] = None
    ema50: Optional[float] = None
    ema200: Optional[float] = None
    ema20_prev: Optional[float] = None

    # MACD states
    macd_signal: Optional[float] = None

    # RSI states
    rsi_avg_gain: Optional[float] = None
    rsi_avg_loss: Optional[float] = None

    # ATR states
    atr14: Optional[float] = None
    atr50: Optional[float] = None

    # Price tracking
    prev_close: Optional[float] = None

    # Windows for calculations
    close_window: deque = field(default_factory=lambda: deque(maxlen=200))
    bb_history: deque = field(default_factory=lambda: deque(maxlen=200))
