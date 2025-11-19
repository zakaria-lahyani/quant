from dataclasses import dataclass, field
from typing import Optional
import pandas as pd

from app.regime.indicator_utilities import ema_update


# ================= HTF Bias Calculator =================

@dataclass
class HTFState:
    """State for Higher Timeframe bias calculation."""
    rule: Optional[str] = None
    bucket: Optional[pd.Timestamp] = None
    last_close: Optional[float] = None
    ema12: Optional[float] = None
    ema26: Optional[float] = None
    ema200: Optional[float] = None
    macd_signal: Optional[float] = None
    bias: str = "neutral"


class HTFBiasCalculator:
    """Calculates Higher Timeframe bias."""

    def __init__(self, htf_rule: Optional[str] = None):
        self.state = HTFState(rule=htf_rule)

    def update(self, timestamp: pd.Timestamp, close: float) -> str:
        """Update HTF bias and return current bias."""
        if not self.state.rule:
            return "neutral"

        bucket = timestamp.floor(self.state.rule)

        if self.state.bucket is None:
            self.state.bucket = bucket
            self.state.last_close = close
            return self.state.bias

        if bucket != self.state.bucket:
            # HTF bar closed - update indicators
            self._update_htf_indicators()
            self._calculate_bias()

            # Move to new bucket
            self.state.bucket = bucket
            self.state.last_close = close
        else:
            # Still in same bucket
            self.state.last_close = close

        return self.state.bias

    def _update_htf_indicators(self) -> None:
        """Update HTF indicators when a HTF bar closes."""
        if self.state.last_close is None:
            return

        close = self.state.last_close
        self.state.ema12 = ema_update(self.state.ema12, close, 12)
        self.state.ema26 = ema_update(self.state.ema26, close, 26)
        self.state.ema200 = ema_update(self.state.ema200, close, 200)

        if self.state.ema12 is not None and self.state.ema26 is not None:
            macd_line = self.state.ema12 - self.state.ema26
            self.state.macd_signal = ema_update(self.state.macd_signal, macd_line, 9)

    def _calculate_bias(self) -> None:
        """Calculate HTF bias from indicators."""
        if (self.state.ema200 is None or
                self.state.macd_signal is None or
                self.state.ema12 is None or
                self.state.ema26 is None or
                self.state.last_close is None):
            return

        macd_line = self.state.ema12 - self.state.ema26
        hist = macd_line - self.state.macd_signal
        close = self.state.last_close

        if close > self.state.ema200 and hist > 0:
            self.state.bias = "bull"
        elif close < self.state.ema200 and hist < 0:
            self.state.bias = "bear"
        else:
            self.state.bias = "neutral"
