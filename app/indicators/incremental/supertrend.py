from app.indicators.batch.supertrend import supertrend_batch_numba
from app.indicators.incremental.atr import ATR
import numpy as np


class Supertrend:
    def __init__(self, period, multiplier):
        self.period = period
        self.multiplier = multiplier
        self.atr_calculator = ATR(period)
        self.prev_close = None
        self.final_upper = None
        self.final_lower = None
        self.trend = None

    def update(self, high, low, close):
        atr = self.atr_calculator.update(high, low, close)
        if atr is None:
            return None, None  # Not enough data for ATR

        hl2 = (high + low) / 2
        upper = hl2 + (self.multiplier * atr)
        lower = hl2 - (self.multiplier * atr)

        # Initialize
        if self.final_upper is None or self.final_lower is None:
            self.final_upper = upper
            self.final_lower = lower
            self.trend = 'bullish'
            self.prev_close = close
            return self.final_lower, self.trend

        # Preserve old trend
        prev_trend = self.trend

        # Determine current trend
        if close > self.final_upper:
            self.trend = 'bullish'
        elif close < self.final_lower:
            self.trend = 'bearish'

        # Update final bands based on trend
        if self.trend == 'bullish':
            if self.trend != prev_trend:
                self.final_lower = lower  # Reset lower on trend change
            else:
                self.final_lower = max(lower, self.final_lower)
            self.final_upper = upper  # Reset opposite band to avoid stale values
        else:  # bearish
            if self.trend != prev_trend:
                self.final_upper = upper  # Reset upper on trend change
            else:
                self.final_upper = min(upper, self.final_upper)
            self.final_lower = lower  # Reset opposite band to avoid stale values

        self.prev_close = close
        return (self.final_lower, self.trend) if self.trend == 'bullish' else (self.final_upper, self.trend)

    def batch_update(self, high, low, close):
        high = np.asarray(high, dtype=np.float64)
        low = np.asarray(low, dtype=np.float64)
        close = np.asarray(close, dtype=np.float64)

        atr = self.atr_calculator.batch_update(high, low, close)

        supertrend_vals, trend_flags = supertrend_batch_numba(high, low, close, atr, self.multiplier)
        trend_labels = np.where(trend_flags == 1, 'bullish',
                        np.where(trend_flags == -1, 'bearish', None))

        return supertrend_vals, trend_labels
