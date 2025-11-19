from collections import deque
import numpy as np

from app.indicators.batch.ultimate_rsi import ultimate_rsi_batch
from app.indicators.incremental.rma import RMA

class UltimateRsi:
    def __init__(self, src='close', length=14, smooth_length=14):
        self.src = src
        self.length = length
        self.smooth_length = smooth_length
        self.window = deque(maxlen=length)
        self.prev_upper = None
        self.prev_lower = None
        self.num_rma = RMA(length)
        self.den_rma = RMA(length)
        self.signal_rma = RMA(smooth_length)
        self.prev_close = None

    def update(self, new_row):
        new_value = new_row[self.src]
        # Capture previous close BEFORE updating for current step
        old_close = self.prev_close
        self.prev_close = new_value
        self.window.append(new_value)

        if len(self.window) < self.length:
            return np.nan, np.nan

        upper = max(self.window)
        lower = min(self.window)
        r = upper - lower
        d = new_value - old_close if old_close is not None else 0.0

        # First full window: use price difference (d)
        if self.prev_upper is None:
            diff = d
        # Subsequent windows: apply conditional logic
        else:
            if upper > self.prev_upper:
                diff = r
            elif lower < self.prev_lower:
                diff = -r
            else:
                diff = d

        abs_diff = abs(diff)
        num_val = self.num_rma.update(diff)
        den_val = self.den_rma.update(abs_diff)

        # Compute URSI
        if den_val is None or den_val == 0 or np.isnan(den_val):
            ursi = np.nan
        else:
            ursi = (num_val / den_val) * 50 + 50

        # Compute signal
        signal = np.nan
        if not np.isnan(ursi):
            signal = self.signal_rma.update(ursi)

        # Update state for next iteration
        self.prev_upper = upper
        self.prev_lower = lower

        return ursi, signal

    def batch_update(self, df):
        prices = np.asarray(df[self.src], dtype=np.float64)
        return ultimate_rsi_batch(prices, self.length, self.smooth_length)

