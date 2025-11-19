import numpy as np
from collections import deque

from app.indicators.batch.keltner_channel import keltner_channel_batch
from app.indicators.incremental.ema import EMA


class KeltnerChannel:
    def __init__(self, ema_window=20, atr_window=10, multiplier=2):
        self.ema = EMA(ema_window)
        self.ema_window = ema_window
        self.atr_window = atr_window
        self.multiplier = multiplier
        self.previous_close = None  # Track previous close for True Range
        self.true_ranges = deque(maxlen=atr_window)  # Stores TR values for ATR
        # Optional: Store previous values
        self.prev_upper = None
        self.prev_middle = None
        self.prev_lower = None
        self.prev_percent_k = None

    def update(self, high, low, close):
        # Calculate Typical Price (used for EMA)
        typical_price = (high + low + close) / 3.0

        # Update EMA with the new typical price
        current_ema = self.ema.update(typical_price)

        # Calculate True Range (TR)
        if self.previous_close is None:
            tr = high - low  # First TR uses only high-low
        else:
            tr = max(
                high - low,
                abs(high - self.previous_close),
                abs(low - self.previous_close)
            )
        self.previous_close = close  # Update for next iteration
        self.true_ranges.append(tr)

        # Check if we have enough data to compute ATR
        if len(self.true_ranges) < self.atr_window:
            return None, None, None, None  # Not enough TR values yet

        # Calculate ATR (SMA of TR values)
        atr = sum(self.true_ranges) / self.atr_window

        # Calculate Keltner Channels
        upper = current_ema + (self.multiplier * atr)
        middle = current_ema  # EMA of typical price
        lower = current_ema - (self.multiplier * atr)

        # Calculate %B (position within the bands)
        if upper != lower:
            percent_k = (close - lower) / (upper - lower)
        else:
            percent_k = 0.0  # Avoid division by zero

        # Store previous values (optional)
        self.prev_upper = upper
        self.prev_middle = middle
        self.prev_lower = lower
        self.prev_percent_k = percent_k

        return upper, middle, lower, percent_k

    def batch_update(self, highs, lows, closes):
        highs = np.asarray(highs, dtype=np.float64)
        lows = np.asarray(lows, dtype=np.float64)
        closes = np.asarray(closes, dtype=np.float64)
        return keltner_channel_batch(highs, lows, closes, self.ema_window, self.atr_window, self.multiplier)
