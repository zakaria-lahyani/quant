from collections import deque
import numpy as np
from app.indicators.batch.aroon import aroon_batch_numba

class Aroon:
    def __init__(self, period):
        self.period = period
        self.highs = deque(maxlen=period + 1)
        self.lows = deque(maxlen=period + 1)

    def update(self, high, low):
        self.highs.append(high)
        self.lows.append(low)

        if len(self.highs) < self.period + 1:
            return np.nan, np.nan

        high_list = list(self.highs)
        low_list = list(self.lows)

        # Index of most recent highest high (0 means most recent)
        days_since_high = len(high_list) - 1 - np.argmax(high_list)
        days_since_low = len(low_list) - 1 - np.argmin(low_list)

        aroon_up = ((self.period - days_since_high) / self.period) * 100
        aroon_down = ((self.period - days_since_low) / self.period) * 100

        return aroon_up, aroon_down

    def batch_update(self, highs, lows):
        highs = np.asarray(highs, dtype=np.float64)
        lows = np.asarray(lows, dtype=np.float64)
        return aroon_batch_numba(highs, lows, self.period)
