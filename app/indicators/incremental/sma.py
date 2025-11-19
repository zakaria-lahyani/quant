from collections import deque
import numpy as np

from app.indicators.batch.sma import sma_batch


class SMA:
    def __init__(self, period):
        self.period = period
        self.window = deque(maxlen=period)
        self.sma = None

    def update(self, value):
        self.window.append(value)
        if len(self.window) < self.period:
            return None
        self.sma = sum(self.window) / self.period
        return self.sma

    def batch_update(self, values):
        values = np.asarray(values, dtype=np.float64)
        return sma_batch(values, self.period)
