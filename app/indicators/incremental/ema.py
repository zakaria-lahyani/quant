import numpy as np

from app.indicators.batch.ema import ema_numba


class EMA:
    def __init__(self, period):
        self.period = period
        self.alpha = 2 / (period + 1)
        self.ema = None

    def update(self, value):
        if self.ema is None:
            self.ema = value
        else:
            self.ema = self.alpha * value + (1 - self.alpha) * self.ema
        return self.ema

    def batch_update(self, values):
        values = np.asarray(values, dtype=np.float64)
        return ema_numba(values, self.period)
