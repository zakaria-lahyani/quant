from collections import deque
import numpy as np

from app.indicators.batch.atr import compute_true_range, compute_atr


class ATR:
    def __init__(self, window):
        self.window = window
        self.prev_close = None
        self.tr_values = deque(maxlen=window)
        self.atr = None

    def _true_range(self, high, low, close):
        tr1 = high - low
        tr2 = abs(high - self.prev_close) if self.prev_close else tr1
        tr3 = abs(low - self.prev_close) if self.prev_close else tr1
        return max(tr1, tr2, tr3)

    def update(self, high, low, close):
        tr = self._true_range(high, low, close)
        self.tr_values.append(tr)
        self.prev_close = close

        if len(self.tr_values) < self.window:
            return None

        if self.atr is None:
            self.atr = sum(self.tr_values) / self.window
        else:
            self.atr = (self.atr * (self.window - 1) + tr) / self.window

        return self.atr

    def batch_update(self, high, low, close):
        high = np.asarray(high, dtype=np.float64)
        low = np.asarray(low, dtype=np.float64)
        close = np.asarray(close, dtype=np.float64)

        tr = compute_true_range(high, low, close)
        atr = compute_atr(tr, self.window)

        return atr
