from app.indicators.incremental.ema import EMA
from app.indicators.batch.macd import macd_batch_update
import numpy as np

class MACD:
    def __init__(self, fast, slow, signal):
        self.fast_ema = EMA(fast)
        self.slow_ema = EMA(slow)
        self.signal_ema = EMA(signal)
        self.fast = fast
        self.slow = slow
        self.signal = signal
        self.macd_line = None
        self.signal_line = None

    def update(self, price):
        fast_ema = self.fast_ema.update(price)
        slow_ema = self.slow_ema.update(price)

        if None in (fast_ema, slow_ema):
            return None, None, None

        self.macd_line = fast_ema - slow_ema
        self.signal_line = self.signal_ema.update(self.macd_line)

        return self.macd_line, self.signal_line, self.macd_line - self.signal_line

    def batch_update(self, prices):
        prices = np.asarray(prices, dtype=np.float64)
        return macd_batch_update(prices, self.fast, self.slow, self.signal)
