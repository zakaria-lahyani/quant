import numpy as np
from collections import deque
from app.indicators.incremental.ema import EMA

from app.indicators.batch.ema import ema_numba
from app.indicators.batch.rsi import rsi_batch


class RSI:
    def __init__(self, period, signal_period):
        self.period = period
        self.signal_period = signal_period
        self.ema = EMA(signal_period)
        self.prev_price = None
        self.avg_gain = 0
        self.avg_loss = 0
        self.gains = deque(maxlen=period)
        self.losses = deque(maxlen=period)

    def update(self, price):
        if self.prev_price is None:
            self.prev_price = price
            return None

        delta = price - self.prev_price
        gain = max(delta, 0)
        loss = abs(min(delta, 0))

        self.gains.append(gain)
        self.losses.append(loss)

        if len(self.gains) < self.period:
            self.prev_price = price
            return None

        # Initial calculation
        if self.avg_gain == 0 and self.avg_loss == 0:
            self.avg_gain = sum(self.gains) / self.period
            self.avg_loss = sum(self.losses) / self.period
        else:
            self.avg_gain = (self.avg_gain * (self.period - 1) + gain) / self.period
            self.avg_loss = (self.avg_loss * (self.period - 1) + loss) / self.period

        rs = self.avg_gain / self.avg_loss if self.avg_loss != 0 else 0
        rsi = 100 - (100 / (1 + rs))

        self.prev_price = price
        rsi_signal = self.ema.update(rsi)
        return rsi, rsi_signal


    def batch_update(self, prices):
        prices = np.asarray(prices, dtype=np.float64)
        rsi = rsi_batch(prices, self.period)
        rsi_signal = ema_numba(rsi, self.signal_period)
        return rsi, rsi_signal
