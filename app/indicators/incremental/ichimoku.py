import numpy as np
from collections import deque

from app.indicators.batch.ichimoku import ichimoku_batch_numba, decode_cloud


class Ichimoku:
    def __init__(self, tenkan_period, kijun_period, senkou_b_period, chikou_shift):
        self.tenkan_period = tenkan_period
        self.kijun_period = kijun_period
        self.senkou_b_period = senkou_b_period
        self.chikou_shift = chikou_shift

        self.highs = deque(maxlen=senkou_b_period)
        self.lows = deque(maxlen=senkou_b_period)
        self.closes = deque(maxlen=chikou_shift + 1)

    def update(self, high, low, close):
        self.highs.append(high)
        self.lows.append(low)
        self.closes.append(close)

        if len(self.highs) < self.senkou_b_period:
            return None, None, None, None, None, None

        tenkan = (max(list(self.highs)[-self.tenkan_period:]) + min(list(self.lows)[-self.tenkan_period:])) / 2
        kijun = (max(list(self.highs)[-self.kijun_period:]) + min(list(self.lows)[-self.kijun_period:])) / 2
        senkou_a = (tenkan + kijun) / 2
        senkou_b = (max(self.highs) + min(self.lows)) / 2
        chikou = self.closes[0] if len(self.closes) == self.chikou_shift + 1 else None
        cloud = 'bullish' if senkou_a > senkou_b else 'bearish'

        return tenkan, kijun, senkou_a, senkou_b, chikou, cloud

    def batch_update(self, highs, lows, closes):
        highs = np.asarray(highs, dtype=np.float64)
        lows = np.asarray(lows, dtype=np.float64)
        closes = np.asarray(closes, dtype=np.float64)

        tenkan, kijun, senkou_a, senkou_b, chikou, cloud_code = ichimoku_batch_numba(
            highs, lows, closes,
            self.tenkan_period, self.kijun_period,
            self.senkou_b_period, self.chikou_shift
        )
        cloud = decode_cloud(cloud_code)
        return tenkan, kijun, senkou_a, senkou_b, chikou, cloud
