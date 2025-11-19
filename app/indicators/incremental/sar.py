import numpy as np

from app.indicators.batch.sar import sar_batch


class SAR:
    def __init__(self, acceleration, max_acceleration):
        self.acceleration = acceleration
        self.max_acceleration = max_acceleration
        self.trend = None
        self.ep = None  # Extreme Price
        self.af = acceleration  # Acceleration Factor
        self.sar = None
        self.prev_high = None
        self.prev_low = None

    def update(self, high, low):
        if self.sar is None:  # Initialization
            self.sar = low
            self.trend = 'bullish'
            self.ep = high
            return self.sar

        reversal = False
        new_sar = self.sar

        if self.trend == 'bullish':
            new_sar += self.af * (self.ep - self.sar)
            if new_sar > low:
                reversal = True
                new_sar = max(self.ep, high)
        else:
            new_sar += self.af * (self.ep - self.sar)
            if new_sar < high:
                reversal = True
                new_sar = min(self.ep, low)

        if reversal:
            self.trend = 'bearish' if self.trend == 'bullish' else 'bullish'
            self.af = self.acceleration
            self.ep = high if self.trend == 'bullish' else low
        else:
            if self.trend == 'bullish':
                if high > self.ep:
                    self.ep = high
                    self.af = min(self.af + self.acceleration, self.max_acceleration)
            else:
                if low < self.ep:
                    self.ep = low
                    self.af = min(self.af + self.acceleration, self.max_acceleration)

        self.sar = new_sar
        return self.sar

    def batch_update(self, highs, lows):
        highs = np.asarray(highs, dtype=np.float64)
        lows = np.asarray(lows, dtype=np.float64)
        return sar_batch(highs, lows, self.acceleration, self.max_acceleration)
