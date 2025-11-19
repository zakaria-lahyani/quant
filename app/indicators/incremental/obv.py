import numpy as np
from app.indicators.incremental.ema import EMA


class OBV:
    def __init__(self, period):
        self.ema = EMA(period)
        self.prev_close = None
        self.obv = 0

    def update(self, close, volume):
        if self.prev_close is None:
            self.prev_close = close
            # For first value, update EMA with initial OBV (0)
            obv_ema = self.ema.update(self.obv)
            return self.obv, obv_ema

        if close > self.prev_close:
            self.obv += volume
        elif close < self.prev_close:
            self.obv -= volume
        # If close == prev_close, OBV stays the same

        self.prev_close = close

        # Update EMA with current OBV
        obv_ema = self.ema.update(self.obv)
        obv_osciliator = self.obv - obv_ema

        return self.obv, obv_osciliator

    def batch_update(self, close, volume):
        close = np.asarray(close)
        volume = np.asarray(volume)

        # Compute daily price direction: +1 (up), -1 (down), 0 (same)
        direction = np.sign(np.diff(close))

        # direction with 0 for first day
        direction = np.insert(direction, 0, 0)

        # Multiply direction by volume
        signed_volume = direction * volume

        # Cumulative sum to get OBV
        obv = np.cumsum(signed_volume)

        # Calculate EMA of OBV
        obv_ema = self.ema.batch_update(obv)
        obv_osciliator = obv - obv_ema

        return obv, obv_osciliator

