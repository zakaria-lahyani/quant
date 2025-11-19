from collections import deque
import numpy as np

from app.indicators.batch.bollinger_bands import bollinger_bands_batch


class BollingerBands:
    def __init__(self, window, num_std_dev):
        self.window = window
        self.num_std_dev = num_std_dev
        self.close_window = deque(maxlen=window)
        self.prev_upper = None
        self.prev_middle = None
        self.prev_lower = None
        self.prev_percent_b = None

    def update(self, new_close):
        self.close_window.append(new_close)
        if len(self.close_window) < self.window:
            return None, None, None, None

        sma = sum(self.close_window) / self.window
        std = np.std(self.close_window, ddof=0)

        upper = sma + (std * self.num_std_dev)
        lower = sma - (std * self.num_std_dev)
        middle = sma

        percent_b = (new_close - lower) / (upper - lower) if upper != lower else 0.0

        self.prev_upper = upper
        self.prev_middle = middle
        self.prev_lower = lower
        self.prev_percent_b = percent_b

        return upper, middle, lower, percent_b

    def batch_update(self, close):
        close = np.asarray(close, dtype=np.float64)
        return bollinger_bands_batch(close, self.window, self.num_std_dev)
