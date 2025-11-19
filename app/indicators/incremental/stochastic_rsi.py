from collections import deque
import numpy as np
import pandas as pd

from app.indicators.incremental.rsi import RSI
from app.indicators.incremental.sma import SMA


class StochasticRSI:
    def __init__(self, rsi_period, stochrsi_period, k_smooth, d_smooth):
        self.rsi = RSI(rsi_period, rsi_period)
        self.stoch_period = stochrsi_period
        self.rsi_window = deque(maxlen=stochrsi_period)
        self.k_ma = SMA(k_smooth)
        self.d_ma = SMA(d_smooth)

    def update(self, price):
        result = self.rsi.update(price)
        if result is None:
            return None, None
        rsi, _ = result

        self.rsi_window.append(rsi)

        if len(self.rsi_window) < self.stoch_period:
            return None, None

        current_rsi = rsi
        lowest = min(self.rsi_window)
        highest = max(self.rsi_window)

        if highest == lowest:
            stoch = 0
        else:
            stoch = 100 * (current_rsi - lowest) / (highest - lowest)

        k = self.k_ma.update(stoch)
        d = self.d_ma.update(k) if k is not None else None

        return k, d

    def rolling_min_max_ignore_nan(self, arr, window):
        arr = pd.Series(arr)
        return arr.rolling(window=window, min_periods=window).min().to_numpy(), \
            arr.rolling(window=window, min_periods=window).max().to_numpy()

    def batch_update_sma(self, values,period):
        values = np.asarray(values, dtype=np.float64)
        sma = np.full_like(values, np.nan)

        if len(values) < period:
            return sma

        for i in range(period - 1, len(values)):
            window = values[i - period + 1: i + 1]
            if np.any(np.isnan(window)):
                continue
            sma[i] = np.mean(window)

        return sma

    def batch_update(self, prices):
        prices = np.asarray(prices, dtype=np.float64)

        # Step 1: Compute RSI batch
        from app.indicators.batch.rsi import rsi_batch  # assume you have this
        rsi_values = rsi_batch(prices, self.rsi.period)

        # Step 2: Compute Stochastic RSI (%K unsmoothed)
        min_rsi, max_rsi = self.rolling_min_max_ignore_nan(rsi_values, self.stoch_period)

        # Avoid divide-by-zero
        denom = max_rsi - min_rsi
        denom[denom == 0] = np.nan  # to match incremental behavior

        stoch = 100 * (rsi_values - min_rsi) / denom
        k = self.batch_update_sma(stoch, self.k_ma.period )
        d = self.batch_update_sma(k, self.d_ma.period )

        return k, d
