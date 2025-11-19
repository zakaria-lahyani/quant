import numpy as np
from numba import njit

from app.indicators.batch.atr import compute_true_range
from app.indicators.batch.ema import ema_numba


@njit
def keltner_channel_batch(highs, lows, closes, ema_window, atr_window, multiplier):
    n = len(closes)

    typical_price = (highs + lows + closes) / 3.0

    # Calculate EMA of typical price using your ema_numba
    ema = ema_numba(typical_price, ema_window)

    # Calculate True Range using your compute_true_range
    tr = compute_true_range(highs, lows, closes)

    # Calculate ATR (SMA of TR)
    atr = np.full(n, np.nan)
    cumsum_tr = np.cumsum(tr)
    for i in range(atr_window - 1, n):
        if i == atr_window - 1:
            atr[i] = cumsum_tr[i]
        else:
            atr[i] = cumsum_tr[i] - cumsum_tr[i - atr_window]
        atr[i] /= atr_window

    # Calculate Keltner Channels and %K
    upper = np.full(n, np.nan)
    middle = np.full(n, np.nan)
    lower = np.full(n, np.nan)
    percent_k = np.full(n, np.nan)

    for i in range(n):
        if not np.isnan(ema[i]) and not np.isnan(atr[i]):
            upper[i] = ema[i] + multiplier * atr[i]
            middle[i] = ema[i]
            lower[i] = ema[i] - multiplier * atr[i]
            if upper[i] != lower[i]:
                percent_k[i] = (closes[i] - lower[i]) / (upper[i] - lower[i])
            else:
                percent_k[i] = 0.0

    return upper, middle, lower, percent_k
