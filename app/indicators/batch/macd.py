import numpy as np
from numba import njit

from app.indicators.batch.ema import ema_numba


@njit
def macd_batch_update(prices, fast=12, slow=26, signal=9):
    prices = np.asarray(prices, dtype=np.float64)
    fast_ema = ema_numba(prices, fast)
    slow_ema = ema_numba(prices, slow)

    macd_line = fast_ema - slow_ema
    signal_line = ema_numba(macd_line, signal)
    macd_histogram = macd_line - signal_line

    return macd_line, signal_line, macd_histogram
