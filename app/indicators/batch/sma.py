import numpy as np
from numba import njit

@njit
def sma_batch(values, period):
    length = len(values)
    sma = np.full(length, np.nan)
    if length < period:
        return sma

    window_sum = 0.0
    for i in range(period):
        window_sum += values[i]
    sma[period - 1] = window_sum / period

    for i in range(period, length):
        window_sum += values[i] - values[i - period]
        sma[i] = window_sum / period

    return sma
