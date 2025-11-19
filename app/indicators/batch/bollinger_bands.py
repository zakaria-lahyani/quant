import numpy as np
from numba import njit


@njit
def bollinger_bands_batch(close, window, num_std_dev):
    n = len(close)

    upper = np.full(n, np.nan)
    middle = np.full(n, np.nan)
    lower = np.full(n, np.nan)
    percent_b = np.full(n, np.nan)

    sum_ = 0.0
    sum_sq = 0.0

    for i in range(n):
        c = close[i]
        sum_ += c
        sum_sq += c * c

        if i >= window:
            old = close[i - window]
            sum_ -= old
            sum_sq -= old * old

        if i >= window - 1:
            mean = sum_ / window
            var = (sum_sq / window) - mean * mean
            std = np.sqrt(var)

            upper[i] = mean + num_std_dev * std
            middle[i] = mean
            lower[i] = mean - num_std_dev * std

            if upper[i] != lower[i]:
                percent_b[i] = (close[i] - lower[i]) / (upper[i] - lower[i])
            else:
                percent_b[i] = 0.0

    return upper, middle, lower, percent_b
