from numba import njit
import numpy as np

@njit
def aroon_batch_numba(highs, lows, period):
    n = len(highs)
    aroon_up = np.full(n, np.nan)
    aroon_down = np.full(n, np.nan)

    for i in range(period, n):  # <- window is period+1, so start from period
        high_window = highs[i - period : i + 1]  # includes i
        low_window = lows[i - period : i + 1]

        days_since_high = period - np.argmax(high_window)
        days_since_low = period - np.argmin(low_window)

        aroon_up[i] = ((period - days_since_high) / period) * 100
        aroon_down[i] = ((period - days_since_low) / period) * 100

    return aroon_up, aroon_down
