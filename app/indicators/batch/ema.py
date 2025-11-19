import numpy as np
from numba import njit

@njit
def ema_numba(values, period):
    n = len(values)
    alpha = 2 / (period + 1)
    ema = np.full(n, np.nan)

    # Find first valid index
    start_idx = -1
    for i in range(n):
        if not np.isnan(values[i]):
            start_idx = i
            break
    if start_idx == -1:
        return ema  # no valid data

    ema[start_idx] = values[start_idx]

    for i in range(start_idx + 1, n):
        if not np.isnan(values[i]):
            ema[i] = alpha * values[i] + (1 - alpha) * ema[i - 1]
        else:
            ema[i] = ema[i - 1]

    return ema
