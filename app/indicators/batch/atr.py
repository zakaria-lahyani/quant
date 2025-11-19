import numpy as np
from numba import njit

@njit
def compute_true_range(high, low, close):
    n = len(close)
    tr = np.empty(n)

    tr[0] = high[0] - low[0]  # First TR without previous close

    for i in range(1, n):
        tr1 = high[i] - low[i]
        tr2 = abs(high[i] - close[i - 1])
        tr3 = abs(low[i] - close[i - 1])
        tr[i] = max(tr1, tr2, tr3)

    return tr


@njit
def compute_atr(tr, window):
    n = len(tr)
    atr = np.full(n, np.nan)

    if n < window:
        return atr

    # First ATR = Simple Moving Average
    atr[window - 1] = np.mean(tr[:window])

    for i in range(window, n):
        atr[i] = (atr[i - 1] * (window - 1) + tr[i]) / window

    return atr
