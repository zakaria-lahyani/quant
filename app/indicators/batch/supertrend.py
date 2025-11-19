import numpy as np
from numba import njit


@njit
def supertrend_batch_numba(high, low, close, atr, multiplier):
    n = len(close)
    hl2 = (high + low) / 2
    upperband = hl2 + multiplier * atr
    lowerband = hl2 - multiplier * atr

    final_upper = np.full(n, np.nan)
    final_lower = np.full(n, np.nan)
    supertrend = np.full(n, np.nan)
    trend = np.empty(n, dtype=np.int8)  # 1 = bullish, -1 = bearish, 0 = unknown

    # Find first valid ATR index
    start = -1
    for i in range(n):
        if not np.isnan(atr[i]):
            start = i
            break

    if start == -1:
        return supertrend, trend

    # Initialize
    final_upper[start] = upperband[start]
    final_lower[start] = lowerband[start]
    trend[start] = 1  # bullish
    supertrend[start] = final_lower[start]

    for i in range(start + 1, n):
        prev_trend = trend[i - 1]
        prev_final_upper = final_upper[i - 1]
        prev_final_lower = final_lower[i - 1]

        # Determine trend
        if close[i] > prev_final_upper:
            trend[i] = 1
        elif close[i] < prev_final_lower:
            trend[i] = -1
        else:
            trend[i] = prev_trend

        # Update bands
        if trend[i] == 1:
            if trend[i] != prev_trend:
                final_lower[i] = lowerband[i]
            else:
                final_lower[i] = max(lowerband[i], prev_final_lower)
            final_upper[i] = upperband[i]
            supertrend[i] = final_lower[i]
        else:
            if trend[i] != prev_trend:
                final_upper[i] = upperband[i]
            else:
                final_upper[i] = min(upperband[i], prev_final_upper)
            final_lower[i] = lowerband[i]
            supertrend[i] = final_upper[i]

    return supertrend, trend
