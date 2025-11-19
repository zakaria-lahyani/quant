import numpy as np
from numba import njit

@njit
def sar_batch(highs, lows, acceleration, max_acceleration):
    length = len(highs)
    sar = np.full(length, np.nan)
    af = acceleration
    trend = 1  # 1 = bullish, -1 = bearish

    # Initialize
    sar[0] = lows[0]
    ep = highs[0]

    for i in range(1, length):
        prev_sar = sar[i - 1]
        new_sar = prev_sar + af * (ep - prev_sar)

        reversal = False
        if trend == 1:  # bullish
            if new_sar > lows[i]:
                reversal = True
                trend = -1
                sar[i] = ep
                ep = lows[i]
                af = acceleration
            else:
                sar[i] = new_sar
                if highs[i] > ep:
                    ep = highs[i]
                    af = min(af + acceleration, max_acceleration)
        else:  # bearish
            if new_sar < highs[i]:
                reversal = True
                trend = 1
                sar[i] = ep
                ep = highs[i]
                af = acceleration
            else:
                sar[i] = new_sar
                if lows[i] < ep:
                    ep = lows[i]
                    af = min(af + acceleration, max_acceleration)

    return sar
