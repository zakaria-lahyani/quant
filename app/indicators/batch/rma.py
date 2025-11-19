from numba import njit
import numpy as np

@njit
def compute_rma(values, period):
    n = len(values)
    rma = np.full(n, np.nan)
    acc = 0.0
    count = 0

    for i in range(n):
        if not np.isnan(values[i]):
            if count < period:
                acc += values[i]
                count += 1
                if count == period:
                    acc /= period
                    rma[i] = acc
            else:
                acc = (acc * (period - 1) + values[i]) / period
                rma[i] = acc
    return rma


