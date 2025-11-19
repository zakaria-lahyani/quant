from numba import njit
import numpy as np

@njit
def compute_rma(values, period):
    n = len(values)
    rma = np.full(n, np.nan)
    acc = 0.0
    count = 0

    for i in range(n):
        val = values[i]
        if np.isnan(val):
            continue

        if count < period:
            acc += val
            count += 1
            if count == period:
                acc /= period
                rma[i] = acc
        else:
            acc = (acc * (period - 1) + val) / period
            rma[i] = acc
    return rma

class RMA:
    def __init__(self, period):
        self.period = period
        self.value = None
        self.initial_values = []
        self.ready = False

    def update(self, new_value):
        if not self.ready:
            self.initial_values.append(new_value)
            if len(self.initial_values) == self.period:
                self.value = np.mean(self.initial_values)
                self.ready = True
            return self.value if self.ready else np.nan
        else:
            self.value = (self.value * (self.period - 1) + new_value) / self.period
            return self.value

    def batch_update(self, values):
        values = np.asarray(values, dtype=np.float64)
        return compute_rma(values, self.period)
