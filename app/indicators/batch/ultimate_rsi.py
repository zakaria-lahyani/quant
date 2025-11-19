from numba import njit
import numpy as np

from app.indicators.incremental.rma import compute_rma


@njit
def ultimate_rsi_batch(prices, length, smooth_length):
    n = len(prices)
    ursi = np.full(n, np.nan)
    signal = np.full(n, np.nan)

    # Rolling window buffers for max/min
    window = np.full(length, np.nan)
    window_count = 0

    prev_upper = np.nan
    prev_lower = np.nan
    prev_close = np.nan

    # Buffers for RMA computations
    diff_arr = np.full(n, np.nan)
    abs_diff_arr = np.full(n, np.nan)

    for i in range(n):
        new_value = prices[i]

        # Shift window left and add new_value
        if window_count < length:
            window[window_count] = new_value
            window_count += 1
        else:
            # Shift left and add new value at the end
            for j in range(length - 1):
                window[j] = window[j + 1]
            window[length - 1] = new_value

        if window_count < length:
            # Not enough data yet
            prev_upper = np.nan
            prev_lower = np.nan
            prev_close = new_value
            continue

        upper = window[0]
        lower = window[0]
        for k in range(1, length):
            if window[k] > upper:
                upper = window[k]
            if window[k] < lower:
                lower = window[k]

        r = upper - lower
        if np.isnan(prev_close):
            d = 0.0
        else:
            d = new_value - prev_close

        if i == length - 1:
            diff = d
        else:
            condition1 = not np.isnan(prev_upper) and upper > prev_upper
            condition2 = not np.isnan(prev_lower) and lower < prev_lower

            if condition1:
                diff = r
            elif condition2:
                diff = -r
            else:
                diff = d

        diff_arr[i] = diff
        abs_diff_arr[i] = abs(diff)

        prev_upper = upper
        prev_lower = lower
        prev_close = new_value

    # Compute RMA for numerator and denominator
    num_rma = compute_rma(diff_arr, length)
    den_rma = compute_rma(abs_diff_arr, length)

    # Compute URSI values
    for i in range(n):
        if np.isnan(num_rma[i]) or np.isnan(den_rma[i]) or den_rma[i] == 0:
            ursi[i] = np.nan
        else:
            ursi[i] = (num_rma[i] / den_rma[i]) * 50 + 50

    # Compute signal RMA on URSI
    signal = compute_rma(ursi, smooth_length)

    return ursi, signal
