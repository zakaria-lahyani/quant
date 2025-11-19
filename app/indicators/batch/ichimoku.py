import numpy as np
from numba import njit

@njit
def ichimoku_batch_numba(highs, lows, closes, tenkan_period, kijun_period, senkou_b_period, chikou_shift):
    n = len(highs)

    tenkan = np.full(n, np.nan)
    kijun = np.full(n, np.nan)
    senkou_a = np.full(n, np.nan)
    senkou_b = np.full(n, np.nan)
    chikou = np.full(n, np.nan)
    cloud_code = np.zeros(n, dtype=np.int8)

    for i in range(n):
        # Tenkan-sen
        if i >= tenkan_period - 1:
            h = highs[i - tenkan_period + 1:i + 1]
            l = lows[i - tenkan_period + 1:i + 1]
            tenkan[i] = (np.max(h) + np.min(l)) / 2

        # Kijun-sen
        if i >= kijun_period - 1:
            h = highs[i - kijun_period + 1:i + 1]
            l = lows[i - kijun_period + 1:i + 1]
            kijun[i] = (np.max(h) + np.min(l)) / 2

        # Senkou Span A
        if not np.isnan(tenkan[i]) and not np.isnan(kijun[i]):
            senkou_a[i] = (tenkan[i] + kijun[i]) / 2

        # Senkou Span B
        if i >= senkou_b_period - 1:
            h = highs[i - senkou_b_period + 1:i + 1]
            l = lows[i - senkou_b_period + 1:i + 1]
            senkou_b[i] = (np.max(h) + np.min(l)) / 2

        # Chikou Span
        lag_idx = i - chikou_shift
        if lag_idx >= 0:
            chikou[i] = closes[lag_idx]

        # Cloud type
        if not np.isnan(senkou_a[i]) and not np.isnan(senkou_b[i]):
            cloud_code[i] = 1 if senkou_a[i] > senkou_b[i] else -1

    return tenkan, kijun, senkou_a, senkou_b, chikou, cloud_code


def decode_cloud(cloud_code):
    return np.where(cloud_code == 1, 'bullish', np.where(cloud_code == -1, 'bearish', None))
