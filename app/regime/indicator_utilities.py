from collections import deque
from typing import Optional

import numpy as np

# ================= Pure Utility Functions =================

def ema_update(prev: Optional[float], price: float, period: int) -> float:
    """Update EMA incrementally."""
    alpha = 2.0 / (period + 1.0)
    return price if prev is None else (alpha * price + (1 - alpha) * prev)


def wilder_update(prev: Optional[float], value: float, period: int) -> float:
    """Update using Wilder's smoothing method."""
    return value if prev is None else (prev + (value - prev) / period)


def true_range(high: float, low: float, prev_close: Optional[float]) -> float:
    """Calculate True Range for a bar."""
    if prev_close is None:
        return high - low
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def bb_width_normalized(prices_window: deque, period: int = 20, k: float = 2.0) -> float:
    """Calculate normalized Bollinger Band width."""
    if len(prices_window) == 0:
        return 0.0
    n = min(period, len(prices_window))
    arr = np.array(list(prices_window)[-n:])
    mean = arr.mean()
    std = arr.std(ddof=0)
    if mean == 0.0:
        return 0.0
    upper = mean + k * std
    lower = mean - k * std
    return (upper - lower) / mean


def safe_clip(value: float, min_val: float, max_val: float) -> float:
    """Safely clip a value to a range."""
    return float(np.clip(value, min_val, max_val))


