"""
ADX (Average Directional Index) Module
======================================

This module provides both batch and incremental implementations of the Average Directional Movement Index (ADX),
along with the Positive Directional Indicator (+DI) and Negative Directional Indicator (-DI).

It supports:
- High-performance batch processing using Numba
- Stateful, bar-by-bar updates for streaming or real-time systems

Key Features
------------
- **[`Batch Mode`](../indicators/batch/adx.html)**:
    - Implemented via `adx_batch_numba`, compiled with Numba for near-native speed.
    - Ideal for fast backtesting, scanning large datasets, or offline analysis.
    - Uses Wilder's RMA for smoothing.

- **Incremental ADX**:
    - Implemented in the `ADX` class.
    - Processes one OHLC bar at a time using internal state and RMA logic.
    - Suitable for live trading pipelines or tick-by-tick processing.
    - Returns ADX, +DI, and -DI values after the warm-up period.

Functions
---------
- `adx_batch_numba(high, low, close, period)`:
    Computes ADX, +DI, and -DI in batch mode using Numba-accelerated logic.

Classes
-------
- `ADX(period, max_di=100.0)`:
    Stateful, incremental ADX calculator.
    - `.update(high, low, close)` — update with a new OHLC bar.
    - `.batch_update(high, low, close)` — convenience method that calls `adx_batch_numba`.

Usage Examples
--------------

**Batch Mode**
 indicators.batch.adx

adx, plus_di, minus_di = adx_batch_numba(high, low, close, period=14)
"""

from collections import deque
import numpy as np

from app.indicators.batch.adx import adx_batch_numba
from app.indicators.incremental.rma import RMA


class ADX:
    """
    Incremental ADX calculator using Wilder's RMA smoothing.

    This class maintains internal state to compute ADX, +DI, and -DI values one bar at a time.
    It is suitable for real-time or streaming use cases where values arrive incrementally.

    Parameters
    ----------
    period : int
        Lookback period for ADX calculation (typically 14).
    max_di : float, optional
        Maximum clamp value for +DI and -DI to prevent extreme spikes (default is 100.0).

    Attributes
    ----------
    adx : float or None
        Current ADX value after warm-up.
    bars_processed : int
        Number of OHLC bars processed.
    """
    def __init__(self, period, max_di=100.0):
        self.period = period
        self.max_di = max_di
        self.prev_high = None
        self.prev_low = None
        self.prev_close = None

        self.rma_tr = RMA(period)
        self.rma_plus_dm = RMA(period)
        self.rma_minus_dm = RMA(period)

        self.dx_values = deque(maxlen=period)
        self.adx = None
        self.bars_processed = 0

    def update(self, high, low, close):
        """
        Update the ADX calculation with a new OHLC bar.

        Parameters
        ----------
        high : float
            High price of the current bar.
        low : float
            Low price of the current bar.
        close : float
            Close price of the current bar.

        Returns
        -------
        tuple or None
            Returns a tuple `(adx, plus_di, minus_di)` once the warm-up period is complete,
            else returns None.
        """
        if self.prev_high is None:
            self.prev_high = high
            self.prev_low = low
            self.prev_close = close
            self.bars_processed += 1
            return None

        # True Range
        tr = max(high - low, abs(high - self.prev_close), abs(low - self.prev_close))
        tr = max(tr, 1e-8)  # prevent division by 0

        # Directional Movement
        up_move = high - self.prev_high
        down_move = self.prev_low - low

        plus_dm = up_move if (up_move > down_move and up_move > 0) else 0
        minus_dm = down_move if (down_move > up_move and down_move > 0) else 0

        tr_rma = self.rma_tr.update(tr)
        plus_dm_rma = self.rma_plus_dm.update(plus_dm)
        minus_dm_rma = self.rma_minus_dm.update(minus_dm)

        self.bars_processed += 1

        if tr_rma is None or np.isnan(tr_rma):
            self.prev_high = high
            self.prev_low = low
            self.prev_close = close
            return None

        plus_di = 100 * (plus_dm_rma / tr_rma)
        minus_di = 100 * (minus_dm_rma / tr_rma)

        plus_di = min(plus_di, self.max_di)
        minus_di = min(minus_di, self.max_di)

        di_sum = plus_di + minus_di
        dx = 0 if di_sum == 0 else 100 * abs(plus_di - minus_di) / di_sum
        dx = min(dx, 100)

        self.dx_values.append(dx)

        if self.adx is None:
            if len(self.dx_values) == self.period:
                self.adx = np.mean(self.dx_values)
        else:
            self.adx = (self.adx * (self.period - 1) + dx) / self.period

        self.adx = min(self.adx, 100.0) if self.adx is not None else None

        # Update state
        self.prev_high = high
        self.prev_low = low
        self.prev_close = close

        if self.adx is not None:
            return round(self.adx, 6), round(plus_di, 6), round(minus_di, 6)
        return None

    def batch_update(self, high, low, close):
        """
        Compute ADX, +DI, and -DI values in batch using the internal Numba-accelerated function.

        This is a convenience wrapper that mirrors the behavior of `adx_batch_numba`.

        Parameters
        ----------
        high : array-like
            High prices as a list or NumPy array.
        low : array-like
            Low prices.
        close : array-like
            Close prices.

        Returns
        -------
        tuple of np.ndarray
            Tuple of (adx, plus_di, minus_di) arrays.
        """
        high = np.asarray(high, dtype=np.float64)
        low = np.asarray(low, dtype=np.float64)
        close = np.asarray(close, dtype=np.float64)
        return adx_batch_numba(high, low, close, self.period )
