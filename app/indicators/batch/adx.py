from numba import njit
import numpy as np

from app.indicators.batch.rma import compute_rma


@njit
def adx_batch_numba(high, low, close, period):
    """
    Compute ADX, +DI, and -DI values in batch mode using Numba for acceleration.

    This function calculates the Average Directional Index (ADX) and the Positive and
    Negative Directional Indicators (+DI, -DI) over a vector of price data. It uses
    Wilder’s smoothing technique (RMA) and is optimized for performance using Numba.

    Parameters
    ----------
    high : np.ndarray
        Array of high prices.
    low : np.ndarray
        Array of low prices.
    close : np.ndarray
        Array of close prices.
    period : int
        The period over which to calculate ADX and DIs, typically 14.

    Returns
    -------
    adx : np.ndarray
        ADX values (trend strength indicator).
    plus_di : np.ndarray
        +DI values (bullish strength).
    minus_di : np.ndarray
        -DI values (bearish strength).

    Notes
    -----
    - The first few values will be `np.nan` until the period is fully initialized.
    - All arrays must be the same length.
    - Requires Numba and NumPy to be installed.

    Examples
    --------
    >>> adx, plus_di, minus_di = adx_batch_numba(high, low, close, period=14)
    """
    n = len(high)

    tr = np.zeros(n)
    plus_dm = np.zeros(n)
    minus_dm = np.zeros(n)

    for i in range(1, n):
        tr1 = high[i] - low[i]
        tr2 = abs(high[i] - close[i - 1])
        tr3 = abs(low[i] - close[i - 1])
        tr[i] = max(tr1, tr2, tr3)

        up_move = high[i] - high[i - 1]
        down_move = low[i - 1] - low[i]

        if up_move > down_move and up_move > 0:
            plus_dm[i] = up_move
            minus_dm[i] = 0
        elif down_move > up_move and down_move > 0:
            plus_dm[i] = 0
            minus_dm[i] = down_move

    # Wilder's RMA smoothing
    sm_tr = compute_rma(tr, period)
    sm_plus_dm = compute_rma(plus_dm, period)
    sm_minus_dm = compute_rma(minus_dm, period)

    plus_di = np.full(n, np.nan)
    minus_di = np.full(n, np.nan)
    dx = np.full(n, np.nan)

    for i in range(n):
        if sm_tr[i] > 0:
            plus_di[i] = 100 * sm_plus_dm[i] / sm_tr[i]
            minus_di[i] = 100 * sm_minus_dm[i] / sm_tr[i]
            di_sum = plus_di[i] + minus_di[i]
            if di_sum > 0:
                dx[i] = 100 * abs(plus_di[i] - minus_di[i]) / di_sum

    adx = compute_rma(dx, period)
    return adx, plus_di, minus_di
