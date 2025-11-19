# ================= Pure Indicator Calculators =================
from typing import Optional

from app.regime.data_structure import IndicatorState, BarData
from app.regime.indicator_utilities import ema_update, wilder_update, safe_clip, bb_width_normalized, true_range


class IndicatorCalculators:
    """Pure functions for calculating technical indicators."""

    @staticmethod
    def update_emas(state: IndicatorState, close: float) -> None:
        """Update all EMA values in the state."""
        state.ema12 = ema_update(state.ema12, close, 12)
        state.ema26 = ema_update(state.ema26, close, 26)
        state.ema20 = ema_update(state.ema20, close, 20)
        state.ema50 = ema_update(state.ema50, close, 50)
        state.ema200 = ema_update(state.ema200, close, 200)

    @staticmethod
    def calculate_ema_slope(state: IndicatorState) -> float:
        """Calculate EMA20 slope (t vs t-1)."""
        if state.ema20 is None or state.ema20_prev is None:
            return 0.0

        d = state.ema20 - state.ema20_prev
        return 1.0 if d > 0 else (-1.0 if d < 0 else 0.0)

    @staticmethod
    def update_ema_slope_state(state: IndicatorState) -> None:
        """Update EMA slope tracking state."""
        state.ema20_prev = state.ema20

    @staticmethod
    def calculate_macd_hist(state: IndicatorState) -> Optional[float]:
        """Calculate MACD histogram."""
        if state.ema12 is None or state.ema26 is None:
            return None

        macd_line = state.ema12 - state.ema26
        state.macd_signal = ema_update(state.macd_signal, macd_line, 9)

        if state.macd_signal is None:
            return None

        return macd_line - state.macd_signal

    @staticmethod
    def calculate_rsi(state: IndicatorState, close: float) -> float:
        """Calculate RSI value."""
        if state.prev_close is None:
            gain = loss = 0.0
        else:
            delta = close - state.prev_close
            gain = max(delta, 0.0)
            loss = max(-delta, 0.0)

        state.rsi_avg_gain = wilder_update(state.rsi_avg_gain, gain, 14)
        state.rsi_avg_loss = wilder_update(state.rsi_avg_loss, loss, 14)

        if state.rsi_avg_loss is None or state.rsi_avg_gain is None:
            return 50.0
        elif state.rsi_avg_loss == 0:
            return 100.0 if state.rsi_avg_gain > 0 else 50.0
        else:
            rs = state.rsi_avg_gain / state.rsi_avg_loss
            return 100.0 - (100.0 / (1.0 + rs))

    @staticmethod
    def calculate_atr_ratio(state: IndicatorState, bar: BarData) -> float:
        """Calculate ATR ratio with outlier protection."""
        tr = true_range(bar.high, bar.low, state.prev_close)
        state.atr14 = wilder_update(state.atr14, tr, 14)
        state.atr50 = wilder_update(state.atr50, tr, 50)

        if state.atr14 is not None and state.atr50 not in (None, 0.0):
            ratio = state.atr14 / state.atr50
            return safe_clip(ratio, 0.5, 3.0)  # Anti-outlier clipping
        return 1.0

    @staticmethod
    def calculate_bb_width(state: IndicatorState, close: float) -> float:
        """Calculate Bollinger Band width."""
        state.close_window.append(close)
        return bb_width_normalized(state.close_window, 20, 2.0)

    @staticmethod
    def update_bb_history(state: IndicatorState, bb_width: float) -> None:
        """Update BB history for threshold calculation."""
        state.bb_history.append(bb_width)
