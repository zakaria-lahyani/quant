# ================= Regime Classification =================
from app.regime.data_structure import IndicatorValues, ClassificationResult


class RegimeClassifier:
    """Handles the logic of classifying market regimes."""

    @staticmethod
    def calculate_direction_score(indicators: IndicatorValues, close: float) -> int:
        """Calculate directional score from indicators."""
        score = 0

        # EMA trend signals (removed ema20 to avoid double counting with slope)
        if indicators.ema50 is not None and indicators.ema200 is not None:
            score += 2 if close > indicators.ema50 else -2
            score += 3 if close > indicators.ema200 else -3

        # RSI momentum signals
        if indicators.rsi is not None:
            score += 2 if indicators.rsi > 55 else (-2 if indicators.rsi < 45 else 0)
            score += 1 if indicators.rsi > 70 else (-1 if indicators.rsi < 30 else 0)

        # MACD momentum (only if available)
        if indicators.macd_hist is not None:
            score += 2 if indicators.macd_hist > 0 else -2

        # EMA slope
        if indicators.ema_slope is not None:
            score += int(indicators.ema_slope)

        return score

    @staticmethod
    def calculate_adaptive_confidence(dir_score: int, indicators: IndicatorValues) -> float:
        """Calculate confidence based on available indicators."""
        total_weight = 0

        # EMA contributions (removed ema20 to avoid double counting)
        if indicators.ema50 is not None and indicators.ema200 is not None:
            total_weight += 2 + 3

        # RSI contributions
        total_weight += 2 + 1

        # MACD contribution
        if indicators.macd_hist is not None:
            total_weight += 2

        # EMA slope contribution
        if indicators.ema_slope is not None and indicators.ema_slope != 0.0:
            total_weight += 1

        return min(1.0, abs(dir_score) / total_weight) if total_weight > 0 else 0.0

    @staticmethod
    def classify_regime(indicators: IndicatorValues, close: float, bb_threshold: float) -> ClassificationResult:
        """Classify regime based on indicators."""
        # Calculate direction
        dir_score = RegimeClassifier.calculate_direction_score(indicators, close)
        direction = "bull" if dir_score > 0 else ("bear" if dir_score < 0 else "neutral")

        # Calculate volatility
        is_expansion = (
                (indicators.atr_ratio is not None and indicators.atr_ratio > 1.1) or
                (indicators.bb_width is not None and indicators.bb_width > bb_threshold)
        )
        volatility = "expansion" if is_expansion else "contraction"

        # Calculate confidence
        confidence = RegimeClassifier.calculate_adaptive_confidence(dir_score, indicators)

        return ClassificationResult(direction, volatility, confidence, dir_score)

