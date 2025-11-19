
import json
from collections import deque
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from app.regime.data_structure import IndicatorState, RegimeSnapshot, BarData, IndicatorValues, ClassificationResult
from app.regime.htf_regime_bias import HTFBiasCalculator
from app.regime.indicator_calculator import IndicatorCalculators
from app.regime.regime_classifier import RegimeClassifier
from app.regime.regime_state_machine import RegimeStateMachine


# ================= Main Regime Detector =================

class RegimeDetector:
    """Main orchestrator for regime detection with clean separation of concerns."""

    def __init__(self,
                 warmup: int = 500,
                 persist_n: int = 2,
                 transition_bars: int = 3,
                 bb_threshold_len: int = 200,
                 htf_rule: Optional[str] = None):

        # Configuration
        self.warmup = warmup

        # Components
        self.indicator_state = IndicatorState()
        self.indicator_state.bb_history = deque(maxlen=bb_threshold_len)

        self.htf_calculator = HTFBiasCalculator(htf_rule)
        self.state_machine = RegimeStateMachine(persist_n, transition_bars)

        # Output
        self.history: List[RegimeSnapshot] = []

    def process_bar(self, bar: BarData) -> RegimeSnapshot:
        """Process a single bar and return regime snapshot."""
        # Update HTF bias first
        htf_bias = self.htf_calculator.update(bar.timestamp, bar.close)

        # During warmup, still calculate indicators but return warming_up regime
        if bar.bar_index < self.warmup:
            indicators = self._calculate_all_indicators(bar)
            self._update_state_tracking(bar.close)

            snapshot = RegimeSnapshot(
                timestamp=bar.timestamp,
                bar_index=bar.bar_index,
                regime="warming_up",
                confidence=0.0,
                indicators=indicators,
                is_transition=False,
                htf_bias=htf_bias
            )
            self.history.append(snapshot)
            return snapshot

        # Calculate indicators
        indicators = self._calculate_all_indicators(bar)

        # Get BB threshold (BB history is already updated in _calculate_all_indicators)
        if len(self.indicator_state.bb_history) > 1:
            bb_threshold = float(np.percentile(list(self.indicator_state.bb_history)[:-1], 70))
        else:
            bb_threshold = 0.04

        # Classify regime
        classification = RegimeClassifier.classify_regime(indicators, bar.close, bb_threshold)

        # Apply HTF bias filtering
        regime_with_htf = self._apply_htf_bias(classification, htf_bias)

        # Apply persistence
        final_regime, is_transition = self.state_machine.update(regime_with_htf)

        # Update state tracking
        self._update_state_tracking(bar.close)

        # Create snapshot
        snapshot = RegimeSnapshot(
            timestamp=bar.timestamp,
            bar_index=bar.bar_index,
            regime=final_regime,
            confidence=classification.confidence,
            indicators=indicators,
            is_transition=is_transition,
            htf_bias=htf_bias
        )

        self.history.append(snapshot)
        return snapshot

    def _calculate_all_indicators(self, bar: BarData) -> IndicatorValues:
        """Calculate all indicators for the current bar."""
        # Update EMAs
        IndicatorCalculators.update_emas(self.indicator_state, bar.close)

        # Calculate indicators
        rsi = IndicatorCalculators.calculate_rsi(self.indicator_state, bar.close)
        atr_ratio = IndicatorCalculators.calculate_atr_ratio(self.indicator_state, bar)
        bb_width = IndicatorCalculators.calculate_bb_width(self.indicator_state, bar.close)
        macd_hist = IndicatorCalculators.calculate_macd_hist(self.indicator_state)
        ema_slope = IndicatorCalculators.calculate_ema_slope(self.indicator_state)

        # Update histories
        IndicatorCalculators.update_bb_history(self.indicator_state, bb_width)
        IndicatorCalculators.update_ema_slope_state(self.indicator_state)

        return IndicatorValues(
            rsi=rsi,
            atr_ratio=atr_ratio,
            bb_width=bb_width,
            macd_hist=macd_hist,
            ema20=self.indicator_state.ema20,
            ema50=self.indicator_state.ema50,
            ema200=self.indicator_state.ema200,
            ema_slope=ema_slope
        )

    def _apply_htf_bias(self, classification: ClassificationResult, htf_bias: str) -> str:
        """Apply HTF bias to regime classification."""
        base_regime = f"{classification.direction}_{classification.volatility}"

        if htf_bias in ("bull", "bear"):
            is_counter_trend = (
                    (classification.direction == "bull" and htf_bias == "bear") or
                    (classification.direction == "bear" and htf_bias == "bull")
            )
            if is_counter_trend:
                return f"neutral_{classification.volatility}"

        return base_regime

    def _update_state_tracking(self, close: float) -> None:
        """Update internal state tracking."""
        self.indicator_state.prev_close = close

    def stats(self) -> Dict:
        """Calculate statistics from the history."""
        non_warmup = [s for s in self.history if s.regime != "warming_up"]
        if not non_warmup:
            return {}

        regimes = [s.regime for s in non_warmup]
        counts = pd.Series(regimes).value_counts().to_dict()

        avg_confidence = {}
        for regime in set(regimes):
            confidences = [s.confidence for s in non_warmup if s.regime == regime]
            avg_confidence[regime] = float(np.mean(confidences))

        # Duration analysis
        durations = []
        current_regime = None
        current_duration = 0

        for snapshot in non_warmup:
            if snapshot.regime != current_regime:
                if current_regime is not None:
                    durations.append(current_duration)
                current_regime = snapshot.regime
                current_duration = 1
            else:
                current_duration += 1

        if current_duration > 0:
            durations.append(current_duration)

        return {
            "counts": counts,
            "avg_confidence": avg_confidence,
            "avg_duration": float(np.mean(durations)) if durations else 0.0,
            "max_duration": int(np.max(durations)) if durations else 0,
            "min_duration": int(np.min(durations)) if durations else 0,
            "num_transitions": sum(1 for s in self.history if s.is_transition)
        }

    def export(self, path: str) -> None:
        """Export results to JSON file."""
        data = {
            "metadata": {
                "warmup": self.warmup,
                "persist_n": self.state_machine.persist_n,
                "transition_bars": self.state_machine.transition_bars,
                "htf_rule": self.htf_calculator.state.rule,
                "total_bars": len(self.history)
            },
            "stats": self.stats(),
            "history": [s.to_dict() for s in self.history]
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

