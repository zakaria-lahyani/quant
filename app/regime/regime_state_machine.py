from dataclasses import dataclass
from typing import Optional, Tuple

# ================= Regime State Machine =================

@dataclass
class StateMachineState:
    """State for regime persistence and transitions."""
    current_regime: str = "warming_up"
    pending_regime: Optional[str] = None
    pending_count: int = 0
    transition_countdown: int = 0


class RegimeStateMachine:
    """Manages regime persistence and transition logic."""

    def __init__(self, persist_n: int = 2, transition_bars: int = 3):
        self.persist_n = persist_n
        self.transition_bars = transition_bars
        self.state = StateMachineState()

    def update(self, new_regime: str) -> Tuple[str, bool]:
        """Update state machine and return (final_regime, is_transition)."""
        if self.state.current_regime in ("warming_up", None):
            self.state.current_regime = new_regime
            self._reset_pending()
            return self.state.current_regime, False

        changed = False

        if new_regime != self.state.current_regime:
            if self.state.pending_regime != new_regime:
                self.state.pending_regime = new_regime
                self.state.pending_count = 1
            else:
                self.state.pending_count += 1
                if self.state.pending_count >= self.persist_n:
                    # Commit change
                    self.state.current_regime = new_regime
                    self._reset_pending()
                    self.state.transition_countdown = self.transition_bars
                    changed = True
        else:
            self._reset_pending()

        is_transition = self.state.transition_countdown > 0
        if self.state.transition_countdown > 0:
            self.state.transition_countdown -= 1

        return self.state.current_regime, changed or is_transition

    def _reset_pending(self) -> None:
        """Reset pending state."""
        self.state.pending_regime = None
        self.state.pending_count = 0

