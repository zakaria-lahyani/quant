"""
Orchestrator infrastructure for managing multi-symbol trading operations.

This module provides orchestrators for coordinating event-driven services
across multiple trading symbols.
"""

from .events import (
    OrchestratorEvent,
    OrchestratorEventType,
    NewCandleEvent,
    IndicatorsUpdatedEvent,
    SignalGeneratedEvent,
    TradeExecutedEvent
)
from .symbol_orchestrator import SymbolOrchestrator
from .multi_symbol_orchestrator import MultiSymbolTradingOrchestrator

__all__ = [
    # Events
    'OrchestratorEvent',
    'OrchestratorEventType',
    'NewCandleEvent',
    'IndicatorsUpdatedEvent',
    'SignalGeneratedEvent',
    'TradeExecutedEvent',

    # Orchestrators
    'SymbolOrchestrator',
    'MultiSymbolTradingOrchestrator',
]
