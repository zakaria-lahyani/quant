"""
Strategy executor with dependency injection.
"""

from collections import deque
from typing import Dict, Optional, Any
import pandas as pd

from app.strategy_builder.core.domain.protocols import StrategyExecutorInterface, EvaluatorFactory
from app.strategy_builder.core.domain.models import TradingStrategy
from app.strategy_builder.data.dtos import SignalResult


class StrategyExecutor(StrategyExecutorInterface):
    """Executes trading strategy evaluation with injected dependencies."""
    
    def __init__(
        self,
        strategy: TradingStrategy,
        recent_rows: Dict[str, deque[pd.Series]],
        evaluator_factory: EvaluatorFactory,
        position_data: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize strategy executor.
        
        Args:
            strategy: Trading strategy to execute
            recent_rows: Market data by timeframe
            evaluator_factory: Factory for creating evaluators
            position_data: Optional position tracking data for time-based exits
        """
        self.strategy = strategy
        self.recent_rows = recent_rows
        self.evaluator_factory = evaluator_factory
        self.position_data = position_data
        
        # Create evaluators using factory
        self.condition_evaluator = self.evaluator_factory.create_condition_evaluator(recent_rows)
        self.logic_evaluator = self.evaluator_factory.create_logic_evaluator(
            self.condition_evaluator,
            position_data
        )
    
    def check_entry(self) -> SignalResult:
        """
        Check entry conditions for the strategy.
        
        Returns:
            SignalResult with long/short entry signals
        """
        entry = self.strategy.entry
        result = SignalResult()
        
        if entry:
            if entry.long:
                try:
                    result.long = self.logic_evaluator.evaluate_entry_rules(entry.long)
                except (ValueError, TypeError, KeyError) as e:
                    # Log specific error but don't crash - return False for safety
                    result.long = False
            
            if entry.short:
                try:
                    result.short = self.logic_evaluator.evaluate_entry_rules(entry.short)
                except (ValueError, TypeError, KeyError) as e:
                    # Log specific error but don't crash - return False for safety
                    result.short = False
        
        return result
    
    def check_exit(self) -> SignalResult:
        """
        Check exit conditions for the strategy.
        
        Returns:
            SignalResult with long/short exit signals
        """
        exit_rules = self.strategy.exit
        result = SignalResult()
        
        if exit_rules:
            if exit_rules.long:
                try:
                    result.long = self.logic_evaluator.evaluate_exit_rules(exit_rules.long)
                except (ValueError, TypeError, KeyError) as e:
                    # Log specific error but don't crash - return False for safety
                    result.long = False
            
            if exit_rules.short:
                try:
                    result.short = self.logic_evaluator.evaluate_exit_rules(exit_rules.short)
                except (ValueError, TypeError, KeyError) as e:
                    # Log specific error but don't crash - return False for safety
                    result.short = False
        
        return result
    
    def get_strategy_name(self) -> str:
        """
        Get the name of the strategy being executed.
        
        Returns:
            Strategy name
        """
        return self.strategy.name
    
    def is_strategy_active(self) -> bool:
        """
        Check if strategy is currently active based on activation settings.
        
        Returns:
            True if strategy should be active
        """
        if not self.strategy.activation:
            return True  # Default to active if no activation settings
        
        if not self.strategy.activation.enabled:
            return False
        
        # TODO: Implement schedule-based activation checking
        # This would require current time context and schedule evaluation
        
        return True
    
    def validate_data_availability(self) -> bool:
        """
        Validate that required market data is available for strategy execution.
        
        Returns:
            True if all required timeframes have data
        """
        for timeframe in self.strategy.timeframes:
            if timeframe not in self.recent_rows:
                return False
            if len(self.recent_rows[timeframe]) == 0:
                return False
        
        return True


def create_strategy_executor(
    strategy: TradingStrategy,
    recent_rows: Dict[str, deque[pd.Series]],
    evaluator_factory: EvaluatorFactory,
    position_data: Optional[Dict[str, Any]] = None
) -> StrategyExecutorInterface:
    """
    Factory function to create strategy executor.
    
    Args:
        strategy: Trading strategy to execute
        recent_rows: Market data by timeframe
        evaluator_factory: Factory for creating evaluators
        position_data: Optional position tracking data for time-based exits
        
    Returns:
        Strategy executor instance
    """
    return StrategyExecutor(strategy, recent_rows, evaluator_factory, position_data)