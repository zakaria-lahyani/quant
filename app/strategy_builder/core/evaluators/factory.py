"""
Factory for creating evaluator instances.
"""

from collections import deque
from typing import Dict, Optional, Any
import pandas as pd

from app.strategy_builder.core.domain.protocols import (
    EvaluatorFactory,
    ConditionEvaluatorInterface,
    LogicEvaluatorInterface,
    Logger
)
from app.strategy_builder.core.evaluators.condition import ConditionEvaluator
from app.strategy_builder.core.evaluators.logic import LogicEvaluator


class DefaultEvaluatorFactory:
    """Default implementation of evaluator factory."""
    
    def __init__(self, logger: Logger):
        """
        Initialize factory with logger dependency.
        
        Args:
            logger: Logger instance for evaluators
        """
        self.logger = logger
    
    def create_condition_evaluator(
        self, 
        data: Dict[str, deque[pd.Series]]
    ) -> ConditionEvaluatorInterface:
        """
        Create condition evaluator instance.
        
        Args:
            data: Market data by timeframe
            
        Returns:
            Condition evaluator instance
        """
        return ConditionEvaluator(data, self.logger)
    
    def create_logic_evaluator(
        self,
        condition_evaluator: ConditionEvaluatorInterface,
        position_data: Optional[Dict[str, Any]] = None
    ) -> LogicEvaluatorInterface:
        """
        Create logic evaluator instance.
        
        Args:
            condition_evaluator: Condition evaluator dependency
            position_data: Optional position tracking data for time-based exits
            
        Returns:
            Logic evaluator instance
        """
        return LogicEvaluator(condition_evaluator, position_data)


def create_evaluator_factory(logger: Logger) -> EvaluatorFactory:
    """
    Factory function to create evaluator factory.
    
    Args:
        logger: Logger instance
        
    Returns:
        Evaluator factory instance
    """
    return DefaultEvaluatorFactory(logger)