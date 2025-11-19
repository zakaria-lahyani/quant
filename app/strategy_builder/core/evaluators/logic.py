"""
Logic evaluator with dependency injection.
"""

from typing import Union, Optional, Dict, Any
from datetime import datetime, timedelta
import re

from app.strategy_builder.core.domain.protocols import LogicEvaluatorInterface, ConditionEvaluatorInterface
from app.strategy_builder.core.domain.models import ConditionTree, Condition, EntryRules, ExitRules, TimeBasedExit
from app.strategy_builder.core.domain.enums import LogicModeEnum


class LogicEvaluator(LogicEvaluatorInterface):
    """Evaluates complex logic rules using condition evaluators."""
    
    def __init__(
        self,
        condition_evaluator: ConditionEvaluatorInterface,
        position_data: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize logic evaluator.
        
        Args:
            condition_evaluator: Condition evaluator instance
            position_data: Optional position tracking data for time-based exits
                          Expected format: {
                              'entry_time': datetime,
                              'strategy_name': str,
                              'direction': str  # 'long' or 'short'
                          }
        """
        self.condition_evaluator = condition_evaluator
        self.position_data = position_data or {}
    
    def evaluate_entry_rules(self, entry_rules: EntryRules) -> bool:
        """
        Evaluate entry rules.
        
        Args:
            entry_rules: Entry rules to evaluate
            
        Returns:
            True if entry conditions are met
        """
        if entry_rules.mode == LogicModeEnum.ALL:
            if not entry_rules.conditions:
                return False
            return all(self.condition_evaluator.evaluate(cond) for cond in entry_rules.conditions)
        
        elif entry_rules.mode == LogicModeEnum.ANY:
            if not entry_rules.conditions:
                return False
            return any(self.condition_evaluator.evaluate(cond) for cond in entry_rules.conditions)
        
        elif entry_rules.mode == LogicModeEnum.COMPLEX:
            if not entry_rules.tree:
                return False
            return self._evaluate_tree(entry_rules.tree)
        
        else:
            raise ValueError(f"Unsupported logic mode: {entry_rules.mode}")
    
    def evaluate_exit_rules(self, exit_rules: ExitRules) -> bool:
        """
        Evaluate exit rules.
        
        Args:
            exit_rules: Exit rules to evaluate
            
        Returns:
            True if exit conditions are met
        """
        # Check basic conditions (conditions/tree)
        basic_result = self._evaluate_basic_exit_conditions(exit_rules)
        
        # Check time-based and profit guard conditions
        additional_result = self._evaluate_additional_exit_conditions(exit_rules)
        
        # For ANY mode, either basic OR additional conditions can trigger exit
        # For ALL mode, both basic AND additional conditions must be met
        if exit_rules.mode == LogicModeEnum.ANY:
            return basic_result or additional_result
        elif exit_rules.mode == LogicModeEnum.ALL:
            # If there are additional conditions, both must be true
            if exit_rules.time_based or exit_rules.profit_guard:
                return basic_result and additional_result
            else:
                return basic_result
        elif exit_rules.mode == LogicModeEnum.COMPLEX:
            # For complex mode, tree evaluation takes precedence
            # Additional conditions are checked separately
            return basic_result or additional_result
        else:
            return basic_result
    
    def _evaluate_basic_exit_conditions(self, exit_rules: ExitRules) -> bool:
        """
        Evaluate basic exit conditions (conditions/tree).
        
        Args:
            exit_rules: Exit rules to evaluate
            
        Returns:
            True if basic exit conditions are met
        """
        if exit_rules.mode == LogicModeEnum.ALL:
            if not exit_rules.conditions:
                return False  # No basic conditions means false (rely on time_based/profit_guard)
            return all(self.condition_evaluator.evaluate(cond) for cond in exit_rules.conditions)
        
        elif exit_rules.mode == LogicModeEnum.ANY:
            if not exit_rules.conditions:
                return False  # No basic conditions means false (rely on time_based/profit_guard)
            return any(self.condition_evaluator.evaluate(cond) for cond in exit_rules.conditions)
        
        elif exit_rules.mode == LogicModeEnum.COMPLEX:
            if not exit_rules.tree:
                return False  # No tree means false (rely on time_based/profit_guard)
            return self._evaluate_tree(exit_rules.tree)
        
        else:
            raise ValueError(f"Unsupported logic mode: {exit_rules.mode}")
    
    def _evaluate_additional_exit_conditions(self, exit_rules: ExitRules) -> bool:
        """
        Evaluate additional exit conditions (time-based, profit guard).
        
        Args:
            exit_rules: Exit rules to evaluate
            
        Returns:
            True if additional conditions are satisfied (exit should trigger)
        """
        # Check time-based exit conditions
        if exit_rules.time_based:
            time_exit_result = self._evaluate_time_based_exit(exit_rules.time_based)
            if time_exit_result:
                return True  # Time-based exit triggered
        
        # TODO: Implement profit guard evaluation when position P&L data is available
        # if exit_rules.profit_guard:
        #     profit_guard_result = self._evaluate_profit_guard(exit_rules.profit_guard)
        #     if profit_guard_result:
        #         return True
        
        return False  # No additional exit conditions triggered
    
    def _evaluate_time_based_exit(self, time_based: TimeBasedExit) -> bool:
        """
        Evaluate time-based exit conditions.
        
        Args:
            time_based: Time-based exit configuration
            
        Returns:
            True if time-based exit should trigger
        """
        if not self.position_data or 'entry_time' not in self.position_data:
            return False  # No position data available
        
        entry_time = self.position_data['entry_time']
        current_time = datetime.now()
        position_duration = current_time - entry_time
        
        # Check maximum duration
        if time_based.max_duration:
            max_duration = self._parse_duration(time_based.max_duration)
            if position_duration >= max_duration:
                return True  # Maximum duration exceeded, trigger exit
        
        # Check minimum duration (position must be held for at least this long)
        if time_based.min_duration:
            min_duration = self._parse_duration(time_based.min_duration)
            if position_duration < min_duration:
                # Position hasn't been held long enough, don't allow other exits
                # This is handled by the caller logic
                pass
        
        return False
    
    def _parse_duration(self, duration_str: str) -> timedelta:
        """
        Parse duration string into timedelta object.
        
        Args:
            duration_str: Duration string like "4h", "30m", "2d", "1w"
            
        Returns:
            timedelta object
            
        Raises:
            ValueError: If duration format is invalid
        """
        match = re.match(r'^(\d+)([mhdw])$', duration_str.lower())
        if not match:
            raise ValueError(f"Invalid duration format: {duration_str}")
        
        value, unit = match.groups()
        value = int(value)
        
        if unit == 'm':
            return timedelta(minutes=value)
        elif unit == 'h':
            return timedelta(hours=value)
        elif unit == 'd':
            return timedelta(days=value)
        elif unit == 'w':
            return timedelta(weeks=value)
        else:
            raise ValueError(f"Unsupported duration unit: {unit}")
    
    def _evaluate_tree(self, node: Union[ConditionTree, Condition]) -> bool:
        """
        Recursively evaluate a condition tree.
        
        Args:
            node: Tree node or condition to evaluate
            
        Returns:
            True if tree evaluation is satisfied
        """
        # Base case: single condition
        if isinstance(node, Condition):
            return self.condition_evaluator.evaluate(node)
        
        # Recursive case: node is ConditionTree
        if not isinstance(node, ConditionTree):
            raise ValueError(f"Invalid node type: {type(node)}")
        
        op = node.operator
        results = [self._evaluate_tree(child) for child in node.conditions]
        
        if op == "and":
            return all(results)
        elif op == "or":
            return any(results)
        elif op == "not":
            if len(results) != 1:
                raise ValueError("NOT operator must have exactly one child condition.")
            return not results[0]
        else:
            raise ValueError(f"Unsupported tree operator: {op}")