"""
Condition evaluator with dependency injection.
"""

import pandas as pd
from collections import deque
from typing import Dict, Union

from app.strategy_builder.core.domain.protocols import ConditionEvaluatorInterface, Logger
from app.strategy_builder.core.domain.models import Condition
from app.strategy_builder.core.domain.enums import ConditionOperatorEnum


class ConditionEvaluator(ConditionEvaluatorInterface):
    """Evaluates individual trading conditions."""
    
    def __init__(self, recent_rows: Dict[str, deque[pd.Series]], logger: Logger):
        """
        Initialize condition evaluator.
        
        Args:
            recent_rows: Market data by timeframe
            logger: Logger instance for error reporting
        """
        self.recent_rows = recent_rows
        self.logger = logger
    
    def evaluate(self, condition: Condition) -> bool:
        """
        Evaluate a single condition.
        
        Args:
            condition: Condition to evaluate
            
        Returns:
            True if condition is met, False otherwise
        """
        tf = condition.timeframe
        
        # Check if timeframe data is available
        if tf not in self.recent_rows or len(self.recent_rows[tf]) == 0:
            self.logger.warning(
                f"ConditionEvaluator: {tf} not in recent_rows or recent_rows for {tf} is empty"
            )
            return False
        
        row = self.recent_rows[tf][-1]
        
        # Log time and row info for debugging
        time_value = row.get('time', 'N/A')

        # Check for duplicate columns
        if hasattr(row, 'index'):
            duplicates = row.index.duplicated()
            if duplicates.any():
                dup_cols = row.index[duplicates].tolist()
                self.logger.warning(f"ConditionEvaluator: Found duplicate columns: {dup_cols}")
                
        # Log some key columns to see their types and values
        for col in ['close', 'previous_close']:
            if col in row:
                val = row[col]
                if hasattr(val, '__len__') and not isinstance(val, str):
                    self.logger.info(f"ConditionEvaluator: {col} is array-like with shape: {getattr(val, 'shape', len(val))}")
        
        # Get current signal value
        current_signal = row.get(condition.signal)
        if current_signal is None:
            self.logger.warning(
                f"ConditionEvaluator: {condition.signal} is None"
            )
            return False
        
        # Ensure current_signal is a scalar value
        if hasattr(current_signal, '__len__') and not isinstance(current_signal, str):
            current_signal = current_signal.iloc[0] if hasattr(current_signal, 'iloc') else current_signal[0]
        
        # Check if we need previous signal (only for certain operators)
        needs_previous = condition.operator in [
            ConditionOperatorEnum.CROSSES_ABOVE,
            ConditionOperatorEnum.CROSSES_BELOW,
            ConditionOperatorEnum.CHANGES_TO,
            ConditionOperatorEnum.REMAINS
        ]
        
        if needs_previous:
            previous_signal = row.get(f"previous_{condition.signal}")
            if previous_signal is None:
                self.logger.warning(
                    f"ConditionEvaluator: previous_{condition.signal} is None (required for {condition.operator.value})"
                )
                return False
            
            # Ensure previous_signal is a scalar value
            if hasattr(previous_signal, '__len__') and not isinstance(previous_signal, str):
                self.logger.warning(f"ConditionEvaluator: previous_{condition.signal} is array-like: {previous_signal}, taking first value")
                previous_signal = previous_signal.iloc[0] if hasattr(previous_signal, 'iloc') else previous_signal[0]
            
            # Handle operators that need previous values
            if condition.operator == ConditionOperatorEnum.CROSSES_ABOVE:
                result = self._crosses(previous_signal, current_signal, row, condition.value, direction="above")
                return result
            
            if condition.operator == ConditionOperatorEnum.CROSSES_BELOW:
                result = self._crosses(previous_signal, current_signal, row, condition.value, direction="below")
                return result
            
            if condition.operator == ConditionOperatorEnum.CHANGES_TO:
                target_value = self._resolve_value(condition.value, row, current_signal)
                result = current_signal == target_value and previous_signal != target_value
                return result
            
            if condition.operator == ConditionOperatorEnum.REMAINS:
                target_value = self._resolve_value(condition.value, row, current_signal)
                result = current_signal == target_value and previous_signal == target_value
                return result
        
        # Handle standard comparison operators (don't need previous values)
        target_value = self._resolve_value(condition.value, row, current_signal)
        result = self._compare(current_signal, condition.operator, target_value)

        return result
    
    def _resolve_value(self, val, row, ref_type: Union[str, float, int]) -> Union[str, float, int]:
        """
        Resolve a value (which may be a column reference or literal) to match the type of ref_type.
        
        Args:
            val: Value to resolve (may be column name or literal)
            row: Current data row
            ref_type: Reference type for casting
            
        Returns:
            Resolved and type-cast value
        """
        if isinstance(val, str) and val in row:
            resolved = row[val]
            # Ensure resolved value is scalar
            if hasattr(resolved, '__len__') and not isinstance(resolved, str):
                resolved = resolved.iloc[0] if hasattr(resolved, 'iloc') else resolved[0]
        else:
            resolved = val
        
        # Type casting based on reference value
        if isinstance(ref_type, (int, float)):
            try:
                return type(ref_type)(resolved)
            except (ValueError, TypeError):
                return resolved  # fallback
        elif isinstance(ref_type, str):
            return str(resolved).lower()
        return resolved
    
    def _crosses(self, prev, curr, row, target, direction: str) -> bool:
        """
        Check if signal crosses above/below target.
        
        Args:
            prev: Previous signal value
            curr: Current signal value
            row: Current data row
            target: Target value or column name
            direction: "above" or "below"
            
        Returns:
            True if crossing condition is met
        """
        if isinstance(target, str) and target in row:
            prev_target = row.get(f"previous_{target}")
            curr_target = row.get(target)
        else:
            curr_target = prev_target = float(self._resolve_value(target, row, curr))
        
        if any(v is None for v in [prev, curr, prev_target, curr_target]):
            return False
        
        try:
            prev = float(prev)
            curr = float(curr)
            prev_target = float(prev_target)
            curr_target = float(curr_target)
        except (ValueError, TypeError):
            return False
        
        if direction == "above":
            return prev <= prev_target and curr > curr_target
        elif direction == "below":
            return prev >= prev_target and curr < curr_target
        return False
    
    def _compare(self, left, operator: ConditionOperatorEnum, right) -> bool:
        """
        Compare values with automatic type coercion when possible.
        
        Args:
            left: Left operand
            operator: Comparison operator
            right: Right operand
            
        Returns:
            True if comparison is satisfied
        """
        # Try to align types
        try:
            # Try casting right to the type of left
            if not isinstance(right, type(left)):
                right = type(left)(right)
        except (ValueError, TypeError):
            try:
                # If that fails, try casting left to the type of right
                if not isinstance(left, type(right)):
                    left = type(right)(left)
            except (ValueError, TypeError):
                pass  # Keep original values if casting fails
        
        # Normalize strings for comparison
        if isinstance(left, str):
            left = left.lower()
        if isinstance(right, str):
            right = right.lower()
        
        try:
            if operator == ConditionOperatorEnum.EQ:
                return left == right
            elif operator == ConditionOperatorEnum.NE:
                return left != right
            elif operator == ConditionOperatorEnum.GT:
                return left > right
            elif operator == ConditionOperatorEnum.GTE:
                return left >= right
            elif operator == ConditionOperatorEnum.LT:
                return left < right
            elif operator == ConditionOperatorEnum.LTE:
                return left <= right
            elif operator == ConditionOperatorEnum.IN:
                return left in right
            elif operator == ConditionOperatorEnum.NOT_IN:
                return left not in right
        except Exception as e:
            self.logger.warning(f"Comparison error: {e}")
        
        return False