"""
Multi-target take profit implementation.
"""

from typing import Optional, Dict, Any, List
import logging

from ..core.base import BaseTakeProfitCalculator
from ..core.exceptions import ValidationError
from ...strategy_builder.data.dtos import TakeProfitResult, TPLevel
from ...strategy_builder.core.domain.models import MultiTargetTakeProfit


class MultiTargetTakeProfitCalculator(BaseTakeProfitCalculator):
    """Calculator for multi-target take profit levels."""
    
    def __init__(self, config: MultiTargetTakeProfit, pip_value: float, logger: Optional[logging.Logger] = None):
        super().__init__(config, logger)
        
        if config.type != "multi_target":
            raise ValidationError(
                f"Expected 'multi_target' take profit type, got {config.type}",
                field_name="type"
            )
        
        if pip_value <= 0:
            raise ValidationError(
                "pip_value must be positive",
                field_name="pip_value"
            )
        
        self.pip_value = pip_value  # Store for consistency, though not used in percentage-based calculations
        
        if not config.targets or len(config.targets) == 0:
            raise ValidationError(
                "Multi-target take profit must have at least one target",
                field_name="targets"
            )
        
        # Validate targets
        total_percent = sum(target.percent for target in config.targets)
        if abs(total_percent - 100.0) > 0.01:  # Allow small floating point errors
            raise ValidationError(
                f"Target percentages must sum to 100%, got {total_percent}%",
                field_name="targets"
            )
        
        # Validate individual targets
        for i, target in enumerate(config.targets):
            if target.value <= 0:
                raise ValidationError(
                    f"Target {i+1} value must be positive",
                    field_name=f"targets[{i}].value"
                )
            if target.percent <= 0 or target.percent > 100:
                raise ValidationError(
                    f"Target {i+1} percent must be between 0 and 100",
                    field_name=f"targets[{i}].percent"
                )
    
    def calculate_take_profit(
        self,
        entry_price: float,
        is_long: bool,
        market_data: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> TakeProfitResult:
        """
        Calculate multi-target take profit levels.
        
        Args:
            entry_price: The entry price for the trade
            is_long: Whether this is a long position
            market_data: Not used for multi-target take profit
            **kwargs: Additional parameters
            
        Returns:
            TakeProfitResult with calculated multi-target take profit details
        """
        self._validate_inputs(entry_price, is_long, market_data)
        
        targets_info = []
        
        for i, target in enumerate(self.config.targets):
            # Calculate profit distance based on percentage of entry price
            profit_distance = entry_price * (target.value / 100.0)
            profit_level = self._calculate_profit_level(entry_price, profit_distance, is_long)
            
            targets_info.append(TPLevel(
                level=profit_level,
                value=target.value,
                percent=target.percent,
                move_stop=target.move_stop if hasattr(target, 'move_stop') else False
            ))
            
            self.logger.debug(
                f"Multi-target TP {i+1}: entry={entry_price}, "
                f"target_value={target.value}%, level={profit_level}, "
                f"percent={target.percent}%, move_stop={getattr(target, 'move_stop', False)}"
            )
        
        return TakeProfitResult(
            type="multi_target",
            targets=targets_info
        )
    
    def calculate_partial_exit_sizes(
        self,
        total_position_size: float,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Calculate the position sizes for each partial exit.
        
        Args:
            total_position_size: Total position size in base currency
            **kwargs: Additional parameters
            
        Returns:
            List of dictionaries with exit information for each target
        """
        exit_info = []
        
        for i, target in enumerate(self.config.targets):
            exit_size = total_position_size * (target.percent / 100.0)
            
            exit_info.append({
                'target_number': i + 1,
                'exit_size': exit_size,
                'exit_percentage': target.percent,
                'target_value': target.value,
                'move_stop': getattr(target, 'move_stop', False)
            })
        
        return exit_info
    
    def get_next_target(
        self,
        current_price: float,
        entry_price: float,
        is_long: bool,
        executed_targets: List[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get the next target that should be executed based on current price.
        
        Args:
            current_price: Current market price
            entry_price: Entry price
            is_long: Whether this is a long position
            executed_targets: List of already executed target indices
            
        Returns:
            Dictionary with next target info, or None if no target is ready
        """
        if executed_targets is None:
            executed_targets = []
        
        tp_result = self.calculate_take_profit(entry_price, is_long)
        
        for i, target in enumerate(tp_result.targets):
            if i in executed_targets:
                continue
            
            # Check if current price has reached this target
            if is_long:
                if current_price >= target.level:
                    return {
                        'target_index': i,
                        'target_level': target.level,
                        'target_percent': target.percent,
                        'target_value': target.value,
                        'move_stop': target.move_stop
                    }
            else:
                if current_price <= target.level:
                    return {
                        'target_index': i,
                        'target_level': target.level,
                        'target_percent': target.percent,
                        'target_value': target.value,
                        'move_stop': target.move_stop
                    }
        
        return None
    
    def calculate_remaining_position(
        self,
        original_size: float,
        executed_targets: List[int]
    ) -> float:
        """
        Calculate remaining position size after partial exits.
        
        Args:
            original_size: Original position size
            executed_targets: List of executed target indices
            
        Returns:
            Remaining position size
        """
        executed_percent = sum(
            self.config.targets[i].percent 
            for i in executed_targets 
            if i < len(self.config.targets)
        )
        
        remaining_percent = 100.0 - executed_percent
        return original_size * (remaining_percent / 100.0)
    
    def should_move_stop_loss(
        self,
        target_index: int,
        current_stop: float,
        entry_price: float,
        is_long: bool
    ) -> Optional[float]:
        """
        Determine if stop loss should be moved after hitting a target.
        
        Args:
            target_index: Index of the target that was hit
            current_stop: Current stop loss level
            entry_price: Entry price
            is_long: Whether this is a long position
            
        Returns:
            New stop loss level if it should be moved, None otherwise
        """
        if target_index >= len(self.config.targets):
            return None
        
        target = self.config.targets[target_index]
        
        if not getattr(target, 'move_stop', False):
            return None
        
        # Move stop to breakeven or better
        if is_long:
            new_stop = max(current_stop, entry_price)
        else:
            new_stop = min(current_stop, entry_price)
        
        self.logger.debug(
            f"Moving stop loss after target {target_index + 1}: "
            f"current_stop={current_stop}, new_stop={new_stop}"
        )
        
        return new_stop
    
    def get_execution_summary(
        self,
        entry_price: float,
        position_size: float,
        is_long: bool,
        executed_targets: List[int] = None
    ) -> Dict[str, Any]:
        """
        Get a summary of the multi-target execution plan.
        
        Args:
            entry_price: Entry price
            position_size: Total position size
            is_long: Whether this is a long position
            executed_targets: List of executed target indices
            
        Returns:
            Dictionary with execution summary
        """
        if executed_targets is None:
            executed_targets = []
        
        tp_result = self.calculate_take_profit(entry_price, is_long)
        exit_info = self.calculate_partial_exit_sizes(position_size)
        
        total_executed_percent = sum(
            target.percent for i, target in enumerate(tp_result.targets)
            if i in executed_targets
        )
        
        remaining_percent = 100.0 - total_executed_percent
        remaining_size = position_size * (remaining_percent / 100.0)
        
        return {
            'total_targets': len(tp_result.targets),
            'executed_targets': len(executed_targets),
            'remaining_targets': len(tp_result.targets) - len(executed_targets),
            'executed_percent': total_executed_percent,
            'remaining_percent': remaining_percent,
            'remaining_size': remaining_size,
            'targets_detail': [
                {
                    'target_number': i + 1,
                    'level': target.level,
                    'percent': target.percent,
                    'size': exit_info[i]['exit_size'],
                    'executed': i in executed_targets,
                    'move_stop': target.move_stop
                }
                for i, target in enumerate(tp_result.targets)
            ]
        }