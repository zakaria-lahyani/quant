"""
Stop loss calculator for maintaining constant monetary risk across scaled positions.
"""

from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import logging


@dataclass
class PositionEntry:
    """Represents a single position entry for stop loss calculation."""
    entry_price: float
    position_size: float
    weight: float = 1.0  # Weight of this position in the group


class MonetaryStopLossCalculator:
    """
    Calculates stop loss levels to maintain constant dollar risk across multiple position entries.
    
    For example, if you have:
    - Target risk: $500
    - Gold: 1 lot with 5 points movement = $500 risk
    - Multiple entries at different prices
    
    The calculator will find the stop loss level where the combined risk of all positions equals $500.
    """
    
    def __init__(self, symbol: str, logger: Optional[logging.Logger] = None):
        """
        Initialize the calculator.
        
        Args:
            symbol: Trading symbol (e.g., 'XAUUSD', 'BTCUSD')
            logger: Optional logger instance
        """
        self.symbol = symbol
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        
        # Point values per lot for different instruments
        # These define how much $1 point movement is worth per lot
        self.point_values = {
            'XAUUSD': 100.0,  # Gold: $100 per point per lot
            'BTCUSD': 1.0,     # Bitcoin: $1 per point per lot
            'EURUSD': 100000.0,  # EUR/USD: pip value calculation needed
        }
    
    def get_point_value(self) -> float:
        """Get the point value for the current symbol."""
        return self.point_values.get(self.symbol, 1.0)
    
    def calculate_group_stop_loss_from_price_level(
        self,
        entries: List[PositionEntry],
        original_entry_price: float,
        original_stop_price: float,
        original_position_size: float,
        direction: str
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate stop loss for scaled positions based on original price levels.
        
        Maintains the same dollar risk as the original single position would have had.
        
        Args:
            entries: List of position entries with prices and sizes
            original_entry_price: Original planned entry price
            original_stop_price: Original planned stop loss price
            original_position_size: Original planned position size
            direction: 'long' or 'short'
            
        Returns:
            Tuple of (stop_loss_level, calculation_details)
        """
        is_long = direction.lower() == 'long'
        point_value = self.get_point_value()
        
        # Calculate the original risk in dollars - always use absolute difference
        original_points = abs(original_entry_price - original_stop_price)
        
        # Determine which side the original stop is on
        stop_is_below = original_stop_price < original_entry_price
        
        # Log a warning if stop seems to be on wrong side
        if is_long and not stop_is_below:
            self.logger.warning(f"Stop loss {original_stop_price} is above entry {original_entry_price} for long position")
        elif not is_long and stop_is_below:
            self.logger.warning(f"Stop loss {original_stop_price} is below entry {original_entry_price} for short position - might be using TP as SL?")
        
        original_risk = original_points * original_position_size * point_value
        
        # Calculate the group stop loss maintaining the same side relationship
        stop_loss, details = self.calculate_group_stop_loss_with_side(
            entries, original_risk, direction, stop_is_below
        )
        
        return stop_loss, details
    
    def calculate_group_stop_loss_with_side(
        self,
        entries: List[PositionEntry],
        target_risk: float,
        direction: str,
        stop_below_entry: bool
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate stop loss with explicit control over which side of entry it should be on.
        
        Args:
            entries: List of position entries
            target_risk: Target risk in dollars
            direction: 'long' or 'short'
            stop_below_entry: If True, place stop below avg entry; if False, above
            
        Returns:
            Tuple of (stop_loss_level, calculation_details)
        """
        if not entries:
            raise ValueError("No entries provided")
        
        point_value = self.get_point_value()
        
        # Calculate weighted average entry price
        total_value = sum(entry.entry_price * entry.position_size for entry in entries)
        total_size = sum(entry.position_size for entry in entries)
        
        if total_size == 0:
            raise ValueError("Total position size is zero")
        
        weighted_avg_price = total_value / total_size
        
        # Calculate required points movement for target risk
        required_points = target_risk / (total_size * point_value)
        
        # Calculate stop loss level based on specified side
        if stop_below_entry:
            stop_loss = weighted_avg_price - required_points
        else:
            stop_loss = weighted_avg_price + required_points
        
        # Calculate individual risks for verification
        individual_risks = []
        for entry in entries:
            points_to_stop = abs(entry.entry_price - stop_loss)
            risk = points_to_stop * entry.position_size * point_value
            individual_risks.append({
                'entry_price': entry.entry_price,
                'position_size': entry.position_size,
                'points_to_stop': points_to_stop,
                'risk': risk
            })
        
        total_calculated_risk = sum(item['risk'] for item in individual_risks)
        
        details = {
            'weighted_avg_price': weighted_avg_price,
            'total_size': total_size,
            'required_points': required_points,
            'stop_loss': stop_loss,
            'target_risk': target_risk,
            'calculated_total_risk': total_calculated_risk,
            'individual_risks': individual_risks,
            'point_value': point_value,
            'stop_side': 'below' if stop_below_entry else 'above'
        }
        
        self.logger.info(
            f"Calculated stop loss: {stop_loss:.2f} ({details['stop_side']} entry) for {len(entries)} entries "
            f"with total risk: ${total_calculated_risk:.2f} (target: ${target_risk:.2f})"
        )
        
        return stop_loss, details
    
    def calculate_group_stop_loss(
        self,
        entries: List[PositionEntry],
        target_risk: float,
        direction: str
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate a single stop loss level for all positions to maintain target risk.
        
        Uses the average entry price method: If original position was 1 lot at 3000 with stop at 2995 ($500 risk),
        and we scale into 2 positions of 0.5 lots each at 3000 and 2995, the average entry is 2997.5,
        so the stop should be at 2992.5 to maintain the same 5 point distance from average entry.
        
        Args:
            entries: List of position entries with prices and sizes
            target_risk: Target total risk in dollars (e.g., $500)
            direction: 'long' or 'short'
            
        Returns:
            Tuple of (stop_loss_level, calculation_details)
        """
        if not entries:
            raise ValueError("No entries provided")
        
        is_long = direction.lower() == 'long'
        point_value = self.get_point_value()
        
        # Calculate weighted average entry price
        total_value = sum(entry.entry_price * entry.position_size for entry in entries)
        total_size = sum(entry.position_size for entry in entries)
        
        if total_size == 0:
            raise ValueError("Total position size is zero")
        
        weighted_avg_price = total_value / total_size
        
        # Calculate required points movement for target risk
        # Risk = Points * Total_Size * Point_Value
        # Points = Risk / (Total_Size * Point_Value)
        required_points = target_risk / (total_size * point_value)
        
        # Calculate stop loss level based on average entry and direction
        # We need to determine which side the stop should be on based on the direction
        if is_long:
            # For long: stop loss is below average entry
            stop_loss = weighted_avg_price - required_points
        else:
            # For short: traditionally stop loss is above average entry
            # But if the original stop was below (like a target), maintain that relationship
            stop_loss = weighted_avg_price + required_points
        
        # Calculate individual risks for verification
        individual_risks = []
        for entry in entries:
            if is_long:
                points_to_stop = entry.entry_price - stop_loss
            else:
                points_to_stop = stop_loss - entry.entry_price
            
            risk = points_to_stop * entry.position_size * point_value
            individual_risks.append({
                'entry_price': entry.entry_price,
                'position_size': entry.position_size,
                'points_to_stop': points_to_stop,
                'risk': risk
            })
        
        total_calculated_risk = sum(item['risk'] for item in individual_risks)
        
        details = {
            'weighted_avg_price': weighted_avg_price,
            'total_size': total_size,
            'required_points': required_points,
            'stop_loss': stop_loss,
            'target_risk': target_risk,
            'calculated_total_risk': total_calculated_risk,
            'individual_risks': individual_risks,
            'point_value': point_value
        }
        
        self.logger.info(
            f"Calculated stop loss: {stop_loss:.2f} for {len(entries)} entries "
            f"with total risk: ${total_calculated_risk:.2f} (target: ${target_risk:.2f})"
        )
        
        return stop_loss, details
    
    def calculate_adjusted_stop_for_new_entry(
        self,
        existing_entries: List[PositionEntry],
        new_entry: PositionEntry,
        target_risk: float,
        direction: str
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate adjusted stop loss when adding a new entry to existing positions.
        
        This maintains the same total dollar risk even as new positions are added.
        
        Args:
            existing_entries: List of already filled position entries
            new_entry: New position entry being added
            target_risk: Target total risk in dollars
            direction: 'long' or 'short'
            
        Returns:
            Tuple of (new_stop_loss_level, calculation_details)
        """
        # Combine existing and new entries
        all_entries = existing_entries + [new_entry]
        
        # Calculate new stop loss for all positions
        return self.calculate_group_stop_loss(all_entries, target_risk, direction)
    
    def calculate_risk_for_stop(
        self,
        entries: List[PositionEntry],
        stop_loss: float,
        direction: str
    ) -> Dict[str, Any]:
        """
        Calculate the total risk for given entries and stop loss level.
        
        Args:
            entries: List of position entries
            stop_loss: Stop loss level
            direction: 'long' or 'short'
            
        Returns:
            Dictionary with risk calculation details
        """
        is_long = direction.lower() == 'long'
        point_value = self.get_point_value()
        
        individual_risks = []
        for entry in entries:
            if is_long:
                points_to_stop = entry.entry_price - stop_loss
            else:
                points_to_stop = stop_loss - entry.entry_price
            
            # Ensure we don't have negative risk (stop on wrong side)
            if points_to_stop < 0:
                self.logger.warning(
                    f"Stop loss {stop_loss:.2f} is on wrong side of entry {entry.entry_price:.2f}"
                )
                points_to_stop = 0
            
            risk = points_to_stop * entry.position_size * point_value
            individual_risks.append({
                'entry_price': entry.entry_price,
                'position_size': entry.position_size,
                'points_to_stop': points_to_stop,
                'risk': risk
            })
        
        total_risk = sum(item['risk'] for item in individual_risks)
        
        return {
            'stop_loss': stop_loss,
            'total_risk': total_risk,
            'individual_risks': individual_risks,
            'point_value': point_value
        }