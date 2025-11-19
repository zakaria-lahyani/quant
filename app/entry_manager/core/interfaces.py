"""
Interfaces for the risk manager components.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from datetime import datetime

from ...strategy_builder.data.dtos import StopLossResult, TakeProfitResult, EntryDecision, ExitDecision


class PositionSizerInterface(ABC):
    """Interface for position sizing calculations."""
    
    @abstractmethod
    def calculate_position_size(
        self,
        entry_price: float,
        account_balance: Optional[float] = None,
        volatility: Optional[float] = None,
        **kwargs
    ) -> float:
        """
        Calculate the position size for a trade.
        
        Args:
            entry_price: The entry price for the trade
            account_balance: Available account balance
            volatility: Market volatility (e.g., ATR)
            **kwargs: Additional parameters specific to sizing type
            
        Returns:
            Position size in base currency units
        """
        pass


class StopLossCalculatorInterface(ABC):
    """Interface for stop loss calculations."""
    
    @abstractmethod
    def calculate_stop_loss(
        self,
        entry_price: float,
        is_long: bool,
        market_data: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> StopLossResult:
        """
        Calculate the stop loss for a trade.
        
        Args:
            entry_price: The entry price for the trade
            is_long: Whether this is a long position
            market_data: Current market data for indicator-based calculations
            **kwargs: Additional parameters specific to stop loss type
            
        Returns:
            StopLossResult with calculated stop loss details
        """
        pass


class TakeProfitCalculatorInterface(ABC):
    """Interface for take profit calculations."""
    
    @abstractmethod
    def calculate_take_profit(
        self,
        entry_price: float,
        is_long: bool,
        market_data: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> TakeProfitResult:
        """
        Calculate the take profit for a trade.
        
        Args:
            entry_price: The entry price for the trade
            is_long: Whether this is a long position
            market_data: Current market data for indicator-based calculations
            **kwargs: Additional parameters specific to take profit type
            
        Returns:
            TakeProfitResult with calculated take profit details
        """
        pass


class EntryManagerInterface(ABC):
    """Interface for the main risk manager."""
    
    @abstractmethod
    def calculate_entry_decision(
        self,
        strategy_name: str,
        symbol: str,
        direction: str,
        entry_price: float,
        decision_time: datetime,
        market_data: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> EntryDecision:
        """
        Calculate a complete entry decision with risk management.
        
        Args:
            strategy_name: Name of the strategy
            symbol: Trading symbol
            direction: Trade direction ('long' or 'short')
            entry_price: Entry price for the trade
            decision_time: Time of the decision
            market_data: Current market data
            **kwargs: Additional parameters
            
        Returns:
            Complete EntryDecision with risk management applied
        """
        pass
    
    @abstractmethod
    def calculate_exit_decision(
        self,
        strategy_name: str,
        symbol: str,
        direction: str,
        decision_time: datetime,
        **kwargs
    ) -> ExitDecision:
        """
        Calculate an exit decision.
        
        Args:
            strategy_name: Name of the strategy
            symbol: Trading symbol
            direction: Trade direction ('long' or 'short')
            decision_time: Time of the decision
            **kwargs: Additional parameters
            
        Returns:
            ExitDecision for the trade
        """
        pass