"""
Main risk manager implementation that coordinates all risk management components.
"""

from typing import Optional, Dict, Any, List, Union
from collections import deque
from datetime import datetime
import logging
import pandas as pd

from .core.interfaces import EntryManagerInterface
from .core.exceptions import ValidationError, CalculationError
from .position_sizing.factory import create_position_sizer
from .stop_loss.factory import create_stop_loss_calculator
from .take_profit.factory import create_take_profit_calculator

from ..strategy_builder.core.domain.models import TradingStrategy
from ..strategy_builder.core.domain.enums import TimeFrameEnum
from ..strategy_builder.data.dtos import EntryDecision, ExitDecision, Trades
from ..utils.functions_helper import generate_magic_number
from ..utils.logger import AppLogger


class EntryManager(EntryManagerInterface):
    """
    Main risk manager that coordinates position sizing, stop loss, and take profit calculations.
    """
    
    def __init__(
        self,
        strategies: Dict[str, TradingStrategy],
        symbol: str,
        pip_value: float,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the risk manager.
        
        Args:
            strategies: Dictionary of strategy configurations
            symbol: Trading symbol
            pip_value: Value used to convert pips to price distance (default: 10000.0 for forex)
                      - Forex major pairs: 10000.0 (1 pip = 0.0001)
                      - Forex JPY pairs: 100.0 (1 pip = 0.01)
                      - Stocks: 100.0 (1 cent = 0.01)
                      - Crypto: 100.0 (1 cent = 0.01)
                      - Indices: 100.0 (1 point = 0.01)
            logger: Optional logger instance
        """
        if pip_value <= 0:
            raise ValidationError(
                "pip_value must be positive",
                field_name="pip_value"
            )
        
        self.strategies = strategies
        self.symbol = symbol
        self.pip_value = pip_value
        self.logger = logger or AppLogger.get_logger("risk-manager")
        
        # Validate strategies
        self._validate_strategies()
    
    def _validate_strategies(self) -> None:
        """Validate that all strategies have proper risk configurations."""
        for name, strategy in self.strategies.items():
            if not strategy.risk:
                raise ValidationError(
                    f"Strategy '{name}' must have risk configuration",
                    field_name="risk"
                )
            
            if not strategy.risk.sl:
                raise ValidationError(
                    f"Strategy '{name}' must have stop loss configuration",
                    field_name="risk.sl"
                )
            
            if not strategy.risk.tp:
                raise ValidationError(
                    f"Strategy '{name}' must have take profit configuration",
                    field_name="risk.tp"
                )
    
    def calculate_entry_decision(
        self,
        strategy_name: str,
        symbol: str,
        direction: str,
        entry_price: float,
        decision_time: datetime,
        market_data: Optional[Dict[str, Union[deque, List[Dict[str, Any]]]]] = None,
        account_balance: Optional[float] = None,
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
            market_data: Current market data (deques of Series or lists of dicts)
            account_balance: Available account balance
            **kwargs: Additional parameters
            
        Returns:
            Complete EntryDecision with risk management applied
        """
        if strategy_name not in self.strategies:
            raise ValidationError(
                f"Strategy '{strategy_name}' not found",
                field_name="strategy_name"
            )
        
        strategy = self.strategies[strategy_name]
        is_long = direction.lower() == "long"
        
        self.logger.debug(
            f"Calculating entry decision for {strategy_name}: "
            f"symbol={symbol}, direction={direction}, entry_price={entry_price}"
        )
        
        try:
            # Calculate position size
            position_size = self._calculate_position_size(
                strategy, entry_price, account_balance, market_data, **kwargs
            )
            
            # Calculate stop loss
            stop_loss_result = self._calculate_stop_loss(
                strategy, entry_price, is_long, market_data, position_size=position_size, **kwargs
            )
            
            # Calculate take profit
            take_profit_result = self._calculate_take_profit(
                strategy, entry_price, is_long, market_data, **kwargs
            )
            
            # Determine entry type based on ATR distance
            atr_distance = getattr(strategy.risk.position_sizing, 'atr_distance', 0) or 0
            if atr_distance > 0.001:  # 1 pip threshold for forex
                entry_signals = "BUY_LIMIT" if is_long else "SELL_LIMIT"
                actual_entry_price = entry_price - atr_distance if is_long else entry_price + atr_distance
            else:
                entry_signals = "BUY" if is_long else "SELL"
                actual_entry_price = entry_price
            
            # Generate magic number
            magic = generate_magic_number(
                strategy_name, symbol, strategy.timeframes, direction
            )
            
            # Check if this is a manual signal
            is_manual = kwargs.get('is_manual', False)

            entry_decision = EntryDecision(
                symbol=symbol,
                strategy_name=strategy_name,
                magic=magic,
                direction=direction,
                entry_signals=entry_signals,
                entry_price=actual_entry_price,
                position_size=position_size,
                stop_loss=stop_loss_result,
                take_profit=take_profit_result,
                decision_time=decision_time,
                is_manual=is_manual
            )
            
            self.logger.info(
                f"Entry decision calculated: {strategy_name} {direction} "
                f"size={position_size} @ {actual_entry_price}"
            )
            
            return entry_decision
            
        except Exception as e:
            self.logger.error(f"Error calculating entry decision: {e}")
            raise CalculationError(
                f"Failed to calculate entry decision for {strategy_name}: {str(e)}",
                calculation_type="entry_decision"
            )
    
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
        if strategy_name not in self.strategies:
            raise ValidationError(
                f"Strategy '{strategy_name}' not found",
                field_name="strategy_name"
            )
        
        strategy = self.strategies[strategy_name]
        
        # Generate magic number
        magic = generate_magic_number(
            strategy_name, symbol, strategy.timeframes, direction
        )
        
        # Get optional parameters for manual exits
        close_percent = kwargs.get('close_percent', 100.0)
        is_manual = kwargs.get('is_manual', False)

        exit_decision = ExitDecision(
            symbol=symbol,
            strategy_name=strategy_name,
            magic=magic,
            direction=direction,
            decision_time=decision_time,
            close_percent=close_percent,
            is_manual=is_manual
        )

        pct_text = f" ({close_percent}%)" if close_percent < 100 else ""
        self.logger.info(
            f"Exit decision calculated: {strategy_name} {direction}{pct_text}"
        )
        
        return exit_decision
    
    def _calculate_position_size(
        self,
        strategy: TradingStrategy,
        entry_price: float,
        account_balance: Optional[float],
        market_data: Optional[Dict[str, Union[deque, List[Dict[str, Any]]]]],
        **kwargs
    ) -> float:
        """Calculate position size in units/lots using the appropriate sizer."""
        if not strategy.risk.position_sizing:
            raise CalculationError(
                "Strategy must have position sizing configuration",
                calculation_type="position_sizing"
            )
        
        sizer = create_position_sizer(strategy.risk.position_sizing, self.logger)
        
        # Extract volatility from market data if available
        volatility = None
        if market_data:
            # First check if ATR is directly in kwargs or market_data root
            volatility = kwargs.get('ATR') or market_data.get('ATR') or market_data.get('volatility')
            
            # If not found, try to extract from the most recent data point
            if volatility is None:
                for tf_data in market_data.values():
                    if isinstance(tf_data, deque) and tf_data:
                        last_row = tf_data[-1]
                        if isinstance(last_row, pd.Series):
                            volatility = last_row.get('ATR') or last_row.get('volatility')
                            if volatility is not None:
                                break
                    elif isinstance(tf_data, list) and tf_data:
                        last_bar = tf_data[-1]
                        if isinstance(last_bar, dict):
                            volatility = last_bar.get('ATR') or last_bar.get('volatility')
                            if volatility is not None:
                                break
        
        # For percentage and other monetary-based sizing, return units instead of monetary value
        if hasattr(sizer, 'get_position_units'):
            return sizer.get_position_units(
                entry_price=entry_price,
                account_balance=account_balance,
                volatility=volatility,
                **kwargs
            )
        else:
            # Fallback for other sizer types that return units directly
            return sizer.calculate_position_size(
                entry_price=entry_price,
                account_balance=account_balance,
                volatility=volatility,
                **kwargs
            )
    
    def _calculate_stop_loss(
        self,
        strategy: TradingStrategy,
        entry_price: float,
        is_long: bool,
        market_data: Optional[Dict[str, Union[deque, List[Dict[str, Any]]]]],
        position_size: Optional[float] = None,
        **kwargs
    ):
        """Calculate stop loss using the appropriate calculator."""
        calculator = create_stop_loss_calculator(strategy.risk.sl, self.pip_value, self.logger)
        
        return calculator.calculate_stop_loss(
            entry_price=entry_price,
            is_long=is_long,
            market_data=market_data,
            position_size=position_size,
            **kwargs
        )
    
    def _calculate_take_profit(
        self,
        strategy: TradingStrategy,
        entry_price: float,
        is_long: bool,
        market_data: Optional[Dict[str, Union[deque, List[Dict[str, Any]]]]],
        **kwargs
    ):
        """Calculate take profit using the appropriate calculator."""
        calculator = create_take_profit_calculator(strategy.risk.tp, self.pip_value, self.logger)
        
        return calculator.calculate_take_profit(
            entry_price=entry_price,
            is_long=is_long,
            market_data=market_data,
            **kwargs
        )
    
    def manage_trades(
        self,
        strategy_results: Dict[str, Any],
        market_data: Dict[str, Union[deque, List[Dict[str, Any]]]],
        account_balance: Optional[float] = None,
        is_manual: bool = False,
        **kwargs
    ) -> Trades:
        """
        Manage trades based on strategy evaluation results.

        Args:
            strategy_results: Results from strategy evaluation
            market_data: Current market data (can be deques of Series or lists of dicts)
            account_balance: Available account balance
            is_manual: True for manual signals (adds "Manual_" prefix to trade comments)
            **kwargs: Additional parameters

        Returns:
            Trades object with entry and exit decisions
        """
        entries: List[EntryDecision] = []
        exits: List[ExitDecision] = []
        
        # Extract decision time from market data
        decision_time = datetime.now()
        if market_data:
            # Try to get time from market data
            for timeframe_data in market_data.values():
                if isinstance(timeframe_data, deque) and timeframe_data:
                    # Handle deque of Series (streaming data)
                    last_row = timeframe_data[-1]
                    if isinstance(last_row, pd.Series) and 'time' in last_row.index:
                        decision_time = last_row['time']
                        break
                elif isinstance(timeframe_data, list) and timeframe_data:
                    # Handle list of dicts (legacy format)
                    last_bar = timeframe_data[-1]
                    if isinstance(last_bar, dict) and 'time' in last_bar:
                        decision_time = last_bar['time']
                        break
        
        for strategy_name, eval_result in strategy_results.items():
            if strategy_name not in self.strategies:
                self.logger.warning(f"Strategy '{strategy_name}' not found in risk manager")
                continue
            
            strategy = self.strategies[strategy_name]
            
            # Get the lowest timeframe for price data
            lowest_tf = min(strategy.timeframes)
            
            # Extract current price
            current_price = self._extract_current_price(market_data, lowest_tf)
            
            # Check for exit signals
            if hasattr(eval_result, 'exit') and eval_result.exit:
                if eval_result.exit.long or eval_result.exit.short:
                    direction = "long" if eval_result.exit.long else "short"
                    # Get close_percent from evaluation result (for manual exits)
                    close_percent = getattr(eval_result, 'close_percent', 100.0)
                    exit_decision = self.calculate_exit_decision(
                        strategy_name, self.symbol, direction, decision_time,
                        close_percent=close_percent, is_manual=is_manual
                    )
                    exits.append(exit_decision)
            
            # Check for entry signals
            if hasattr(eval_result, 'entry') and eval_result.entry:
                if eval_result.entry.long:
                    entry_decision = self.calculate_entry_decision(
                        strategy_name, self.symbol, "long", current_price,
                        decision_time, market_data, account_balance,
                        is_manual=is_manual, **kwargs
                    )
                    entries.append(entry_decision)

                if eval_result.entry.short:
                    entry_decision = self.calculate_entry_decision(
                        strategy_name, self.symbol, "short", current_price,
                        decision_time, market_data, account_balance,
                        is_manual=is_manual, **kwargs
                    )
                    entries.append(entry_decision)

        return Trades(entries=entries, exits=exits)
    
    def _extract_current_price(
        self,
        market_data: Dict[str, Union[deque, List[Dict[str, Any]]]],
        timeframe: TimeFrameEnum
    ) -> float:
        """Extract current price from market data (supports both deque and list formats)."""
        tf_key = str(timeframe)
        
        if tf_key in market_data:
            tf_data = market_data[tf_key]
            
            # Handle deque of Series (streaming data)
            if isinstance(tf_data, deque) and tf_data:
                last_row = tf_data[-1]
                if isinstance(last_row, pd.Series) and 'close' in last_row.index:
                    return float(last_row['close'])
            # Handle list of dicts (legacy format)
            elif isinstance(tf_data, list) and tf_data:
                last_bar = tf_data[-1]
                if isinstance(last_bar, dict) and 'close' in last_bar:
                    return float(last_bar['close'])
        
        # Fallback: try to find any price data
        for data in market_data.values():
            if isinstance(data, deque) and data:
                last_row = data[-1]
                if isinstance(last_row, pd.Series) and 'close' in last_row.index:
                    return float(last_row['close'])
            elif isinstance(data, list) and data:
                last_bar = data[-1]
                if isinstance(last_bar, dict) and 'close' in last_bar:
                    return float(last_bar['close'])
        
        raise CalculationError(
            "Could not extract current price from market data",
            calculation_type="price_extraction"
        )
    
    def get_strategy_risk_summary(self, strategy_name: str) -> Dict[str, Any]:
        """
        Get a summary of risk parameters for a strategy.
        
        Args:
            strategy_name: Name of the strategy
            
        Returns:
            Dictionary with risk summary
        """
        if strategy_name not in self.strategies:
            raise ValidationError(
                f"Strategy '{strategy_name}' not found",
                field_name="strategy_name"
            )
        
        strategy = self.strategies[strategy_name]
        
        return {
            'strategy_name': strategy_name,
            'position_sizing': {
                'type': strategy.risk.position_sizing.type if strategy.risk.position_sizing else None,
                'value': strategy.risk.position_sizing.value if strategy.risk.position_sizing else None,
                'atr_distance': getattr(strategy.risk.position_sizing, 'atr_distance', None) if strategy.risk.position_sizing else None
            },
            'stop_loss': {
                'type': strategy.risk.sl.type,
                'config': strategy.risk.sl.model_dump() if hasattr(strategy.risk.sl, 'model_dump') else str(strategy.risk.sl)
            },
            'take_profit': {
                'type': strategy.risk.tp.type,
                'config': strategy.risk.tp.model_dump() if hasattr(strategy.risk.tp, 'model_dump') else str(strategy.risk.tp)
            },
            'timeframes': [str(tf) for tf in strategy.timeframes]
        }