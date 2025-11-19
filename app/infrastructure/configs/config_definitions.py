"""
Configuration models and loader for the trading system.

This module provides Pydantic models for validating configuration
and a loader that reads from YAML files with environment variable overrides.
"""

import os
import logging
from typing import List, Optional, Dict, Any, Literal
from pathlib import Path

from pydantic import BaseModel, Field, field_validator
import yaml


class DataFetchingConfig(BaseModel):
    """Configuration for DataFetchingService."""

    enabled: bool = True
    fetch_interval: int = Field(default=5, ge=1, le=60)
    retry_attempts: int = Field(default=3, ge=0, le=10)
    candle_index: int = Field(default=1, ge=1)
    nbr_bars: int = Field(default=3, ge=1)


class IndicatorCalculationConfig(BaseModel):
    """Configuration for IndicatorCalculationService."""

    enabled: bool = True
    recent_rows_limit: int = Field(default=6, ge=1)
    track_regime_changes: bool = True


class StrategyEvaluationConfig(BaseModel):
    """Configuration for StrategyEvaluationService."""

    enabled: bool = True
    evaluation_mode: Literal["on_new_candle", "continuous"] = "on_new_candle"
    min_rows_required: int = Field(default=3, ge=1)


class TradeExecutionConfig(BaseModel):
    """Configuration for TradeExecutionService."""

    enabled: bool = True
    execution_mode: Literal["immediate", "batch"] = "immediate"
    batch_size: int = Field(default=1, ge=1)


class ServicesConfig(BaseModel):
    """Configuration for all services."""

    data_fetching: DataFetchingConfig = Field(default_factory=DataFetchingConfig)
    indicator_calculation: IndicatorCalculationConfig = Field(
        default_factory=IndicatorCalculationConfig
    )
    strategy_evaluation: StrategyEvaluationConfig = Field(
        default_factory=StrategyEvaluationConfig
    )
    trade_execution: TradeExecutionConfig = Field(
        default_factory=TradeExecutionConfig
    )


class EventBusConfig(BaseModel):
    """Configuration for EventBus."""

    mode: Literal["synchronous", "asynchronous"] = "synchronous"
    event_history_limit: int = Field(default=1000, ge=0)
    log_all_events: bool = False


class OrchestratorConfig(BaseModel):
    """Configuration for TradingOrchestrator."""

    enable_auto_restart: bool = True
    health_check_interval: int = Field(default=60, ge=10)
    status_log_interval: int = Field(default=10, ge=1)


class LoggingConfig(BaseModel):
    """Configuration for logging."""

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    format: Literal["json", "text"] = "text"
    correlation_ids: bool = True
    file_output: bool = False
    log_file: str = "logs/trading_system.log"


class SymbolConfig(BaseModel):
    symbol: str
    pip_value: float
    position_split: int
    scaling_type: str
    entry_spacing: float
    risk_per_group: float
    timeframes: List[str]

class TradingConfig(BaseModel):
    """Configuration for trading parameters."""

    symbols: List[SymbolConfig]
    timeframes: Optional[List[str]] = None  # Optional: can be defined per symbol instead

    @field_validator("symbols")
    @classmethod
    def validate_symbols(cls, v: List[SymbolConfig]) -> List[SymbolConfig]:
        """Validate symbols are not empty and normalize symbol names."""
        if not v:
            raise ValueError("At least one symbol must be specified")

        # Normalize symbol names to uppercase
        for symbol_config in v:
            symbol_config.symbol = symbol_config.symbol.upper()

        return v

    @field_validator("timeframes")
    @classmethod
    def validate_timeframes(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """Validate timeframes are not empty if provided."""
        if v is not None and not v:
            raise ValueError("timeframes list cannot be empty if provided")
        return v


class AccountStopLossConfig(BaseModel):
    """Configuration for account-level stop loss."""

    enabled: bool = True
    daily_loss_limit: float = Field(default=1000.0, ge=0)
    max_drawdown_pct: float = Field(default=10.0, ge=0, le=100)
    close_positions_on_breach: bool = True
    stop_trading_on_breach: bool = True
    cooldown_period_minutes: int = Field(default=60, ge=0)
    daily_reset_time: str = "00:00:00"
    timezone_offset: str = "+00:00"


class RiskConfig(BaseModel):
    """Configuration for risk management."""

    daily_loss_limit: float = Field(default=1000.0, ge=0)  # Legacy - use account_stop_loss.daily_loss_limit
    max_positions: int = Field(default=10, ge=1)
    max_position_size: float = Field(default=1.0, ge=0.01)
    account_stop_loss: AccountStopLossConfig = Field(default_factory=AccountStopLossConfig)




class SystemConfig(BaseModel):
    """Complete system configuration."""

    services: ServicesConfig = Field(default_factory=ServicesConfig)
    event_bus: EventBusConfig = Field(default_factory=EventBusConfig)
    orchestrator: OrchestratorConfig = Field(default_factory=OrchestratorConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    trading: TradingConfig
    risk: RiskConfig = Field(default_factory=RiskConfig)

    def to_orchestrator_config(self) -> Dict[str, Any]:
        """
        Convert to orchestrator configuration dictionary.

        Returns:
            Dictionary suitable for TradingOrchestrator initialization
        """
        return {
            "symbols": self.trading.symbols,
            "timeframes": self.trading.timeframes,  # May be None if defined per symbol
            "enable_auto_restart": self.orchestrator.enable_auto_restart,
            "health_check_interval": self.orchestrator.health_check_interval,
            "event_history_limit": self.event_bus.event_history_limit,
            "log_all_events": self.event_bus.log_all_events,
            "candle_index": self.services.data_fetching.candle_index,
            "nbr_bars": self.services.data_fetching.nbr_bars,
            "track_regime_changes": self.services.indicator_calculation.track_regime_changes,
            "min_rows_required": self.services.strategy_evaluation.min_rows_required,
            "execution_mode": self.services.trade_execution.execution_mode,
        }

    def get_data_fetching_config(self, symbol: str) -> Dict[str, Any]:
        """
        Get configuration for DataFetchingService for a specific symbol.

        Args:
            symbol: Trading symbol (e.g., "XAUUSD")

        Returns:
            Configuration dictionary for DataFetchingService
        """
        # Find the symbol config to get symbol-specific timeframes
        symbol_config = next((s for s in self.trading.symbols if s.symbol.upper() == symbol.upper()), None)
        timeframes = symbol_config.timeframes if symbol_config else self.trading.timeframes

        return {
            "symbol": symbol,
            "timeframes": timeframes,
            "candle_index": self.services.data_fetching.candle_index,
            "nbr_bars": self.services.data_fetching.nbr_bars,
        }

    def get_indicator_calculation_config(self, symbol: str) -> Dict[str, Any]:
        """
        Get configuration for IndicatorCalculationService for a specific symbol.

        Args:
            symbol: Trading symbol (e.g., "XAUUSD")

        Returns:
            Configuration dictionary for IndicatorCalculationService
        """
        # Find the symbol config to get symbol-specific timeframes
        symbol_config = next((s for s in self.trading.symbols if s.symbol.upper() == symbol.upper()), None)
        timeframes = symbol_config.timeframes if symbol_config else self.trading.timeframes

        return {
            "symbol": symbol,
            "timeframes": timeframes,
            "track_regime_changes": self.services.indicator_calculation.track_regime_changes,
        }

    def get_strategy_evaluation_config(self, symbol: str) -> Dict[str, Any]:
        """
        Get configuration for StrategyEvaluationService for a specific symbol.

        Args:
            symbol: Trading symbol (e.g., "XAUUSD")

        Returns:
            Configuration dictionary for StrategyEvaluationService
        """
        return {
            "symbol": symbol,
            "min_rows_required": self.services.strategy_evaluation.min_rows_required,
        }

    def get_trade_execution_config(self, symbol: str) -> Dict[str, Any]:
        """
        Get configuration for TradeExecutionService for a specific symbol.

        Args:
            symbol: Trading symbol (e.g., "XAUUSD")

        Returns:
            Configuration dictionary for TradeExecutionService
        """
        return {
            "symbol": symbol,
            "execution_mode": self.services.trade_execution.execution_mode,
            "batch_size": self.services.trade_execution.batch_size,
        }


