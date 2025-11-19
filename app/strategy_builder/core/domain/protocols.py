"""
Abstract interfaces and protocols for the strategy engine.
"""

from abc import ABC, abstractmethod
from collections import deque
from typing import Any, Dict, List, Optional, Protocol
import pandas as pd

from app.strategy_builder.core.domain.models import Condition, EntryRules, ExitRules, TradingStrategy


class DataProvider(Protocol):
    """Protocol for data providers that supply market data."""

    def get_recent_rows(self) -> Dict[str, deque[pd.Series]]:
        """Get recent market data rows by timeframe."""
        ...


class Logger(Protocol):
    """Protocol for logging abstraction."""

    def info(self, message: str) -> None:
        """Log info message."""
        ...

    def error(self, message: str) -> None:
        """Log error message."""
        ...

    def warning(self, message: str) -> None:
        """Log warning message."""
        ...

    def exception(self, message: str) -> None:
        """Log exception message."""
        ...


class ConditionEvaluatorInterface(ABC):
    """Abstract interface for condition evaluators."""

    @abstractmethod
    def evaluate(self, condition: Condition) -> bool:
        """Evaluate a single condition."""
        pass


class LogicEvaluatorInterface(ABC):
    """Abstract interface for logic evaluators."""

    @abstractmethod
    def evaluate_entry_rules(self, entry_rules: EntryRules) -> bool:
        """Evaluate entry rules."""
        pass

    @abstractmethod
    def evaluate_exit_rules(self, exit_rules: ExitRules) -> bool:
        """Evaluate exit rules."""
        pass


class StrategyLoaderInterface(ABC):
    """Abstract interface for strategy loaders."""

    @abstractmethod
    def load_strategies(self) -> Dict[str, TradingStrategy]:
        """Load and validate trading strategies."""
        pass

    @abstractmethod
    def get_strategy(self, name: str) -> TradingStrategy:
        """Get a specific strategy by name."""
        pass

    @abstractmethod
    def list_strategy_names(self) -> List[str]:
        """Get list of all loaded strategy names."""
        pass

    @abstractmethod
    def reload_strategies(self) -> Dict[str, TradingStrategy]:
        """Force reload of all strategies."""
        pass


class StrategyExecutorInterface(ABC):
    """Abstract interface for strategy executors."""

    @abstractmethod
    def check_entry(self) -> 'SignalResult':
        """Check entry conditions for the strategy."""
        pass

    @abstractmethod
    def check_exit(self) -> 'SignalResult':
        """Check exit conditions for the strategy."""
        pass

    @abstractmethod
    def get_strategy_name(self) -> str:
        """Get the name of the strategy being executed."""
        pass

    @abstractmethod
    def is_strategy_active(self) -> bool:
        """Check if strategy is currently active based on activation settings."""
        pass

    @abstractmethod
    def validate_data_availability(self) -> bool:
        """Validate that required market data is available for strategy execution."""
        pass


class ValidationService(Protocol):
    """Protocol for validation services."""

    def validate_config(self, config: Dict[str, Any], schema: Dict[str, Any]) -> List[str]:
        """Validate configuration against schema. Returns list of errors."""
        ...


class ConfigurationManager(Protocol):
    """Protocol for configuration management."""

    def load_schema(self, path: str) -> Dict[str, Any]:
        """Load validation schema."""
        ...

    def load_config(self, path: str) -> Dict[str, Any]:
        """Load configuration file."""
        ...


class EvaluatorFactory(Protocol):
    """Protocol for evaluator factory."""

    def create_condition_evaluator(self, data: Dict[str, deque[pd.Series]]) -> ConditionEvaluatorInterface:
        """Create condition evaluator instance."""
        ...

    def create_logic_evaluator(
            self,
            condition_evaluator: ConditionEvaluatorInterface,
            position_data: Optional[Dict[str, Any]] = None
    ) -> LogicEvaluatorInterface:
        """Create logic evaluator instance."""
        ...