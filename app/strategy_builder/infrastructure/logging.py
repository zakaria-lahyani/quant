"""
Logging abstraction for the strategy engine.
"""

import logging

from app.strategy_builder.core.domain.protocols import Logger


class StrategyLogger:
    """Concrete implementation of the Logger protocol."""
    
    def __init__(self, name: str = "stratfactory", level: int = logging.INFO):
        """Initialize logger with given name and level."""
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        # Add console handler if none exists
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def info(self, message: str) -> None:
        """Log info message."""
        self.logger.info(message)
    
    def error(self, message: str) -> None:
        """Log error message."""
        self.logger.error(message)
    
    def warning(self, message: str) -> None:
        """Log warning message."""
        self.logger.warning(message)
    
    def exception(self, message: str) -> None:
        """Log exception message."""
        self.logger.exception(message)


class NullLogger:
    """Null object pattern implementation for testing."""
    
    def info(self, message: str) -> None:
        """No-op info logging."""
        pass
    
    def error(self, message: str) -> None:
        """No-op error logging."""
        pass
    
    def warning(self, message: str) -> None:
        """No-op warning logging."""
        pass
    
    def exception(self, message: str) -> None:
        """No-op exception logging."""
        pass


def create_logger(name: str = "stratfactory", level: int = logging.INFO) -> Logger:
    """Factory function to create logger instances."""
    return StrategyLogger(name, level)


def create_null_logger() -> Logger:
    """Factory function to create null logger for testing."""
    return NullLogger()