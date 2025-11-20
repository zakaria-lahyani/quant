"""
Logging abstraction for the risk manager.
"""

import logging
from typing import Protocol, runtime_checkable


@runtime_checkable
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
    
    def debug(self, message: str) -> None:
        """Log debug message."""
        ...
    
    def exception(self, message: str) -> None:
        """Log exception message."""
        ...


class RiskManagerLogger:
    """Concrete implementation of the Logger protocol."""
    
    def __init__(self, name: str = "risk-manager", level: int = logging.INFO):
        """Initialize logger with given name and level."""
        self.name = name
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
    
    def debug(self, message: str) -> None:
        """Log debug message."""
        self.logger.debug(message)
    
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
    
    def debug(self, message: str) -> None:
        """No-op debug logging."""
        pass
    
    def exception(self, message: str) -> None:
        """No-op exception logging."""
        pass


def create_logger(name: str = "risk-manager", level: int = logging.INFO) -> Logger:
    """Factory function to create logger instances."""
    return RiskManagerLogger(name, level)


def create_null_logger() -> Logger:
    """Factory function to create null logger for testing."""
    return NullLogger()


# Backward compatibility - keep AppLogger for existing code
class AppLogger:
    """Backward compatibility wrapper."""
    
    @classmethod
    def get_logger(cls, name: str, level: int = logging.INFO) -> Logger:
        """Get or create a logger with the given name."""
        return create_logger(name, level)
    
    @classmethod
    def set_level(cls, name: str, level: int) -> None:
        """Set the logging level for a specific logger."""
        logger = logging.getLogger(name)
        logger.setLevel(level)