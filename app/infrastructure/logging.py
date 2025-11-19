"""
Enhanced logging system with correlation IDs and structured logging.

This module provides a logging system that:
- Adds correlation IDs to trace event flow through the system
- Supports JSON and text output formats
- Allows per-service log level configuration
- Integrates with the event bus for event tracking
"""

import logging
import json
import uuid
from typing import Optional, Dict, Any
from datetime import datetime, timezone
from contextvars import ContextVar
from pathlib import Path

# Context variable to store correlation ID
_correlation_id: ContextVar[Optional[str]] = ContextVar('correlation_id', default=None)


class CorrelationIdFilter(logging.Filter):
    """Add correlation ID to log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Add correlation_id to the log record."""
        record.correlation_id = _correlation_id.get() or "no-correlation-id"
        return True


class JsonFormatter(logging.Formatter):
    """
    Format log records as JSON.

    Each log record is formatted as a JSON object with:
    - timestamp: ISO format timestamp
    - level: Log level (DEBUG, INFO, etc.)
    - logger: Logger name
    - message: Log message
    - correlation_id: Correlation ID for tracing
    - service: Service name (if available)
    - Additional fields from extra dict
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as JSON."""
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": getattr(record, 'correlation_id', 'no-correlation-id'),
        }

        # Add service name if available
        if hasattr(record, 'service'):
            log_data["service"] = record.service

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add any extra fields
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'created', 'filename', 'funcName',
                          'levelname', 'levelno', 'lineno', 'module', 'msecs',
                          'message', 'pathname', 'process', 'processName',
                          'relativeCreated', 'thread', 'threadName', 'exc_info',
                          'exc_text', 'stack_info', 'correlation_id', 'service']:
                try:
                    # Only add JSON-serializable values
                    json.dumps(value)
                    log_data[key] = value
                except (TypeError, ValueError):
                    pass

        return json.dumps(log_data)


class TextFormatter(logging.Formatter):
    """
    Format log records as human-readable text.

    Format: [timestamp] [correlation_id] [service] LEVEL logger: message
    """

    def __init__(self, include_correlation_id: bool = True):
        """
        Initialize the text formatter.

        Args:
            include_correlation_id: Whether to include correlation ID in output
        """
        self.include_correlation_id = include_correlation_id
        super().__init__()

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as text."""
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        parts = [f"[{timestamp}]"]

        if self.include_correlation_id:
            correlation_id = getattr(record, 'correlation_id', 'no-correlation-id')
            parts.append(f"[{correlation_id[:8]}]")

        if hasattr(record, 'service'):
            parts.append(f"[{record.service}]")

        parts.append(f"{record.levelname}")
        parts.append(f"{record.name}:")
        parts.append(record.getMessage())

        message = " ".join(parts)

        if record.exc_info:
            message += "\n" + self.formatException(record.exc_info)

        return message


class LoggingManager:
    """
    Manage logging configuration for the trading system.

    This manager:
    - Configures log format (JSON or text)
    - Sets up correlation ID tracking
    - Configures file and console output
    - Allows per-service log level configuration
    """

    def __init__(
        self,
        level: str = "INFO",
        format_type: str = "text",
        include_correlation_ids: bool = True,
        file_output: bool = False,
        log_file: Optional[str] = None,
        service_levels: Optional[Dict[str, str]] = None
    ):
        """
        Initialize the logging manager.

        Args:
            level: Default log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            format_type: Log format ("json" or "text")
            include_correlation_ids: Whether to include correlation IDs
            file_output: Whether to write logs to file
            log_file: Path to log file (if file_output is True)
            service_levels: Per-service log levels (e.g., {"DataFetchingService": "DEBUG"})
        """
        self.level = level
        self.format_type = format_type
        self.include_correlation_ids = include_correlation_ids
        self.file_output = file_output
        self.log_file = log_file or "logs/trading_system.log"
        self.service_levels = service_levels or {}

        # Track configured loggers
        self.configured_loggers: set[str] = set()

    def configure_root_logger(self) -> None:
        """Configure the root logger with the specified settings."""
        root_logger = logging.getLogger()
        root_logger.setLevel(self.level)

        # Remove existing handlers
        root_logger.handlers.clear()

        # Create formatter
        if self.format_type == "json":
            formatter = JsonFormatter()
        else:
            formatter = TextFormatter(include_correlation_id=self.include_correlation_ids)

        # Add correlation ID filter
        if self.include_correlation_ids:
            correlation_filter = CorrelationIdFilter()
            root_logger.addFilter(correlation_filter)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

        # File handler (if enabled)
        if self.file_output:
            log_path = Path(self.log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            file_handler = logging.FileHandler(self.log_file)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)

    def configure_service_logger(
        self,
        service_name: str,
        logger_name: Optional[str] = None
    ) -> logging.Logger:
        """
        Configure a logger for a specific service.

        Args:
            service_name: Name of the service (used for log context)
            logger_name: Logger name (defaults to service_name)

        Returns:
            Configured logger
        """
        logger_name = logger_name or service_name
        logger = logging.getLogger(logger_name)

        # Set service-specific level if configured
        if service_name in self.service_levels:
            logger.setLevel(self.service_levels[service_name])

        # Add service name to all records
        old_factory = logging.getLogRecordFactory()

        def record_factory(*args, **kwargs):
            record = old_factory(*args, **kwargs)
            record.service = service_name
            return record

        # Only set factory once per logger
        if logger_name not in self.configured_loggers:
            logging.setLogRecordFactory(record_factory)
            self.configured_loggers.add(logger_name)

        return logger

    @staticmethod
    def set_correlation_id(correlation_id: Optional[str] = None) -> str:
        """
        Set the correlation ID for the current context.

        Args:
            correlation_id: Correlation ID to set (generates new one if None)

        Returns:
            The correlation ID that was set
        """
        if correlation_id is None:
            correlation_id = str(uuid.uuid4())

        _correlation_id.set(correlation_id)
        return correlation_id

    @staticmethod
    def get_correlation_id() -> Optional[str]:
        """
        Get the current correlation ID.

        Returns:
            Current correlation ID or None
        """
        return _correlation_id.get()

    @staticmethod
    def clear_correlation_id() -> None:
        """Clear the current correlation ID."""
        _correlation_id.set(None)

    @classmethod
    def from_config(cls, config: Any) -> "LoggingManager":
        """
        Create LoggingManager from configuration.

        Args:
            config: SystemConfig or dict with logging configuration

        Returns:
            Configured LoggingManager
        """
        # Handle both SystemConfig and dict
        if hasattr(config, 'logging'):
            logging_config = config.logging
            level = logging_config.level
            format_type = logging_config.format
            include_correlation_ids = logging_config.correlation_ids
            file_output = logging_config.file_output
            log_file = logging_config.log_file
        else:
            level = config.get('level', 'INFO')
            format_type = config.get('format', 'text')
            include_correlation_ids = config.get('correlation_ids', True)
            file_output = config.get('file_output', False)
            log_file = config.get('log_file', 'logs/trading_system.log')

        manager = cls(
            level=level,
            format_type=format_type,
            include_correlation_ids=include_correlation_ids,
            file_output=file_output,
            log_file=log_file,
        )

        # Configure root logger
        manager.configure_root_logger()

        return manager


class CorrelationContext:
    """
    Context manager for correlation ID tracking.

    Usage:
        ```python
        with CorrelationContext() as correlation_id:
            logger.info("Processing request")
            # All logs in this context will have the same correlation_id
        ```
    """

    def __init__(self, correlation_id: Optional[str] = None):
        """
        Initialize correlation context.

        Args:
            correlation_id: Correlation ID to use (generates new one if None)
        """
        self.correlation_id = correlation_id
        self.previous_correlation_id: Optional[str] = None

    def __enter__(self) -> str:
        """Enter the correlation context."""
        self.previous_correlation_id = LoggingManager.get_correlation_id()
        self.correlation_id = LoggingManager.set_correlation_id(self.correlation_id)
        return self.correlation_id

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the correlation context."""
        if self.previous_correlation_id:
            LoggingManager.set_correlation_id(self.previous_correlation_id)
        else:
            LoggingManager.clear_correlation_id()


def get_logger(
    name: str,
    service_name: Optional[str] = None,
    manager: Optional[LoggingManager] = None
) -> logging.Logger:
    """
    Get a logger with optional service configuration.

    Args:
        name: Logger name
        service_name: Service name for context
        manager: LoggingManager to use for configuration

    Returns:
        Configured logger
    """
    if manager and service_name:
        return manager.configure_service_logger(service_name, name)

    return logging.getLogger(name)
