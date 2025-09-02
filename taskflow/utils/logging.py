"""
Comprehensive logging system for TaskFlow.
Provides structured logging, log rotation, and centralized log management.
"""

import json
import logging
import logging.handlers
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Union

import structlog
from structlog.stdlib import LoggerFactory

from taskflow.utils.config import get_config


class TaskFlowProcessor:
    """Custom structlog processor for TaskFlow specific formatting."""

    def __call__(self, logger, method_name, event_dict):
        """Process log event dictionary."""
        # Skip adding timestamp - user doesn't want timestamps
        
        # Add component information
        if "component" not in event_dict:
            event_dict["component"] = "unknown"

        # Add severity level
        event_dict["level"] = method_name.upper()

        # Add correlation ID if available
        if hasattr(structlog.contextvars, "correlation_id"):
            correlation_id = structlog.contextvars.get_contextvars().get(
                "correlation_id"
            )
            if correlation_id:
                event_dict["correlation_id"] = correlation_id

        return event_dict


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record):
        """Format log record as JSON without timestamp."""
        log_entry = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception information if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Add extra fields
        if hasattr(record, "component"):
            log_entry["component"] = record.component

        if hasattr(record, "task_id"):
            log_entry["task_id"] = record.task_id

        if hasattr(record, "worker_id"):
            log_entry["worker_id"] = record.worker_id

        if hasattr(record, "queue_name"):
            log_entry["queue_name"] = record.queue_name

        return json.dumps(log_entry)


class ColoredFormatter(logging.Formatter):
    """Colored formatter for console output."""

    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"

    def format(self, record):
        """Format log record with colors."""
        color = self.COLORS.get(record.levelname, "")
        reset = self.RESET

        record.levelname = f"{color}{record.levelname}{reset}"

        formatted = super().format(record)
        return formatted


class LoggingManager:
    """Central logging management for TaskFlow."""

    def __init__(self):
        self.config = get_config()
        self._initialized = False
        self.loggers: Dict[str, logging.Logger] = {}

    def setup_logging(self) -> None:
        """Setup logging configuration."""
        if self._initialized:
            return

        # Configure structlog
        import logging

        log_level = getattr(logging, self.config.logging.level.upper())

        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.processors.add_log_level,
                structlog.processors.StackInfoRenderer(),
                TaskFlowProcessor(),
                structlog.dev.ConsoleRenderer()
                if self.config.logging.format == "console"
                else structlog.processors.JSONRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(log_level),
            logger_factory=LoggerFactory(),
            context_class=dict,
            cache_logger_on_first_use=True,
        )

        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, self.config.logging.level))

        # Remove existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Add console handler
        console_handler = logging.StreamHandler(sys.stdout)
        if self.config.logging.format == "json":
            console_handler.setFormatter(JSONFormatter())
        else:
            console_handler.setFormatter(
                ColoredFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            )
        root_logger.addHandler(console_handler)

        # Add file handler if configured
        if self.config.logging.file:
            self._setup_file_logging()

        # Setup component loggers
        self._setup_component_loggers()

        self._initialized = True

    def _setup_file_logging(self) -> None:
        """Setup file logging with rotation."""
        log_file = Path(self.config.logging.file)
        log_file.parent.mkdir(parents=True, exist_ok=True)

        # Rotating file handler
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=self.config.logging.max_size * 1024 * 1024,  # Convert MB to bytes
            backupCount=self.config.logging.backup_count,
        )

        if self.config.logging.format == "json":
            file_handler.setFormatter(JSONFormatter())
        else:
            file_handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
            )

        logging.getLogger().addHandler(file_handler)

    def _setup_component_loggers(self) -> None:
        """Setup loggers for different components."""
        components = [
            "taskflow.core",
            "taskflow.workers",
            "taskflow.api",
            "taskflow.monitoring",
            "taskflow.db",
            "taskflow.cli",
        ]

        for component in components:
            logger = logging.getLogger(component)
            logger.setLevel(getattr(logging, self.config.logging.level))
            self.loggers[component] = logger

    def get_logger(self, name: str) -> structlog.BoundLogger:
        """Get a structured logger for a component."""
        if not self._initialized:
            self.setup_logging()

        return structlog.get_logger(name)

    def get_standard_logger(self, name: str) -> logging.Logger:
        """Get a standard logger."""
        if not self._initialized:
            self.setup_logging()

        return logging.getLogger(name)

    def set_correlation_id(self, correlation_id: str) -> None:
        """Set correlation ID for request tracing."""
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(correlation_id=correlation_id)

    def clear_correlation_id(self) -> None:
        """Clear correlation ID."""
        structlog.contextvars.clear_contextvars()

    def log_task_event(
        self,
        task_id: str,
        event: str,
        status: str,
        worker_id: Optional[str] = None,
        queue_name: Optional[str] = None,
        execution_time: Optional[float] = None,
        error: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Log task-related events."""
        logger = self.get_logger("taskflow.core.task")

        log_data = {
            "task_id": task_id,
            "event": event,
            "status": status,
            "component": "task",
        }

        if worker_id:
            log_data["worker_id"] = worker_id

        if queue_name:
            log_data["queue_name"] = queue_name

        if execution_time:
            log_data["execution_time"] = execution_time

        if error:
            log_data["error"] = error

        log_data.update(kwargs)

        if error:
            logger.error(f"Task {event}", **log_data)
        else:
            logger.info(f"Task {event}", **log_data)

    def log_worker_event(
        self,
        worker_id: str,
        event: str,
        status: str,
        queue_names: Optional[list] = None,
        **kwargs,
    ) -> None:
        """Log worker-related events."""
        logger = self.get_logger("taskflow.workers")

        log_data = {
            "worker_id": worker_id,
            "event": event,
            "status": status,
            "component": "worker",
        }

        if queue_names:
            log_data["queue_names"] = queue_names

        log_data.update(kwargs)

        logger.info(f"Worker {event}", **log_data)

    def log_api_request(
        self,
        method: str,
        path: str,
        status_code: int,
        response_time: float,
        user_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Log API requests."""
        logger = self.get_logger("taskflow.api")

        log_data = {
            "method": method,
            "path": path,
            "status_code": status_code,
            "response_time": response_time,
            "component": "api",
        }

        if user_id:
            log_data["user_id"] = user_id

        log_data.update(kwargs)

        if status_code >= 400:
            logger.warning("API request", **log_data)
        else:
            logger.info("API request", **log_data)

    def log_system_event(
        self,
        event: str,
        component: str,
        severity: str = "info",
        details: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        """Log system events."""
        logger = self.get_logger("taskflow.system")

        log_data = {
            "event": event,
            "component": component,
            "severity": severity,
        }

        if details:
            log_data["details"] = details

        log_data.update(kwargs)

        log_method = getattr(logger, severity.lower(), logger.info)
        log_method(f"System event: {event}", **log_data)

    def log_database_operation(
        self,
        operation: str,
        table: str,
        execution_time: Optional[float] = None,
        rows_affected: Optional[int] = None,
        error: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Log database operations."""
        logger = self.get_logger("taskflow.db")

        log_data = {
            "operation": operation,
            "table": table,
            "component": "database",
        }

        if execution_time:
            log_data["execution_time"] = execution_time

        if rows_affected is not None:
            log_data["rows_affected"] = rows_affected

        if error:
            log_data["error"] = error

        log_data.update(kwargs)

        if error:
            logger.error(f"Database operation failed: {operation}", **log_data)
        else:
            logger.debug(f"Database operation: {operation}", **log_data)

    def configure_uvicorn_logging(self) -> Dict[str, Any]:
        """Configure uvicorn logging."""
        if self.config.logging.format == "json":
            return {
                "version": 1,
                "disable_existing_loggers": False,
                "formatters": {
                    "default": {
                        "()": JSONFormatter,
                    },
                },
                "handlers": {
                    "default": {
                        "formatter": "default",
                        "class": "logging.StreamHandler",
                        "stream": "ext://sys.stdout",
                    },
                },
                "root": {
                    "level": self.config.logging.level,
                    "handlers": ["default"],
                },
                "loggers": {
                    "uvicorn": {
                        "level": self.config.logging.level,
                        "handlers": ["default"],
                        "propagate": False,
                    },
                    "uvicorn.error": {
                        "level": self.config.logging.level,
                        "handlers": ["default"],
                        "propagate": False,
                    },
                    "uvicorn.access": {
                        "level": self.config.logging.level,
                        "handlers": ["default"],
                        "propagate": False,
                    },
                },
            }

        return {}


class LogContext:
    """Context manager for adding structured logging context."""

    def __init__(self, **context):
        self.context = context
        self.previous_context = {}

    def __enter__(self):
        self.previous_context = structlog.contextvars.get_contextvars()
        structlog.contextvars.bind_contextvars(**self.context)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        structlog.contextvars.clear_contextvars()
        if self.previous_context:
            structlog.contextvars.bind_contextvars(**self.previous_context)


# Global logging manager instance
_logging_manager: Optional[LoggingManager] = None


def get_logging_manager() -> LoggingManager:
    """Get the global logging manager instance."""
    global _logging_manager
    if _logging_manager is None:
        _logging_manager = LoggingManager()
    return _logging_manager


def setup_logging() -> None:
    """Setup logging for TaskFlow."""
    manager = get_logging_manager()
    manager.setup_logging()


def get_logger(name: str) -> structlog.BoundLogger:
    """Get a structured logger."""
    manager = get_logging_manager()
    return manager.get_logger(name)


def log_context(**context) -> LogContext:
    """Create a logging context manager."""
    return LogContext(**context)


def set_correlation_id(correlation_id: str) -> None:
    """Set correlation ID for request tracing."""
    manager = get_logging_manager()
    manager.set_correlation_id(correlation_id)


def clear_correlation_id() -> None:
    """Clear correlation ID."""
    manager = get_logging_manager()
    manager.clear_correlation_id()
