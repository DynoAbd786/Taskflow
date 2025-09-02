"""
Custom exceptions and error handling for TaskFlow system.
Provides comprehensive exception hierarchy and error recovery mechanisms.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional


class TaskFlowError(Exception):
    """Base exception for all TaskFlow errors."""

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.details = details or {}
        self.cause = cause
        self.timestamp = datetime.now(timezone.utc)

    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary."""
        return {
            "error_type": self.__class__.__name__,
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
            "cause": str(self.cause) if self.cause else None,
        }

    def __str__(self) -> str:
        return f"{self.error_code}: {self.message}"


class ConfigurationError(TaskFlowError):
    """Configuration-related errors."""

    pass


class DatabaseError(TaskFlowError):
    """Database operation errors."""

    pass


class RedisError(TaskFlowError):
    """Redis operation errors."""

    pass


class TaskError(TaskFlowError):
    """Task-related errors."""

    pass


class TaskNotFoundError(TaskError):
    """Task not found error."""

    pass


class TaskExecutionError(TaskError):
    """Task execution error."""

    pass


class TaskTimeoutError(TaskError):
    """Task execution timeout error."""

    pass


class TaskRetryError(TaskError):
    """Task retry limit exceeded error."""

    pass


class TaskValidationError(TaskError):
    """Task validation error."""

    pass


class QueueError(TaskFlowError):
    """Queue operation errors."""

    pass


class QueueNotFoundError(QueueError):
    """Queue not found error."""

    pass


class QueueFullError(QueueError):
    """Queue is full error."""

    pass


class WorkerError(TaskFlowError):
    """Worker-related errors."""

    pass


class WorkerNotFoundError(WorkerError):
    """Worker not found error."""

    pass


class WorkerUnavailableError(WorkerError):
    """Worker unavailable error."""

    pass


class WorkerOverloadedError(WorkerError):
    """Worker overloaded error."""

    pass


class APIError(TaskFlowError):
    """API-related errors."""

    pass


class AuthenticationError(APIError):
    """Authentication error."""

    pass


class AuthorizationError(APIError):
    """Authorization error."""

    pass


class ValidationError(APIError):
    """Request validation error."""

    pass


class RateLimitError(APIError):
    """Rate limit exceeded error."""

    pass


class MonitoringError(TaskFlowError):
    """Monitoring and metrics errors."""

    pass


class AlertError(MonitoringError):
    """Alert system errors."""

    pass


class MetricsError(MonitoringError):
    """Metrics collection errors."""

    pass


class ResourceError(TaskFlowError):
    """Resource-related errors."""

    pass


class MemoryError(ResourceError):
    """Memory limit exceeded error."""

    pass


class DiskSpaceError(ResourceError):
    """Disk space error."""

    pass


class NetworkError(TaskFlowError):
    """Network-related errors."""

    pass


class ConnectionError(NetworkError):
    """Connection error."""

    pass


class TimeoutError(NetworkError):
    """Network timeout error."""

    pass


class SerializationError(TaskFlowError):
    """Serialization/deserialization errors."""

    pass


class SecurityError(TaskFlowError):
    """Security-related errors."""

    pass


class PermissionError(SecurityError):
    """Permission denied error."""

    pass


class IntegrityError(TaskFlowError):
    """Data integrity error."""

    pass


class ConcurrencyError(TaskFlowError):
    """Concurrency-related errors."""

    pass


class LockError(ConcurrencyError):
    """Lock acquisition error."""

    pass


class DeadlockError(ConcurrencyError):
    """Deadlock detected error."""

    pass


class SystemError(TaskFlowError):
    """System-level errors."""

    pass


class ServiceUnavailableError(SystemError):
    """Service unavailable error."""

    pass


class MaintenanceError(SystemError):
    """System in maintenance mode error."""

    pass


# Error code mappings
ERROR_CODES = {
    "TASK_NOT_FOUND": TaskNotFoundError,
    "TASK_EXECUTION_FAILED": TaskExecutionError,
    "TASK_TIMEOUT": TaskTimeoutError,
    "TASK_RETRY_EXCEEDED": TaskRetryError,
    "TASK_VALIDATION_FAILED": TaskValidationError,
    "QUEUE_NOT_FOUND": QueueNotFoundError,
    "QUEUE_FULL": QueueFullError,
    "WORKER_NOT_FOUND": WorkerNotFoundError,
    "WORKER_UNAVAILABLE": WorkerUnavailableError,
    "WORKER_OVERLOADED": WorkerOverloadedError,
    "DATABASE_CONNECTION_FAILED": DatabaseError,
    "DATABASE_OPERATION_FAILED": DatabaseError,
    "REDIS_CONNECTION_FAILED": RedisError,
    "REDIS_OPERATION_FAILED": RedisError,
    "AUTHENTICATION_FAILED": AuthenticationError,
    "AUTHORIZATION_FAILED": AuthorizationError,
    "VALIDATION_FAILED": ValidationError,
    "RATE_LIMIT_EXCEEDED": RateLimitError,
    "CONFIGURATION_INVALID": ConfigurationError,
    "CONFIGURATION_MISSING": ConfigurationError,
    "MEMORY_LIMIT_EXCEEDED": MemoryError,
    "DISK_SPACE_INSUFFICIENT": DiskSpaceError,
    "NETWORK_CONNECTION_FAILED": ConnectionError,
    "NETWORK_TIMEOUT": TimeoutError,
    "SERIALIZATION_FAILED": SerializationError,
    "DESERIALIZATION_FAILED": SerializationError,
    "PERMISSION_DENIED": PermissionError,
    "SECURITY_VIOLATION": SecurityError,
    "DATA_INTEGRITY_VIOLATION": IntegrityError,
    "LOCK_ACQUISITION_FAILED": LockError,
    "DEADLOCK_DETECTED": DeadlockError,
    "SERVICE_UNAVAILABLE": ServiceUnavailableError,
    "SYSTEM_MAINTENANCE": MaintenanceError,
}


def create_error(error_code: str, message: str, **kwargs) -> TaskFlowError:
    """Create an error instance from error code."""
    error_class = ERROR_CODES.get(error_code, TaskFlowError)
    return error_class(message, error_code=error_code, **kwargs)


def is_retryable_error(error: Exception) -> bool:
    """Check if an error is retryable."""
    retryable_errors = (
        NetworkError,
        DatabaseError,
        RedisError,
        ResourceError,
        ServiceUnavailableError,
        TimeoutError,
    )

    return isinstance(error, retryable_errors)


def is_permanent_error(error: Exception) -> bool:
    """Check if an error is permanent (not retryable)."""
    permanent_errors = (
        TaskValidationError,
        AuthenticationError,
        AuthorizationError,
        ValidationError,
        ConfigurationError,
        PermissionError,
        SecurityError,
        IntegrityError,
        TaskNotFoundError,
        QueueNotFoundError,
        WorkerNotFoundError,
    )

    return isinstance(error, permanent_errors)


def get_error_severity(error: Exception) -> str:
    """Get error severity level."""
    if isinstance(error, (SecurityError, IntegrityError, DeadlockError)):
        return "critical"
    elif isinstance(error, (DatabaseError, RedisError, ServiceUnavailableError)):
        return "high"
    elif isinstance(error, (WorkerError, QueueError, NetworkError)):
        return "medium"
    else:
        return "low"


def format_error_for_user(error: Exception) -> str:
    """Format error message for user display."""
    if isinstance(error, TaskFlowError):
        return error.message
    else:
        return str(error)
