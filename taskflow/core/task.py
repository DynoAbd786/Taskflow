"""
Core task definition and status management.
Defines the Task class and related enums for the TaskFlow system.
"""

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, validator


class TaskStatus(str, Enum):
    """Task execution status."""

    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"
    RETRY = "retry"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class TaskPriority(str, Enum):
    """Task priority levels."""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class TaskResult:
    """Task execution result."""

    status: TaskStatus
    result: Optional[Any] = None
    error: Optional[str] = None
    traceback: Optional[str] = None
    execution_time: Optional[float] = None
    worker_id: Optional[str] = None
    completed_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "status": self.status.value
            if hasattr(self.status, "value")
            else str(self.status),
            "result": self.result,
            "error": self.error,
            "traceback": self.traceback,
            "execution_time": self.execution_time,
            "worker_id": self.worker_id,
            "completed_at": self.completed_at.isoformat()
            if self.completed_at
            else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskResult":
        """Create from dictionary."""
        completed_at = None
        if data.get("completed_at"):
            completed_at = datetime.fromisoformat(data["completed_at"])

        return cls(
            status=TaskStatus(data["status"]),
            result=data.get("result"),
            error=data.get("error"),
            traceback=data.get("traceback"),
            execution_time=data.get("execution_time"),
            worker_id=data.get("worker_id"),
            completed_at=completed_at,
        )


class TaskConfig(BaseModel):
    """Task configuration options."""

    timeout: int = Field(default=300, description="Task timeout in seconds")
    max_retries: int = Field(default=100, description="Maximum retry attempts")
    retry_delay: int = Field(default=2, description="Delay between retries in seconds")
    priority: TaskPriority = Field(
        default=TaskPriority.NORMAL, description="Task priority"
    )
    queue: str = Field(default="default", description="Queue name")
    depends_on: List[str] = Field(default_factory=list, description="Task dependencies")
    tags: List[str] = Field(default_factory=list, description="Task tags")

    @validator("timeout")
    def validate_timeout(cls, v):
        if v <= 0:
            raise ValueError("Timeout must be positive")
        return v

    @validator("max_retries")
    def validate_max_retries(cls, v):
        if v < 0:
            raise ValueError("Max retries cannot be negative")
        return v


class Task(BaseModel):
    """Core task definition."""

    id: str = Field(
        default_factory=lambda: str(uuid.uuid4()), description="Unique task ID"
    )
    name: str = Field(description="Task name/function")
    args: List[Any] = Field(default_factory=list, description="Positional arguments")
    kwargs: Dict[str, Any] = Field(
        default_factory=dict, description="Keyword arguments"
    )
    config: TaskConfig = Field(
        default_factory=TaskConfig, description="Task configuration"
    )

    status: TaskStatus = Field(default=TaskStatus.PENDING, description="Current status")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    scheduled_at: Optional[datetime] = Field(
        default=None, description="Scheduled execution time"
    )
    started_at: Optional[datetime] = Field(
        default=None, description="Execution start time"
    )
    completed_at: Optional[datetime] = Field(
        default=None, description="Execution completion time"
    )

    retry_count: int = Field(default=0, description="Current retry count")
    worker_id: Optional[str] = Field(default=None, description="Assigned worker ID")
    queue_name: Optional[str] = Field(default=None, description="Queue name")

    result: Optional[TaskResult] = Field(default=None, description="Task result")
    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Additional metadata"
    )

    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
        }

    @validator("scheduled_at", pre=True)
    def parse_scheduled_at(cls, v):
        if isinstance(v, str):
            return datetime.fromisoformat(v)
        return v

    def is_ready(self) -> bool:
        """Check if task is ready for execution."""
        if self.status != TaskStatus.QUEUED:
            return False

        if self.scheduled_at and self.scheduled_at > datetime.now(timezone.utc):
            return False

        return True

    def can_retry(self) -> bool:
        """Check if task can be retried."""
        return (
            self.status in [TaskStatus.FAILURE, TaskStatus.TIMEOUT]
            and self.retry_count < self.config.max_retries
        )

    def mark_running(self, worker_id: str) -> None:
        """Mark task as running."""
        self.status = TaskStatus.RUNNING
        self.worker_id = worker_id
        self.started_at = datetime.now(timezone.utc)

    def mark_completed(self, result: TaskResult) -> None:
        """Mark task as completed with result."""
        self.status = result.status
        self.result = result
        self.completed_at = datetime.now(timezone.utc)

    def mark_failed(self, error: str, traceback: Optional[str] = None) -> None:
        """Mark task as failed."""
        self.status = TaskStatus.FAILURE
        self.result = TaskResult(
            status=TaskStatus.FAILURE,
            error=error,
            traceback=traceback,
            completed_at=datetime.now(timezone.utc),
        )
        self.completed_at = datetime.now(timezone.utc)

    def mark_retry(self) -> None:
        """Mark task for retry."""
        if self.can_retry():
            self.status = TaskStatus.RETRY
            self.retry_count += 1
            self.started_at = None
            self.worker_id = None

    def reset_for_retry(self) -> None:
        """Reset task state for retry execution."""
        self.status = TaskStatus.QUEUED
        self.started_at = None
        self.worker_id = None
        self.result = None

    def get_execution_time(self) -> Optional[float]:
        """Get task execution time in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def get_wait_time(self) -> Optional[float]:
        """Get task wait time in queue (seconds)."""
        if self.started_at:
            return (self.started_at - self.created_at).total_seconds()
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert task to dictionary."""
        return self.dict()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Task":
        """Create task from dictionary."""
        if "result" in data and data["result"]:
            data["result"] = TaskResult.from_dict(data["result"])
        return cls(**data)

    def to_json(self) -> str:
        """Convert task to JSON string."""
        return self.json()

    @classmethod
    def from_json(cls, json_str: str) -> "Task":
        """Create task from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)


class BatchTask(BaseModel):
    """Batch task for processing multiple items."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(description="Batch task name")
    items: List[Dict[str, Any]] = Field(description="Items to process")
    config: TaskConfig = Field(default_factory=TaskConfig)

    status: TaskStatus = Field(default=TaskStatus.PENDING)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    total_items: int = Field(default=0)
    completed_items: int = Field(default=0)
    failed_items: int = Field(default=0)

    subtasks: List[str] = Field(default_factory=list, description="Subtask IDs")

    def __init__(self, **data):
        super().__init__(**data)
        if not self.total_items:
            self.total_items = len(self.items)

    @property
    def progress_percentage(self) -> float:
        """Get completion percentage."""
        if self.total_items == 0:
            return 100.0
        return (self.completed_items / self.total_items) * 100

    def is_completed(self) -> bool:
        """Check if batch is completed."""
        return self.completed_items + self.failed_items >= self.total_items

    def update_progress(self, completed: int, failed: int) -> None:
        """Update batch progress."""
        self.completed_items = completed
        self.failed_items = failed

        if self.is_completed():
            self.status = (
                TaskStatus.SUCCESS if self.failed_items == 0 else TaskStatus.FAILURE
            )


def create_task(
    name: str,
    args: Optional[List[Any]] = None,
    kwargs: Optional[Dict[str, Any]] = None,
    config: Optional[TaskConfig] = None,
    scheduled_at: Optional[datetime] = None,
    **metadata,
) -> Task:
    """Helper function to create a new task."""
    return Task(
        name=name,
        args=args or [],
        kwargs=kwargs or {},
        config=config or TaskConfig(),
        scheduled_at=scheduled_at,
        metadata=metadata,
    )
