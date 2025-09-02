"""
API models and schemas for TaskFlow web interface.
Defines request/response models for the REST API.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, validator

from taskflow.core.task import TaskPriority, TaskStatus


class TaskSubmissionRequest(BaseModel):
    """Request model for task submission."""

    name: str = Field(description="Task function name")
    args: List[Any] = Field(default_factory=list, description="Positional arguments")
    kwargs: Dict[str, Any] = Field(
        default_factory=dict, description="Keyword arguments"
    )
    queue: str = Field(default="default", description="Queue name")
    priority: TaskPriority = Field(
        default=TaskPriority.NORMAL, description="Task priority"
    )
    timeout: int = Field(default=300, description="Task timeout in seconds")
    max_retries: int = Field(default=100, description="Maximum retry attempts")
    delay: Optional[int] = Field(default=None, description="Delay execution by seconds")
    scheduled_at: Optional[datetime] = Field(
        default=None, description="Schedule execution time"
    )
    tags: List[str] = Field(default_factory=list, description="Task tags")
    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Additional metadata"
    )


class TaskResponse(BaseModel):
    """Response model for task information."""

    id: str
    name: str
    status: TaskStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    queue_name: Optional[str] = None
    worker_id: Optional[str] = None
    retry_count: int = 0

    args: List[Any] = Field(default_factory=list)
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    result: Optional[Dict[str, Any]] = None
    execution_time: Optional[float] = None
    wait_time: Optional[float] = None


class BatchTaskRequest(BaseModel):
    """Request model for batch task submission."""

    name: str = Field(description="Batch task name")
    items: List[Dict[str, Any]] = Field(description="Items to process")
    queue: str = Field(default="default", description="Queue name")
    priority: TaskPriority = Field(
        default=TaskPriority.NORMAL, description="Task priority"
    )
    timeout: int = Field(default=300, description="Task timeout in seconds")
    max_retries: int = Field(default=100, description="Maximum retry attempts")
    chunk_size: int = Field(default=10, description="Items per subtask")


class QueueStatsResponse(BaseModel):
    """Response model for queue statistics."""

    queue_name: str
    pending_count: int
    running_count: int
    completed_count: int
    failed_count: int
    total_processed: int
    avg_execution_time: float
    avg_wait_time: float
    queue_sizes: Dict[str, int]


class WorkerResponse(BaseModel):
    """Response model for worker information."""

    worker_id: str
    status: str
    queues: List[str]
    started_at: datetime
    last_heartbeat: datetime
    tasks_processed: int
    tasks_succeeded: int
    tasks_failed: int
    current_task: Optional[str] = None
    cpu_percent: float
    memory_percent: float
    memory_mb: float
    uptime_seconds: float


class SystemStatsResponse(BaseModel):
    """Response model for system statistics."""

    total_tasks: int
    pending_tasks: int
    running_tasks: int
    completed_tasks: int
    failed_tasks: int
    active_workers: int
    active_queues: int
    avg_execution_time: float
    system_uptime: float


class HealthCheckResponse(BaseModel):
    """Response model for health check."""

    status: str
    timestamp: datetime
    registered_tasks: int
    active_queues: int
    running_tasks: int
    queues: Dict[str, Dict[str, Any]]


class TaskFilterParams(BaseModel):
    """Query parameters for task filtering."""

    status: Optional[TaskStatus] = None
    queue: Optional[str] = None
    worker_id: Optional[str] = None
    tags: Optional[List[str]] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None
    limit: int = Field(default=50, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)

    @validator("tags", pre=True)
    def parse_tags(cls, v):
        if isinstance(v, str):
            return [tag.strip() for tag in v.split(",") if tag.strip()]
        return v or []


class TaskActionRequest(BaseModel):
    """Request model for task actions."""

    action: str = Field(description="Action to perform (cancel, retry, delete)")
    reason: Optional[str] = Field(default=None, description="Reason for action")


class QueueActionRequest(BaseModel):
    """Request model for queue actions."""

    action: str = Field(description="Action to perform (purge, pause, resume)")
    reason: Optional[str] = Field(default=None, description="Reason for action")


class WorkerActionRequest(BaseModel):
    """Request model for worker actions."""

    action: str = Field(description="Action to perform (shutdown, restart)")
    graceful: bool = Field(default=True, description="Graceful shutdown")
    reason: Optional[str] = Field(default=None, description="Reason for action")


class ErrorResponse(BaseModel):
    """Error response model."""

    error: str
    message: str
    details: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.now)


class PaginatedResponse(BaseModel):
    """Paginated response model."""

    items: List[Any]
    total: int
    limit: int
    offset: int
    has_next: bool
    has_prev: bool


class TaskMetricsResponse(BaseModel):
    """Response model for task metrics."""

    task_id: str
    execution_time: Optional[float]
    wait_time: Optional[float]
    memory_usage: Optional[float]
    cpu_usage: Optional[float]
    error_rate: float
    retry_count: int


class QueueMetricsResponse(BaseModel):
    """Response model for queue metrics."""

    queue_name: str
    throughput: float
    avg_wait_time: float
    avg_execution_time: float
    error_rate: float
    pending_trend: List[int]
    completion_trend: List[int]


class WebSocketMessage(BaseModel):
    """WebSocket message model."""

    type: str = Field(description="Message type")
    data: Dict[str, Any] = Field(description="Message data")
    timestamp: datetime = Field(default_factory=datetime.now)


class TaskEventMessage(WebSocketMessage):
    """Task event WebSocket message."""

    type: str = "task_event"
    task_id: str
    event: str
    status: TaskStatus
    queue_name: str
    worker_id: Optional[str] = None


class SystemEventMessage(WebSocketMessage):
    """System event WebSocket message."""

    type: str = "system_event"
    event: str
    component: str
    severity: str
    message: str


class MetricsMessage(WebSocketMessage):
    """Metrics WebSocket message."""

    type: str = "metrics"
    metrics: Dict[str, Any]
