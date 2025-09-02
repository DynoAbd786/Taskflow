"""
Database models for TaskFlow system.
Defines SQLAlchemy models for persistent task storage and history.
"""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import JSON, Boolean, Column, DateTime
from sqlalchemy import Enum as SQLEnum
from sqlalchemy import Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func

from taskflow.core.task import TaskPriority, TaskStatus

Base = declarative_base()


class TaskModel(Base):
    """Database model for tasks."""

    __tablename__ = "tasks"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False, index=True)
    status = Column(SQLEnum(TaskStatus), nullable=False, index=True)
    priority = Column(
        SQLEnum(TaskPriority), nullable=False, default=TaskPriority.NORMAL
    )

    queue_name = Column(String, nullable=False, index=True)
    worker_id = Column(String, nullable=True, index=True)

    args = Column(JSONB, nullable=False, default=list)
    kwargs = Column(JSONB, nullable=False, default=dict)

    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    scheduled_at = Column(DateTime(timezone=True), nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)

    timeout = Column(Integer, nullable=False, default=300)
    max_retries = Column(Integer, nullable=False, default=100)
    retry_count = Column(Integer, nullable=False, default=0)
    retry_delay = Column(Integer, nullable=False, default=60)

    tags = Column(JSONB, nullable=False, default=list)
    metadata = Column(JSONB, nullable=False, default=dict)

    result_status = Column(SQLEnum(TaskStatus), nullable=True)
    result_data = Column(JSONB, nullable=True)
    result_error = Column(Text, nullable=True)
    result_traceback = Column(Text, nullable=True)
    execution_time = Column(Float, nullable=True)

    __table_args__ = (
        Index("idx_tasks_status_queue", "status", "queue_name"),
        Index("idx_tasks_created_at", "created_at"),
        Index("idx_tasks_worker_status", "worker_id", "status"),
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "status": self.status.value if self.status else None,
            "priority": self.priority.value if self.priority else None,
            "queue_name": self.queue_name,
            "worker_id": self.worker_id,
            "args": self.args or [],
            "kwargs": self.kwargs or {},
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "scheduled_at": self.scheduled_at.isoformat()
            if self.scheduled_at
            else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat()
            if self.completed_at
            else None,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "retry_count": self.retry_count,
            "retry_delay": self.retry_delay,
            "tags": self.tags or [],
            "metadata": self.metadata or {},
            "result": {
                "status": self.result_status.value if self.result_status else None,
                "data": self.result_data,
                "error": self.result_error,
                "traceback": self.result_traceback,
                "execution_time": self.execution_time,
            }
            if self.result_status
            else None,
        }


class QueueModel(Base):
    """Database model for queue metadata."""

    __tablename__ = "queues"

    name = Column(String, primary_key=True)
    description = Column(Text, nullable=True)
    max_concurrency = Column(Integer, nullable=False, default=10)
    default_timeout = Column(Integer, nullable=False, default=300)
    default_max_retries = Column(Integer, nullable=False, default=100)

    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    updated_at = Column(
        DateTime(timezone=True), nullable=False, default=func.now(), onupdate=func.now()
    )

    settings = Column(JSONB, nullable=False, default=dict)


class WorkerModel(Base):
    """Database model for worker registration and stats."""

    __tablename__ = "workers"

    id = Column(String, primary_key=True)
    hostname = Column(String, nullable=False)
    pid = Column(Integer, nullable=False)

    queues = Column(JSONB, nullable=False, default=list)
    concurrency = Column(Integer, nullable=False, default=1)

    status = Column(String, nullable=False, default="active")
    registered_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    last_heartbeat = Column(DateTime(timezone=True), nullable=False, default=func.now())

    tasks_processed = Column(Integer, nullable=False, default=0)
    tasks_succeeded = Column(Integer, nullable=False, default=0)
    tasks_failed = Column(Integer, nullable=False, default=0)

    cpu_percent = Column(Float, nullable=False, default=0.0)
    memory_percent = Column(Float, nullable=False, default=0.0)
    memory_mb = Column(Float, nullable=False, default=0.0)

    avg_task_duration = Column(Float, nullable=False, default=0.0)
    total_execution_time = Column(Float, nullable=False, default=0.0)

    metadata = Column(JSONB, nullable=False, default=dict)

    __table_args__ = (
        Index("idx_workers_status", "status"),
        Index("idx_workers_last_heartbeat", "last_heartbeat"),
    )


class TaskHistoryModel(Base):
    """Database model for task execution history."""

    __tablename__ = "task_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String, nullable=False, index=True)

    event = Column(String, nullable=False)
    old_status = Column(SQLEnum(TaskStatus), nullable=True)
    new_status = Column(SQLEnum(TaskStatus), nullable=True)

    worker_id = Column(String, nullable=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, default=func.now())

    details = Column(JSONB, nullable=True)

    __table_args__ = (
        Index("idx_task_history_task_id", "task_id"),
        Index("idx_task_history_timestamp", "timestamp"),
    )


class QueueStatsModel(Base):
    """Database model for queue statistics snapshots."""

    __tablename__ = "queue_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    queue_name = Column(String, nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, default=func.now())

    pending_count = Column(Integer, nullable=False, default=0)
    running_count = Column(Integer, nullable=False, default=0)
    completed_count = Column(Integer, nullable=False, default=0)
    failed_count = Column(Integer, nullable=False, default=0)

    avg_execution_time = Column(Float, nullable=False, default=0.0)
    avg_wait_time = Column(Float, nullable=False, default=0.0)
    throughput = Column(Float, nullable=False, default=0.0)

    __table_args__ = (
        Index("idx_queue_stats_queue_timestamp", "queue_name", "timestamp"),
    )


class SystemEventModel(Base):
    """Database model for system events and alerts."""

    __tablename__ = "system_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_type = Column(String, nullable=False, index=True)
    component = Column(String, nullable=False)
    severity = Column(String, nullable=False, default="info")

    message = Column(Text, nullable=False)
    details = Column(JSONB, nullable=True)

    timestamp = Column(DateTime(timezone=True), nullable=False, default=func.now())
    acknowledged = Column(Boolean, nullable=False, default=False)
    acknowledged_by = Column(String, nullable=True)
    acknowledged_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("idx_system_events_type_timestamp", "event_type", "timestamp"),
        Index("idx_system_events_severity", "severity"),
    )


class TaskDependencyModel(Base):
    """Database model for task dependencies."""

    __tablename__ = "task_dependencies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String, nullable=False, index=True)
    depends_on_task_id = Column(String, nullable=False, index=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    resolved_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint("task_id", "depends_on_task_id"),
        Index("idx_task_deps_task_id", "task_id"),
    )


class ScheduledTaskModel(Base):
    """Database model for scheduled/recurring tasks."""

    __tablename__ = "scheduled_tasks"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)

    task_name = Column(String, nullable=False)
    args = Column(JSONB, nullable=False, default=list)
    kwargs = Column(JSONB, nullable=False, default=dict)

    cron_expression = Column(String, nullable=True)
    interval_seconds = Column(Integer, nullable=True)

    queue_name = Column(String, nullable=False, default="default")
    priority = Column(
        SQLEnum(TaskPriority), nullable=False, default=TaskPriority.NORMAL
    )
    timeout = Column(Integer, nullable=False, default=300)
    max_retries = Column(Integer, nullable=False, default=100)

    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    updated_at = Column(
        DateTime(timezone=True), nullable=False, default=func.now(), onupdate=func.now()
    )

    last_run_at = Column(DateTime(timezone=True), nullable=True)
    next_run_at = Column(DateTime(timezone=True), nullable=True)

    run_count = Column(Integer, nullable=False, default=0)
    success_count = Column(Integer, nullable=False, default=0)
    failure_count = Column(Integer, nullable=False, default=0)

    metadata = Column(JSONB, nullable=False, default=dict)

    __table_args__ = (
        Index("idx_scheduled_tasks_next_run", "next_run_at"),
        Index("idx_scheduled_tasks_active", "is_active"),
    )


class MetricModel(Base):
    """Database model for system metrics."""

    __tablename__ = "metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    metric_name = Column(String, nullable=False, index=True)
    metric_type = Column(String, nullable=False)  # counter, gauge, histogram

    value = Column(Float, nullable=False)
    labels = Column(JSONB, nullable=False, default=dict)

    timestamp = Column(DateTime(timezone=True), nullable=False, default=func.now())

    __table_args__ = (Index("idx_metrics_name_timestamp", "metric_name", "timestamp"),)


class ConfigModel(Base):
    """Database model for dynamic configuration."""

    __tablename__ = "config"

    key = Column(String, primary_key=True)
    value = Column(JSONB, nullable=False)
    description = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    updated_at = Column(
        DateTime(timezone=True), nullable=False, default=func.now(), onupdate=func.now()
    )
    updated_by = Column(String, nullable=True)

    is_sensitive = Column(Boolean, nullable=False, default=False)
    requires_restart = Column(Boolean, nullable=False, default=False)
