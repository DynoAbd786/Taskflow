"""
Repository classes for database operations in TaskFlow.
Provides data access layer for tasks, queues, workers, and metrics.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, delete, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from taskflow.core.task import TaskPriority, TaskStatus
from taskflow.db.database import Repository
from taskflow.db.models import (
    ConfigModel,
    MetricModel,
    QueueModel,
    QueueStatsModel,
    ScheduledTaskModel,
    SystemEventModel,
    TaskDependencyModel,
    TaskHistoryModel,
    TaskModel,
    WorkerModel,
)


class TaskRepository(Repository):
    """Repository for task operations."""

    async def create_task(self, task_data: Dict[str, Any]) -> TaskModel:
        """Create a new task."""
        task = TaskModel(**task_data)
        self.session.add(task)
        await self.flush()
        return task

    async def get_task(self, task_id: str) -> Optional[TaskModel]:
        """Get task by ID."""
        result = await self.session.execute(
            select(TaskModel).where(TaskModel.id == task_id)
        )
        return result.scalar_one_or_none()

    async def update_task(self, task_id: str, updates: Dict[str, Any]) -> bool:
        """Update task fields."""
        result = await self.session.execute(
            update(TaskModel).where(TaskModel.id == task_id).values(**updates)
        )
        return result.rowcount > 0

    async def delete_task(self, task_id: str) -> bool:
        """Delete a task."""
        result = await self.session.execute(
            delete(TaskModel).where(TaskModel.id == task_id)
        )
        return result.rowcount > 0

    async def list_tasks(
        self,
        queue_name: Optional[str] = None,
        status: Optional[TaskStatus] = None,
        worker_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "created_at",
        order_desc: bool = True,
    ) -> List[TaskModel]:
        """List tasks with filtering and pagination."""
        query = select(TaskModel)

        conditions = []
        if queue_name:
            conditions.append(TaskModel.queue_name == queue_name)
        if status:
            conditions.append(TaskModel.status == status)
        if worker_id:
            conditions.append(TaskModel.worker_id == worker_id)

        if conditions:
            query = query.where(and_(*conditions))

        if hasattr(TaskModel, order_by):
            order_column = getattr(TaskModel, order_by)
            if order_desc:
                order_column = order_column.desc()
            query = query.order_by(order_column)

        query = query.limit(limit).offset(offset)

        result = await self.session.execute(query)
        return result.scalars().all()

    async def count_tasks(
        self,
        queue_name: Optional[str] = None,
        status: Optional[TaskStatus] = None,
        worker_id: Optional[str] = None,
    ) -> int:
        """Count tasks with filtering."""
        query = select(func.count(TaskModel.id))

        conditions = []
        if queue_name:
            conditions.append(TaskModel.queue_name == queue_name)
        if status:
            conditions.append(TaskModel.status == status)
        if worker_id:
            conditions.append(TaskModel.worker_id == worker_id)

        if conditions:
            query = query.where(and_(*conditions))

        result = await self.session.execute(query)
        return result.scalar()

    async def get_tasks_by_status(
        self, status: TaskStatus, limit: int = 100
    ) -> List[TaskModel]:
        """Get tasks by status."""
        result = await self.session.execute(
            select(TaskModel)
            .where(TaskModel.status == status)
            .order_by(TaskModel.created_at.desc())
            .limit(limit)
        )
        return result.scalars().all()

    async def get_pending_tasks(
        self, queue_name: str, limit: int = 100
    ) -> List[TaskModel]:
        """Get pending tasks for a queue ordered by priority and creation time."""
        result = await self.session.execute(
            select(TaskModel)
            .where(
                and_(
                    TaskModel.queue_name == queue_name,
                    TaskModel.status == TaskStatus.QUEUED,
                    or_(
                        TaskModel.scheduled_at.is_(None),
                        TaskModel.scheduled_at <= datetime.now(timezone.utc),
                    ),
                )
            )
            .order_by(TaskModel.priority.desc(), TaskModel.created_at.asc())
            .limit(limit)
        )
        return result.scalars().all()

    async def get_running_tasks(
        self, worker_id: Optional[str] = None
    ) -> List[TaskModel]:
        """Get currently running tasks."""
        query = select(TaskModel).where(TaskModel.status == TaskStatus.RUNNING)

        if worker_id:
            query = query.where(TaskModel.worker_id == worker_id)

        result = await self.session.execute(query)
        return result.scalars().all()

    async def get_failed_tasks(
        self, queue_name: Optional[str] = None
    ) -> List[TaskModel]:
        """Get failed tasks that can be retried."""
        query = select(TaskModel).where(
            and_(
                TaskModel.status == TaskStatus.FAILURE,
                TaskModel.retry_count < TaskModel.max_retries,
            )
        )

        if queue_name:
            query = query.where(TaskModel.queue_name == queue_name)

        result = await self.session.execute(query)
        return result.scalars().all()

    async def cleanup_old_tasks(self, days: int = 7) -> int:
        """Clean up completed tasks older than specified days."""
        cutoff_date = datetime.now(timezone.utc) - timezone.timedelta(days=days)

        result = await self.session.execute(
            delete(TaskModel).where(
                and_(
                    TaskModel.status.in_([TaskStatus.SUCCESS, TaskStatus.FAILURE]),
                    TaskModel.completed_at < cutoff_date,
                )
            )
        )
        return result.rowcount


class QueueRepository(Repository):
    """Repository for queue operations."""

    async def create_queue(self, queue_data: Dict[str, Any]) -> QueueModel:
        """Create a new queue."""
        queue = QueueModel(**queue_data)
        self.session.add(queue)
        await self.flush()
        return queue

    async def get_queue(self, name: str) -> Optional[QueueModel]:
        """Get queue by name."""
        result = await self.session.execute(
            select(QueueModel).where(QueueModel.name == name)
        )
        return result.scalar_one_or_none()

    async def list_queues(self, active_only: bool = True) -> List[QueueModel]:
        """List all queues."""
        query = select(QueueModel)

        if active_only:
            query = query.where(QueueModel.is_active == True)

        result = await self.session.execute(query)
        return result.scalars().all()

    async def update_queue(self, name: str, updates: Dict[str, Any]) -> bool:
        """Update queue settings."""
        result = await self.session.execute(
            update(QueueModel).where(QueueModel.name == name).values(**updates)
        )
        return result.rowcount > 0

    async def delete_queue(self, name: str) -> bool:
        """Delete a queue."""
        result = await self.session.execute(
            delete(QueueModel).where(QueueModel.name == name)
        )
        return result.rowcount > 0


class WorkerRepository(Repository):
    """Repository for worker operations."""

    async def register_worker(self, worker_data: Dict[str, Any]) -> WorkerModel:
        """Register a new worker."""
        worker = WorkerModel(**worker_data)
        self.session.add(worker)
        await self.flush()
        return worker

    async def get_worker(self, worker_id: str) -> Optional[WorkerModel]:
        """Get worker by ID."""
        result = await self.session.execute(
            select(WorkerModel).where(WorkerModel.id == worker_id)
        )
        return result.scalar_one_or_none()

    async def update_worker(self, worker_id: str, updates: Dict[str, Any]) -> bool:
        """Update worker information."""
        result = await self.session.execute(
            update(WorkerModel).where(WorkerModel.id == worker_id).values(**updates)
        )
        return result.rowcount > 0

    async def list_workers(self, status: Optional[str] = None) -> List[WorkerModel]:
        """List workers."""
        query = select(WorkerModel)

        if status:
            query = query.where(WorkerModel.status == status)

        result = await self.session.execute(query)
        return result.scalars().all()

    async def delete_worker(self, worker_id: str) -> bool:
        """Remove a worker."""
        result = await self.session.execute(
            delete(WorkerModel).where(WorkerModel.id == worker_id)
        )
        return result.rowcount > 0

    async def cleanup_inactive_workers(self, heartbeat_timeout: int = 300) -> int:
        """Clean up workers with stale heartbeats."""
        cutoff_time = datetime.now(timezone.utc) - timezone.timedelta(
            seconds=heartbeat_timeout
        )

        result = await self.session.execute(
            delete(WorkerModel).where(WorkerModel.last_heartbeat < cutoff_time)
        )
        return result.rowcount


class TaskHistoryRepository(Repository):
    """Repository for task history operations."""

    async def add_history(self, history_data: Dict[str, Any]) -> TaskHistoryModel:
        """Add task history entry."""
        history = TaskHistoryModel(**history_data)
        self.session.add(history)
        await self.flush()
        return history

    async def get_task_history(self, task_id: str) -> List[TaskHistoryModel]:
        """Get history for a specific task."""
        result = await self.session.execute(
            select(TaskHistoryModel)
            .where(TaskHistoryModel.task_id == task_id)
            .order_by(TaskHistoryModel.timestamp.asc())
        )
        return result.scalars().all()

    async def cleanup_old_history(self, days: int = 30) -> int:
        """Clean up old task history entries."""
        cutoff_date = datetime.now(timezone.utc) - timezone.timedelta(days=days)

        result = await self.session.execute(
            delete(TaskHistoryModel).where(TaskHistoryModel.timestamp < cutoff_date)
        )
        return result.rowcount


class MetricsRepository(Repository):
    """Repository for metrics operations."""

    async def record_metric(self, metric_data: Dict[str, Any]) -> MetricModel:
        """Record a metric value."""
        metric = MetricModel(**metric_data)
        self.session.add(metric)
        await self.flush()
        return metric

    async def get_metrics(
        self,
        metric_name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        labels: Optional[Dict[str, str]] = None,
        limit: int = 1000,
    ) -> List[MetricModel]:
        """Get metrics with filtering."""
        query = select(MetricModel).where(MetricModel.metric_name == metric_name)

        if start_time:
            query = query.where(MetricModel.timestamp >= start_time)

        if end_time:
            query = query.where(MetricModel.timestamp <= end_time)

        if labels:
            for key, value in labels.items():
                query = query.where(MetricModel.labels[key].astext == value)

        query = query.order_by(MetricModel.timestamp.desc()).limit(limit)

        result = await self.session.execute(query)
        return result.scalars().all()

    async def cleanup_old_metrics(self, days: int = 7) -> int:
        """Clean up old metric data."""
        cutoff_date = datetime.now(timezone.utc) - timezone.timedelta(days=days)

        result = await self.session.execute(
            delete(MetricModel).where(MetricModel.timestamp < cutoff_date)
        )
        return result.rowcount


class SystemEventRepository(Repository):
    """Repository for system event operations."""

    async def create_event(self, event_data: Dict[str, Any]) -> SystemEventModel:
        """Create a system event."""
        event = SystemEventModel(**event_data)
        self.session.add(event)
        await self.flush()
        return event

    async def get_events(
        self,
        event_type: Optional[str] = None,
        severity: Optional[str] = None,
        acknowledged: Optional[bool] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[SystemEventModel]:
        """Get system events with filtering."""
        query = select(SystemEventModel)

        conditions = []
        if event_type:
            conditions.append(SystemEventModel.event_type == event_type)
        if severity:
            conditions.append(SystemEventModel.severity == severity)
        if acknowledged is not None:
            conditions.append(SystemEventModel.acknowledged == acknowledged)

        if conditions:
            query = query.where(and_(*conditions))

        query = (
            query.order_by(SystemEventModel.timestamp.desc())
            .limit(limit)
            .offset(offset)
        )

        result = await self.session.execute(query)
        return result.scalars().all()

    async def acknowledge_event(self, event_id: int, acknowledged_by: str) -> bool:
        """Acknowledge a system event."""
        result = await self.session.execute(
            update(SystemEventModel)
            .where(SystemEventModel.id == event_id)
            .values(
                acknowledged=True,
                acknowledged_by=acknowledged_by,
                acknowledged_at=datetime.now(timezone.utc),
            )
        )
        return result.rowcount > 0
