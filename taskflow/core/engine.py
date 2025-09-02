"""
Core task execution engine.
Handles task registration, execution, and lifecycle management.
"""

import asyncio
import inspect
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Set, Union

from taskflow.core.queue import QueueManager, TaskQueue
from taskflow.core.task import Task, TaskConfig, TaskResult, TaskStatus, create_task
from taskflow.utils.config import get_config


class TaskRegistry:
    """Registry for task functions."""

    def __init__(self):
        self._tasks: Dict[str, Callable] = {}
        self._task_configs: Dict[str, TaskConfig] = {}

    def register(
        self, name: Optional[str] = None, config: Optional[TaskConfig] = None
    ) -> Callable:
        """Register a task function."""

        def decorator(func: Callable) -> Callable:
            task_name = name or f"{func.__module__}.{func.__name__}"
            self._tasks[task_name] = func
            self._task_configs[task_name] = config or TaskConfig()

            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            wrapper.task_name = task_name
            wrapper.delay = lambda *args, **kwargs: self._create_delayed_task(
                task_name, args, kwargs
            )
            wrapper.apply_async = lambda *args, **kwargs: self._create_async_task(
                task_name, args, kwargs
            )

            return wrapper

        return decorator

    def get_task(self, name: str) -> Optional[Callable]:
        """Get registered task function."""
        return self._tasks.get(name)

    def get_config(self, name: str) -> Optional[TaskConfig]:
        """Get task configuration."""
        return self._task_configs.get(name)

    def list_tasks(self) -> List[str]:
        """List all registered task names."""
        return list(self._tasks.keys())

    def _create_delayed_task(self, name: str, args: tuple, kwargs: dict) -> Task:
        """Create a task for delayed execution."""
        config = self._task_configs.get(name, TaskConfig())
        return create_task(name, list(args), kwargs, config)

    def _create_async_task(self, name: str, args: tuple, kwargs: dict) -> Task:
        """Create a task for async execution."""
        config = self._task_configs.get(name, TaskConfig())
        return create_task(name, list(args), kwargs, config)


class TaskExecutor:
    """Executes tasks with proper error handling and timeouts."""

    def __init__(self, registry: TaskRegistry, thread_pool_size: int = 10):
        self.registry = registry
        self.thread_pool = ThreadPoolExecutor(max_workers=thread_pool_size)
        self.config = get_config()

    async def execute_task(self, task: Task) -> TaskResult:
        """Execute a single task."""
        start_time = datetime.now(timezone.utc)

        try:
            func = self.registry.get_task(task.name)
            if not func:
                raise ValueError(f"Task function '{task.name}' not found")

            if asyncio.iscoroutinefunction(func):
                result = await asyncio.wait_for(
                    func(*task.args, **task.kwargs), timeout=task.config.timeout
                )
            else:
                # Convert sync function to async by wrapping in coroutine
                # This avoids ThreadPoolExecutor I/O issues
                async def sync_wrapper():
                    return func(*task.args, **task.kwargs)
                
                result = await asyncio.wait_for(
                    sync_wrapper(), timeout=task.config.timeout
                )

            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()

            return TaskResult(
                status=TaskStatus.SUCCESS,
                result=result,
                execution_time=execution_time,
                completed_at=datetime.now(timezone.utc),
            )

        except asyncio.TimeoutError:
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            return TaskResult(
                status=TaskStatus.TIMEOUT,
                error="Task execution timed out",
                execution_time=execution_time,
                completed_at=datetime.now(timezone.utc),
            )

        except Exception as e:
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            return TaskResult(
                status=TaskStatus.FAILURE,
                error=str(e),
                traceback=traceback.format_exc(),
                execution_time=execution_time,
                completed_at=datetime.now(timezone.utc),
            )

    def shutdown(self):
        """Shutdown the executor."""
        self.thread_pool.shutdown(wait=True)


class TaskEngine:
    """Main task engine for TaskFlow system."""

    def __init__(self, queue_manager: Optional[QueueManager] = None):
        self.registry = TaskRegistry()
        self.queue_manager = queue_manager or QueueManager()
        self.executor = TaskExecutor(self.registry)
        self.config = get_config()

        self._running_tasks: Set[str] = set()
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """Start the task engine."""
        await self.queue_manager.connect()

    async def shutdown(self) -> None:
        """Shutdown the task engine."""
        self._shutdown_event.set()
        self.executor.shutdown()

    def task(
        self, name: Optional[str] = None, queue: str = "default", **config_kwargs
    ) -> Callable:
        """Decorator to register a task function."""
        config = TaskConfig(queue=queue, **config_kwargs)
        return self.registry.register(name, config)

    async def submit_task(
        self,
        name: str,
        args: Optional[List[Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        queue: str = "default",
        delay: Optional[int] = None,
        **task_kwargs,
    ) -> str:
        """Submit a task for execution."""
        config = self.registry.get_config(name) or TaskConfig()
        config.queue = queue

        task = create_task(name, args, kwargs, config, **task_kwargs)

        task_queue = self.queue_manager.get_queue(queue)
        return await task_queue.enqueue(task, delay)

    async def submit_task_object(self, task: Task, queue: Optional[str] = None) -> str:
        """Submit a task object for execution."""
        queue_name = queue or task.config.queue
        task_queue = self.queue_manager.get_queue(queue_name)
        return await task_queue.enqueue(task)

    async def get_task(self, task_id: str, queue: str = "default") -> Optional[Task]:
        """Get task by ID."""
        task_queue = self.queue_manager.get_queue(queue)
        return await task_queue.get_task(task_id)

    async def cancel_task(self, task_id: str, queue: str = "default") -> bool:
        """Cancel a pending task."""
        task_queue = self.queue_manager.get_queue(queue)
        return await task_queue.cancel_task(task_id)

    async def retry_task(self, task_id: str, queue: str = "default") -> bool:
        """Retry a failed task."""
        task_queue = self.queue_manager.get_queue(queue)
        task = await task_queue.get_task(task_id)

        if not task or not task.can_retry():
            return False

        task.mark_retry()
        await task_queue.requeue_task(task)
        return True

    async def list_tasks(
        self,
        queue: str = "default",
        status: Optional[TaskStatus] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Task]:
        """List tasks in queue."""
        task_queue = self.queue_manager.get_queue(queue)
        return await task_queue.list_tasks(status, limit, offset)

    async def get_queue_stats(self, queue: str = "default") -> Dict[str, Any]:
        """Get queue statistics."""
        task_queue = self.queue_manager.get_queue(queue)
        stats = await task_queue.get_stats()
        sizes = await task_queue.get_queue_size()

        return {
            "queue_name": queue,
            "pending_count": stats.pending_count,
            "running_count": stats.running_count,
            "completed_count": stats.completed_count,
            "failed_count": stats.failed_count,
            "total_processed": stats.total_processed,
            "avg_execution_time": stats.avg_execution_time,
            "avg_wait_time": stats.avg_wait_time,
            "queue_sizes": sizes,
        }

    async def get_all_queue_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all queues."""
        queue_names = await self.queue_manager.list_queues()
        stats = {}

        for queue_name in queue_names:
            stats[queue_name] = await self.get_queue_stats(queue_name)

        return stats

    async def purge_queue(self, queue: str = "default") -> int:
        """Purge all tasks from a queue."""
        task_queue = self.queue_manager.get_queue(queue)
        return await task_queue.purge_queue()

    async def health_check(self) -> Dict[str, Any]:
        """Perform system health check."""
        try:
            queue_names = await self.queue_manager.list_queues()
            queue_healths = {}

            for queue_name in queue_names:
                task_queue = self.queue_manager.get_queue(queue_name)
                queue_healths[queue_name] = await task_queue.health_check()

            overall_status = "healthy"
            if any(q["status"] == "unhealthy" for q in queue_healths.values()):
                overall_status = "unhealthy"

            return {
                "status": overall_status,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "registered_tasks": len(self.registry.list_tasks()),
                "active_queues": len(queue_names),
                "running_tasks": len(self._running_tasks),
                "queues": queue_healths,
            }

        except Exception as e:
            return {
                "status": "unhealthy",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": str(e),
            }


_global_engine: Optional[TaskEngine] = None


def get_engine() -> TaskEngine:
    """Get the global task engine instance."""
    global _global_engine
    if _global_engine is None:
        _global_engine = TaskEngine()
    return _global_engine


def set_engine(engine: TaskEngine) -> None:
    """Set the global task engine instance."""
    global _global_engine
    _global_engine = engine


def task(name: Optional[str] = None, **kwargs) -> Callable:
    """Decorator to register a task function with the global engine."""
    engine = get_engine()
    return engine.task(name, **kwargs)


async def submit_task(name: str, *args, **kwargs) -> str:
    """Submit a task to the global engine."""
    engine = get_engine()
    return await engine.submit_task(name, list(args), kwargs)
