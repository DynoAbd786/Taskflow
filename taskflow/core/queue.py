"""
Task queue management with Redis backend.
Handles task queuing, dequeuing, and queue operations.
"""

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import redis.asyncio as redis
from redis.asyncio import Redis

from taskflow.core.task import Task, TaskPriority, TaskStatus
from taskflow.utils.config import get_config


class QueueStats:
    """Queue statistics."""

    def __init__(self):
        self.pending_count: int = 0
        self.running_count: int = 0
        self.completed_count: int = 0
        self.failed_count: int = 0
        self.total_processed: int = 0
        self.avg_execution_time: float = 0.0
        self.avg_wait_time: float = 0.0


class TaskQueue:
    """Redis-based task queue implementation."""

    def __init__(self, name: str = "default", redis_client: Optional[Redis] = None):
        self.name = name
        self.redis = redis_client
        self.config = get_config()

        self._queue_key = f"taskflow:queue:{name}"
        self._processing_key = f"taskflow:processing:{name}"
        self._task_data_key = f"taskflow:tasks"
        self._stats_key = f"taskflow:stats:{name}"
        self._delayed_key = f"taskflow:delayed:{name}"

        self._priority_queues = {
            TaskPriority.CRITICAL: f"{self._queue_key}:critical",
            TaskPriority.HIGH: f"{self._queue_key}:high",
            TaskPriority.NORMAL: f"{self._queue_key}:normal",
            TaskPriority.LOW: f"{self._queue_key}:low",
        }

    async def connect(self) -> None:
        """Connect to Redis."""
        if not self.redis:
            self.redis = redis.from_url(
                self.config.redis.url,
                max_connections=self.config.redis.max_connections,
                decode_responses=True,
                retry_on_timeout=True,
                retry_on_error=[redis.ConnectionError, redis.TimeoutError],
                health_check_interval=30,
                socket_keepalive=True,
                socket_keepalive_options={},
            )

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self.redis:
            try:
                await self.redis.close()
            except Exception:
                pass  # Ignore errors during cleanup
            finally:
                self.redis = None

    @asynccontextmanager
    async def get_connection(self):
        """Get Redis connection context manager with robust error handling."""
        max_retries = 3
        
        for retry_count in range(max_retries):
            try:
                # Ensure connection exists
                if not self.redis:
                    await self.connect()

                connection = self.redis

                # Test the connection before yielding
                if connection:
                    await connection.ping()
                    yield connection
                    return  # Success, exit retry loop
                else:
                    raise redis.ConnectionError("Redis connection is None")

            except (redis.RedisError, ConnectionError, AttributeError, OSError) as e:
                if retry_count >= max_retries - 1:
                    raise e

                # Clean up and retry
                try:
                    await self.disconnect()
                except Exception:
                    pass  # Ignore cleanup errors
                    
                await asyncio.sleep(0.1 * (retry_count + 1))  # Exponential backoff

    async def enqueue(self, task: Task, delay: Optional[int] = None) -> str:
        """Enqueue a task."""
        async with self.get_connection() as redis:
            task.status = TaskStatus.QUEUED
            task.queue_name = self.name

            task_data = task.json()

            async with redis.pipeline() as pipe:
                await pipe.hset(self._task_data_key, task.id, task_data)

                if delay and delay > 0:
                    scheduled_time = datetime.now(timezone.utc).timestamp() + delay
                    await pipe.zadd(self._delayed_key, {task.id: scheduled_time})
                else:
                    priority_queue = self._priority_queues[task.config.priority]
                    await pipe.rpush(priority_queue, task.id)

                await pipe.hincrby(self._stats_key, "pending_count", 1)
                await pipe.execute()

            return task.id

    async def dequeue(self, timeout: int = 30) -> Optional[Task]:
        """Dequeue a task with priority handling."""
        async with self.get_connection() as redis:
            await self._process_delayed_tasks()

            for priority in [
                TaskPriority.CRITICAL,
                TaskPriority.HIGH,
                TaskPriority.NORMAL,
                TaskPriority.LOW,
            ]:
                queue_key = self._priority_queues[priority]

                # Use BRPOPLPUSH for Redis 5.0 compatibility instead of BLMOVE
                result = await redis.brpoplpush(
                    queue_key, self._processing_key, timeout
                )

                if result:
                    task_data = await redis.hget(self._task_data_key, result)
                    if task_data:
                        task = Task.parse_raw(task_data)
                        task.status = TaskStatus.RUNNING

                        await redis.hset(self._task_data_key, task.id, task.json())
                        await redis.hincrby(self._stats_key, "pending_count", -1)
                        await redis.hincrby(self._stats_key, "running_count", 1)

                        return task
                    else:
                        await redis.lrem(self._processing_key, 1, result)

            return None

    async def complete_task(self, task_id: str, task: Task) -> None:
        """Mark task as completed and remove from processing."""
        async with self.get_connection() as redis:
            async with redis.pipeline() as pipe:
                await pipe.lrem(self._processing_key, 1, task_id)
                await pipe.hset(self._task_data_key, task_id, task.json())
                await pipe.hincrby(self._stats_key, "running_count", -1)

                if task.status == TaskStatus.SUCCESS:
                    await pipe.hincrby(self._stats_key, "completed_count", 1)
                else:
                    await pipe.hincrby(self._stats_key, "failed_count", 1)

                await pipe.hincrby(self._stats_key, "total_processed", 1)
                await pipe.execute()

    async def requeue_task(self, task: Task) -> None:
        """Requeue a task for retry."""
        async with self.get_connection() as redis:
            task.reset_for_retry()

            async with redis.pipeline() as pipe:
                await pipe.lrem(self._processing_key, 1, task.id)
                await pipe.hset(self._task_data_key, task.id, task.json())

                priority_queue = self._priority_queues[task.config.priority]
                await pipe.rpush(priority_queue, task.id)

                await pipe.hincrby(self._stats_key, "running_count", -1)
                await pipe.hincrby(self._stats_key, "pending_count", 1)
                await pipe.execute()

    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a pending task."""
        async with self.get_connection() as redis:
            task_data = await redis.hget(self._task_data_key, task_id)
            if not task_data:
                return False

            task = Task.parse_raw(task_data)
            if task.status not in [TaskStatus.PENDING, TaskStatus.QUEUED]:
                return False

            task.status = TaskStatus.CANCELLED
            task.completed_at = datetime.now(timezone.utc)

            async with redis.pipeline() as pipe:
                await pipe.hset(self._task_data_key, task_id, task.json())

                for queue_key in self._priority_queues.values():
                    await pipe.lrem(queue_key, 1, task_id)

                await pipe.zrem(self._delayed_key, task_id)
                await pipe.hincrby(self._stats_key, "pending_count", -1)
                await pipe.execute()

            return True

    async def get_task(self, task_id: str) -> Optional[Task]:
        """Get task by ID."""
        async with self.get_connection() as redis:
            task_data = await redis.hget(self._task_data_key, task_id)
            if task_data:
                return Task.parse_raw(task_data)
            return None

    async def get_queue_size(self) -> Dict[str, int]:
        """Get queue sizes by priority."""
        async with self.get_connection() as redis:
            sizes = {}
            for priority, queue_key in self._priority_queues.items():
                size = await redis.llen(queue_key)
                sizes[priority.value] = size

            sizes["processing"] = await redis.llen(self._processing_key)
            sizes["delayed"] = await redis.zcard(self._delayed_key)

            return sizes

    async def get_stats(self) -> QueueStats:
        """Get queue statistics."""
        async with self.get_connection() as redis:
            stats_data = await redis.hgetall(self._stats_key)

            stats = QueueStats()
            if stats_data:
                stats.pending_count = int(stats_data.get("pending_count", 0))
                stats.running_count = int(stats_data.get("running_count", 0))
                stats.completed_count = int(stats_data.get("completed_count", 0))
                stats.failed_count = int(stats_data.get("failed_count", 0))
                stats.total_processed = int(stats_data.get("total_processed", 0))
                stats.avg_execution_time = float(
                    stats_data.get("avg_execution_time", 0.0)
                )
                stats.avg_wait_time = float(stats_data.get("avg_wait_time", 0.0))

            return stats

    async def purge_queue(self) -> int:
        """Purge all tasks from queue."""
        async with self.get_connection() as redis:
            count = 0

            for queue_key in self._priority_queues.values():
                queue_size = await redis.llen(queue_key)
                if queue_size > 0:
                    await redis.delete(queue_key)
                    count += queue_size

            await redis.delete(self._processing_key)
            await redis.delete(self._delayed_key)
            await redis.delete(self._stats_key)

            return count

    async def list_tasks(
        self, status: Optional[TaskStatus] = None, limit: int = 100, offset: int = 0
    ) -> List[Task]:
        """List tasks in queue."""
        async with self.get_connection() as redis:
            task_ids = []

            if status == TaskStatus.PENDING or status is None:
                for queue_key in self._priority_queues.values():
                    ids = await redis.lrange(queue_key, 0, -1)
                    task_ids.extend(ids)

            if status == TaskStatus.RUNNING or status is None:
                ids = await redis.lrange(self._processing_key, 0, -1)
                task_ids.extend(ids)

            # If no specific status or getting all tasks, get all task data and filter by status
            if status is None or status in [TaskStatus.SUCCESS, TaskStatus.FAILURE, TaskStatus.CANCELLED]:
                all_task_ids = await redis.hkeys(self._task_data_key)
                task_ids.extend(all_task_ids)

            # Remove duplicates and get task data
            unique_task_ids = list(set(task_ids))
            tasks = []
            
            for task_id in unique_task_ids:
                task_data = await redis.hget(self._task_data_key, task_id)
                if task_data:
                    try:
                        task = Task.parse_raw(task_data)
                        if status is None or task.status == status:
                            tasks.append(task)
                    except Exception:
                        continue  # Skip invalid task data

            # Sort tasks by created_at timestamp (most recent first) and apply pagination
            tasks.sort(key=lambda t: t.created_at, reverse=True)
            return tasks[offset : offset + limit]

    async def _process_delayed_tasks(self) -> None:
        """Process delayed tasks that are ready to run."""
        async with self.get_connection() as redis:
            current_time = datetime.now(timezone.utc).timestamp()

            ready_tasks = await redis.zrangebyscore(
                self._delayed_key, 0, current_time, withscores=True
            )

            if ready_tasks:
                for task_id, _ in ready_tasks:
                    task_data = await redis.hget(self._task_data_key, task_id)
                    if task_data:
                        task = Task.parse_raw(task_data)
                        priority_queue = self._priority_queues[task.config.priority]

                        async with redis.pipeline() as pipe:
                            await pipe.zrem(self._delayed_key, task_id)
                            await pipe.rpush(priority_queue, task_id)
                            await pipe.execute()

    async def health_check(self) -> Dict[str, Any]:
        """Perform queue health check."""
        try:
            async with self.get_connection() as redis:
                await redis.ping()

                stats = await self.get_stats()
                sizes = await self.get_queue_size()

                return {
                    "status": "healthy",
                    "queue_name": self.name,
                    "redis_connected": True,
                    "stats": {
                        "pending": stats.pending_count,
                        "running": stats.running_count,
                        "completed": stats.completed_count,
                        "failed": stats.failed_count,
                    },
                    "queue_sizes": sizes,
                }
        except Exception as e:
            return {
                "status": "unhealthy",
                "queue_name": self.name,
                "redis_connected": False,
                "error": str(e),
            }


class QueueManager:
    """Manages multiple task queues."""

    def __init__(self, redis_client: Optional[Redis] = None):
        self.redis = redis_client
        self.queues: Dict[str, TaskQueue] = {}
        self.config = get_config()

    async def connect(self) -> None:
        """Connect to Redis."""
        if not self.redis:
            self.redis = redis.from_url(
                self.config.redis.url,
                max_connections=self.config.redis.max_connections,
                decode_responses=True,
                retry_on_timeout=True,
                retry_on_error=[redis.ConnectionError, redis.TimeoutError],
                health_check_interval=30,
                socket_keepalive=True,
                socket_keepalive_options={},
            )

    def get_queue(self, name: str) -> TaskQueue:
        """Get or create a queue."""
        if name not in self.queues:
            self.queues[name] = TaskQueue(name, self.redis)
        return self.queues[name]

    async def list_queues(self) -> List[str]:
        """List all queue names."""
        if not self.redis:
            await self.connect()

        queue_names = set()

        # Method 1: Check for queue priority keys
        pattern = "taskflow:queue:*"
        keys = await self.redis.keys(pattern)

        for key in keys:
            key_str = key.decode() if isinstance(key, bytes) else key
            # Extract queue name from keys like "taskflow:queue:default:normal"
            if (
                key_str.endswith(":critical")
                or key_str.endswith(":high") 
                or key_str.endswith(":normal")
                or key_str.endswith(":low")
            ):
                # Remove the prefix and suffix to get queue name
                queue_part = key_str.replace("taskflow:queue:", "")
                queue_name = queue_part.rsplit(":", 1)[0]  # Remove the priority suffix
                queue_names.add(queue_name)

        # Method 2: Check for stats keys (as backup when priority keys don't exist)
        stats_pattern = "taskflow:stats:*"
        stats_keys = await self.redis.keys(stats_pattern)

        for key in stats_keys:
            key_str = key.decode() if isinstance(key, bytes) else key
            queue_name = key_str.replace("taskflow:stats:", "")
            queue_names.add(queue_name)

        return list(queue_names)

    async def get_all_stats(self) -> Dict[str, QueueStats]:
        """Get statistics for all queues."""
        queue_names = await self.list_queues()
        stats = {}

        for name in queue_names:
            queue = self.get_queue(name)
            stats[name] = await queue.get_stats()

        return stats
