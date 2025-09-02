"""
Distributed worker implementation for TaskFlow.
Handles task consumption, execution, and load balancing.
"""

import asyncio
import json
import os
import signal
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

import psutil

from taskflow.core.engine import TaskEngine, TaskExecutor
from taskflow.core.queue import QueueManager, TaskQueue
from taskflow.core.task import Task, TaskStatus
from taskflow.utils.config import get_config


@dataclass
class WorkerStats:
    """Worker statistics and metrics."""

    worker_id: str
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    tasks_processed: int = 0
    tasks_succeeded: int = 0
    tasks_failed: int = 0
    current_task: Optional[str] = None
    last_heartbeat: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    memory_mb: float = 0.0

    avg_task_duration: float = 0.0
    total_execution_time: float = 0.0

    def update_system_metrics(self) -> None:
        """Update system resource metrics."""
        process = psutil.Process()
        self.cpu_percent = process.cpu_percent()

        memory_info = process.memory_info()
        self.memory_mb = memory_info.rss / 1024 / 1024
        self.memory_percent = process.memory_percent()

    def record_task_completion(self, duration: float, success: bool) -> None:
        """Record task completion metrics."""
        self.tasks_processed += 1
        if success:
            self.tasks_succeeded += 1
        else:
            self.tasks_failed += 1

        self.total_execution_time += duration
        self.avg_task_duration = self.total_execution_time / self.tasks_processed
        self.last_heartbeat = datetime.now(timezone.utc)

    def to_dict(self) -> Dict:
        """Convert stats to dictionary."""
        return {
            "worker_id": self.worker_id,
            "started_at": self.started_at.isoformat(),
            "tasks_processed": self.tasks_processed,
            "tasks_succeeded": self.tasks_succeeded,
            "tasks_failed": self.tasks_failed,
            "current_task": self.current_task,
            "last_heartbeat": self.last_heartbeat.isoformat(),
            "cpu_percent": self.cpu_percent,
            "memory_percent": self.memory_percent,
            "memory_mb": self.memory_mb,
            "avg_task_duration": self.avg_task_duration,
            "uptime_seconds": (
                datetime.now(timezone.utc) - self.started_at
            ).total_seconds(),
        }


class WorkerManager:
    """Manages worker registration and heartbeats."""

    def __init__(self, queue_manager: QueueManager):
        self.queue_manager = queue_manager
        self.config = get_config()
        self._workers_key = "taskflow:workers"
        self._heartbeat_key = "taskflow:heartbeats"

    async def register_worker(self, worker_id: str, queues: List[str]) -> None:
        """Register a worker."""
        queue = self.queue_manager.get_queue("default")
        async with queue.get_connection() as redis:
            async with redis.pipeline() as pipe:
                worker_data = {
                    "worker_id": worker_id,
                    "queues": ",".join(queues),
                    "registered_at": datetime.now(timezone.utc).isoformat(),
                    "status": "active",
                    "pid": os.getpid(),
                    "hostname": os.uname().nodename,
                }

                await pipe.hset(self._workers_key, worker_id, json.dumps(worker_data))
                await pipe.hset(
                    self._heartbeat_key,
                    worker_id,
                    datetime.now(timezone.utc).timestamp(),
                )
                await pipe.execute()

    async def unregister_worker(self, worker_id: str) -> None:
        """Unregister a worker."""
        queue = self.queue_manager.get_queue("default")
        async with queue.get_connection() as redis:
            async with redis.pipeline() as pipe:
                await pipe.hdel(self._workers_key, worker_id)
                await pipe.hdel(self._heartbeat_key, worker_id)
                await pipe.execute()

    async def update_heartbeat(self, worker_id: str, stats: WorkerStats) -> None:
        """Update worker heartbeat and stats."""
        try:
            queue = self.queue_manager.get_queue("default")
            async with queue.get_connection() as redis:
                # Use individual commands instead of pipeline to avoid nested context managers
                await redis.hset(
                    self._heartbeat_key,
                    worker_id,
                    datetime.now(timezone.utc).timestamp(),
                )
                await redis.hset(
                    f"taskflow:worker_stats:{worker_id}", 
                    mapping=stats.to_dict()
                )
        except asyncio.CancelledError:
            raise  # Re-raise cancellation
        except Exception:
            # Silently ignore other errors during heartbeat updates
            # to prevent spam in logs during shutdown
            pass

    async def get_active_workers(self) -> List[Dict]:
        """Get list of active workers."""
        queue = self.queue_manager.get_queue("default")
        async with queue.get_connection() as redis:
            workers = await redis.hgetall(self._workers_key)
            heartbeats = await redis.hgetall(self._heartbeat_key)

            current_time = datetime.now(timezone.utc).timestamp()
            active_workers = []

            for worker_id, worker_data in workers.items():
                last_heartbeat = float(heartbeats.get(worker_id, 0))

                if (
                    current_time - last_heartbeat
                    < self.config.worker.heartbeat_interval * 3
                ):
                    try:
                        worker_info = json.loads(worker_data)
                    except json.JSONDecodeError:
                        # Fallback for old data that was stored with str()
                        worker_info = eval(worker_data)
                        
                    worker_info["last_heartbeat"] = last_heartbeat

                    # Get detailed stats
                    stats_data = await redis.hgetall(f"taskflow:worker_stats:{worker_id}")
                    if stats_data:
                        # Convert stats from string to appropriate types
                        for key, value in stats_data.items():
                            try:
                                # Attempt to convert to float, then int
                                stats_data[key] = float(value)
                                if stats_data[key].is_integer():
                                    stats_data[key] = int(stats_data[key])
                            except (ValueError, TypeError):
                                # Keep as string if conversion fails
                                pass
                        worker_info.update(stats_data)

                    active_workers.append(worker_info)

            return active_workers

    async def cleanup_dead_workers(self) -> List[str]:
        """Clean up workers that haven't sent heartbeats."""
        queue = self.queue_manager.get_queue("default")
        async with queue.get_connection() as redis:
            workers = await redis.hgetall(self._workers_key)
            heartbeats = await redis.hgetall(self._heartbeat_key)

            current_time = datetime.now(timezone.utc).timestamp()
            dead_workers = []

            for worker_id, _ in workers.items():
                last_heartbeat = float(heartbeats.get(worker_id, 0))

                if (
                    current_time - last_heartbeat
                    > self.config.worker.heartbeat_interval * 3
                ):
                    await self.unregister_worker(worker_id)
                    dead_workers.append(worker_id)

            return dead_workers


class TaskWorker:
    """Individual task worker implementation."""

    def __init__(
        self,
        worker_id: str,
        queues: List[str] = None,
        engine: Optional[TaskEngine] = None,
        concurrency: Optional[int] = None,
    ):
        self.worker_id = worker_id
        self.queues = queues or ["default"]
        self.engine = engine or TaskEngine()
        self.concurrency = concurrency or get_config().worker.concurrency
        self.config = get_config()

        self.stats = WorkerStats(worker_id=worker_id)
        self.worker_manager = WorkerManager(self.engine.queue_manager)

        self._running = False
        self._shutdown_event = asyncio.Event()
        self._current_tasks: Set[asyncio.Task] = set()
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._heartbeat_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start the worker."""
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(self.stop()))
        loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(self.stop()))

        await self.engine.start()
        await self.worker_manager.register_worker(self.worker_id, self.queues)

        self._running = True

        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        consumer_tasks = [
            asyncio.create_task(self._consume_tasks(queue_name))
            for queue_name in self.queues
        ]

        try:
            await asyncio.gather(self._heartbeat_task, *consumer_tasks)
        except asyncio.CancelledError:
            pass
        finally:
            await self._cleanup()

    async def stop(self) -> None:
        """Stop the worker gracefully."""
        self._running = False
        self._shutdown_event.set()

        # Cancel heartbeat task and wait for it to finish
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass  # Ignore other exceptions during shutdown

        # Wait for current tasks to complete
        if self._current_tasks:
            await asyncio.gather(*self._current_tasks, return_exceptions=True)

        # Cleanup worker registration
        try:
            await self.worker_manager.unregister_worker(self.worker_id)
        except Exception:
            pass  # Ignore errors during cleanup

        # Shutdown engine
        try:
            await self.engine.shutdown()
        except Exception:
            pass  # Ignore errors during cleanup

    

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats."""
        while self._running and not self._shutdown_event.is_set():
            try:
                self.stats.update_system_metrics()
                
                try:
                    await self.worker_manager.update_heartbeat(self.worker_id, self.stats)
                except Exception as e:
                    print(f"Error updating heartbeat: {e}")
                    # Don't break on heartbeat errors, just continue

                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=0.5
                    )
                    break  # Shutdown requested
                except asyncio.TimeoutError:
                    continue  # Normal timeout, continue heartbeat
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error in heartbeat loop: {e}")
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=0.5
                    )
                    break  # Shutdown requested
                except asyncio.TimeoutError:
                    continue  # Normal timeout, retry heartbeat

    async def _consume_tasks(self, queue_name: str) -> None:
        """Consume tasks from a specific queue."""
        task_queue = self.engine.queue_manager.get_queue(queue_name)

        while self._running and not self._shutdown_event.is_set():
            try:
                async with self._semaphore:
                    task = await task_queue.dequeue(timeout=5)

                    if task:
                        execution_task = asyncio.create_task(
                            self._execute_task(task, task_queue)
                        )
                        self._current_tasks.add(execution_task)
                        execution_task.add_done_callback(self._current_tasks.discard)

            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(1)

    async def _execute_task(self, task: Task, task_queue: TaskQueue) -> None:
        """Execute a single task."""
        start_time = datetime.now(timezone.utc)

        try:
            self.stats.current_task = task.id
            task.mark_running(self.worker_id)
            await self._broadcast_task_event(task, "running")

            # Update the task status to running in the queue
            async with task_queue.get_connection() as redis:
                await redis.hset(task_queue._task_data_key, task.id, task.json())
                # Store task execution log
                await redis.hset(f"taskflow:task_logs:{task.id}", "started", 
                               f"Task {task.name} started by worker {self.worker_id}")

            result = await self.engine.executor.execute_task(task)
            task.mark_completed(result)

            await task_queue.complete_task(task.id, task)
            await self._broadcast_task_event(task, "completed")
            
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            # Log successful completion to file system
            try:
                from task_logs import task_logger
                task_result = result.result if hasattr(result, 'result') else None
                task_logger.log_success(task.id, task.name, task_result, execution_time)
            except Exception:
                pass

            self.stats.record_task_completion(
                execution_time, result.status == TaskStatus.SUCCESS
            )

            if result.status == TaskStatus.FAILURE and task.can_retry():
                task.mark_retry()
                await self._broadcast_task_event(task, "retrying")
                await asyncio.sleep(task.config.retry_delay)
                await task_queue.requeue_task(task)

        except Exception as e:
            task.mark_failed(str(e))
            await task_queue.complete_task(task.id, task)
            await self._broadcast_task_event(task, "failed")
            
            # Log failure to file system
            try:
                from task_logs import task_logger
                execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                task_logger.log_failure(task.id, task.name, str(e), execution_time)
            except Exception:
                pass

            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            self.stats.record_task_completion(execution_time, False)

        finally:
            self.stats.current_task = None

    async def _broadcast_task_event(self, task: Task, event: str) -> None:
        """Broadcast a task event to other system components via Redis Pub/Sub."""
        try:
            queue = self.engine.queue_manager.get_queue("default")
            async with queue.get_connection() as redis:
                message = {
                    "type": "task_event",
                    "task_id": task.id,
                    "event": event,
                    "status": task.status.value,
                    "queue_name": task.queue_name,
                    "worker_id": self.worker_id,
                }
                await redis.publish("task_events", json.dumps(message))
        except Exception as e:
            # Log or handle the error appropriately
            print(f"Error broadcasting task event: {e}")

    async def _cleanup(self) -> None:
        """Cleanup worker resources."""
        for task in self._current_tasks:
            if not task.done():
                task.cancel()

        if self._current_tasks:
            await asyncio.gather(*self._current_tasks, return_exceptions=True)


class WorkerPool:
    """Manages a pool of workers."""

    def __init__(
        self,
        pool_size: int = None,
        queues: List[str] = None,
        engine: Optional[TaskEngine] = None,
    ):
        self.pool_size = pool_size or get_config().worker.concurrency
        self.queues = queues or ["default"]
        self.engine = engine or TaskEngine()
        self.config = get_config()

        self.workers: List[TaskWorker] = []
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """Start the worker pool."""
        await self.engine.start()

        for i in range(self.pool_size):
            worker_id = f"worker-{os.getpid()}-{i}"
            worker = TaskWorker(
                worker_id=worker_id,
                queues=self.queues,
                engine=self.engine,
                concurrency=1,
            )
            self.workers.append(worker)

        worker_tasks = [asyncio.create_task(worker.start()) for worker in self.workers]

        try:
            await asyncio.gather(*worker_tasks)
        except asyncio.CancelledError:
            pass

    async def stop(self) -> None:
        """Stop all workers in the pool."""
        self._shutdown_event.set()

        stop_tasks = [worker.stop() for worker in self.workers]
        await asyncio.gather(*stop_tasks, return_exceptions=True)

    async def scale(self, new_size: int) -> None:
        """Scale the worker pool to a new size."""
        current_size = len(self.workers)

        if new_size > current_size:
            for i in range(current_size, new_size):
                worker_id = f"worker-{os.getpid()}-{i}"
                worker = TaskWorker(
                    worker_id=worker_id,
                    queues=self.queues,
                    engine=self.engine,
                    concurrency=1,
                )
                self.workers.append(worker)
                asyncio.create_task(worker.start())

        elif new_size < current_size:
            workers_to_stop = self.workers[new_size:]
            self.workers = self.workers[:new_size]

            stop_tasks = [worker.stop() for worker in workers_to_stop]
            await asyncio.gather(*stop_tasks, return_exceptions=True)

        self.pool_size = new_size

    def get_stats(self) -> Dict:
        """Get pool statistics."""
        return {
            "pool_size": self.pool_size,
            "active_workers": len(self.workers),
            "queues": self.queues,
            "workers": [worker.stats.to_dict() for worker in self.workers],
        }
