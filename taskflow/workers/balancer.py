"""
Load balancing and worker distribution for TaskFlow.
Handles automatic scaling and task distribution across workers.
"""

import asyncio
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from taskflow.core.queue import QueueManager
from taskflow.utils.config import get_config
from taskflow.workers.worker import WorkerManager, WorkerStats


@dataclass
class LoadBalancingConfig:
    """Load balancing configuration."""

    min_workers: int = 1
    max_workers: int = 10
    scale_up_threshold: float = 0.8
    scale_down_threshold: float = 0.3
    scale_check_interval: int = 60

    cpu_threshold: float = 80.0
    memory_threshold: float = 80.0
    queue_depth_threshold: int = 100

    cool_down_period: int = 300


class LoadBalancer:
    """Automatic load balancing and worker scaling."""

    def __init__(
        self,
        queue_manager: QueueManager,
        config: Optional[LoadBalancingConfig] = None,
    ):
        self.queue_manager = queue_manager
        self.worker_manager = WorkerManager(queue_manager)
        self.config = config or LoadBalancingConfig()
        self.system_config = get_config()

        self._last_scale_action = datetime.now(timezone.utc)
        self._running = False
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """Start the load balancer."""
        self._running = True
        await self._monitoring_loop()

    async def stop(self) -> None:
        """Stop the load balancer."""
        self._running = False
        self._shutdown_event.set()

    async def _monitoring_loop(self) -> None:
        """Main monitoring and scaling loop."""
        while self._running and not self._shutdown_event.is_set():
            try:
                await self._check_and_scale()
                await asyncio.sleep(self.config.scale_check_interval)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(self.config.scale_check_interval)

    async def _check_and_scale(self) -> None:
        """Check system metrics and scale if needed."""
        workers = await self.worker_manager.get_active_workers()
        queue_stats = await self.queue_manager.get_all_stats()

        if not workers:
            return

        metrics = await self._collect_metrics(workers, queue_stats)
        scaling_decision = self._analyze_scaling_need(metrics, len(workers))

        if scaling_decision and self._can_scale():
            await self._execute_scaling(scaling_decision, workers)

    async def _collect_metrics(self, workers: List[Dict], queue_stats: Dict) -> Dict:
        """Collect system metrics for scaling decisions."""
        total_cpu = sum(w.get("cpu_percent", 0) for w in workers)
        total_memory = sum(w.get("memory_percent", 0) for w in workers)

        avg_cpu = total_cpu / len(workers) if workers else 0
        avg_memory = total_memory / len(workers) if workers else 0

        total_pending = sum(stats.pending_count for stats in queue_stats.values())
        total_running = sum(stats.running_count for stats in queue_stats.values())

        pending_per_worker = total_pending / len(workers) if workers else 0
        running_per_worker = total_running / len(workers) if workers else 0

        return {
            "worker_count": len(workers),
            "avg_cpu_percent": avg_cpu,
            "avg_memory_percent": avg_memory,
            "total_pending_tasks": total_pending,
            "total_running_tasks": total_running,
            "pending_per_worker": pending_per_worker,
            "running_per_worker": running_per_worker,
            "queue_depth": total_pending,
        }

    def _analyze_scaling_need(
        self, metrics: Dict, current_workers: int
    ) -> Optional[str]:
        """Analyze if scaling is needed."""
        if current_workers >= self.config.max_workers:
            return None

        if (
            metrics["avg_cpu_percent"] > self.config.cpu_threshold
            or metrics["avg_memory_percent"] > self.config.memory_threshold
            or metrics["queue_depth"] > self.config.queue_depth_threshold
            or metrics["pending_per_worker"] > 10
        ):
            return "scale_up"

        if (
            current_workers > self.config.min_workers
            and metrics["avg_cpu_percent"] < self.config.scale_down_threshold * 100
            and metrics["pending_per_worker"] < 2
            and metrics["queue_depth"] < 10
        ):
            return "scale_down"

        return None

    def _can_scale(self) -> bool:
        """Check if enough time has passed since last scaling action."""
        time_since_last_scale = (
            datetime.now(timezone.utc) - self._last_scale_action
        ).total_seconds()

        return time_since_last_scale >= self.config.cool_down_period

    async def _execute_scaling(self, action: str, workers: List[Dict]) -> None:
        """Execute scaling action."""
        if action == "scale_up":
            await self._scale_up(workers)
        elif action == "scale_down":
            await self._scale_down(workers)

        self._last_scale_action = datetime.now(timezone.utc)

    async def _scale_up(self, workers: List[Dict]) -> None:
        """Scale up workers."""
        current_count = len(workers)
        target_count = min(current_count + 1, self.config.max_workers)

        if target_count > current_count:
            await self._trigger_worker_spawn(target_count - current_count)

    async def _scale_down(self, workers: List[Dict]) -> None:
        """Scale down workers."""
        current_count = len(workers)
        target_count = max(current_count - 1, self.config.min_workers)

        if target_count < current_count:
            await self._trigger_worker_shutdown(current_count - target_count, workers)

    async def _trigger_worker_spawn(self, count: int) -> None:
        """Trigger spawning of new workers."""
        async with self.queue_manager.redis as redis:
            await redis.publish("taskflow:scaling", f"spawn:{count}")

    async def _trigger_worker_shutdown(self, count: int, workers: List[Dict]) -> None:
        """Trigger shutdown of workers."""
        workers_to_shutdown = sorted(
            workers, key=lambda w: w.get("tasks_processed", 0)
        )[:count]

        async with self.queue_manager.redis as redis:
            for worker in workers_to_shutdown:
                worker_id = worker["worker_id"]
                await redis.publish("taskflow:scaling", f"shutdown:{worker_id}")


class TaskDistributor:
    """Distributes tasks across workers based on load."""

    def __init__(self, queue_manager: QueueManager):
        self.queue_manager = queue_manager
        self.worker_manager = WorkerManager(queue_manager)

    async def get_optimal_queue(self, task_priority: str = "normal") -> str:
        """Get the optimal queue for task submission."""
        queue_stats = await self.queue_manager.get_all_stats()

        if not queue_stats:
            return "default"

        queue_loads = {}
        for queue_name, stats in queue_stats.items():
            load_score = stats.pending_count + (stats.running_count * 0.5)
            queue_loads[queue_name] = load_score

        return min(queue_loads.keys(), key=lambda q: queue_loads[q])

    async def get_worker_assignment(self, task_id: str) -> Optional[str]:
        """Get optimal worker assignment for a task."""
        workers = await self.worker_manager.get_active_workers()

        if not workers:
            return None

        worker_loads = []
        for worker in workers:
            load_score = (
                worker.get("cpu_percent", 0) * 0.4
                + worker.get("memory_percent", 0) * 0.3
                + len(worker.get("current_tasks", [])) * 0.3
            )
            worker_loads.append((worker["worker_id"], load_score))

        worker_loads.sort(key=lambda x: x[1])

        return worker_loads[0][0] if worker_loads else None

    async def balance_queues(self) -> Dict[str, int]:
        """Balance tasks across queues."""
        queue_stats = await self.queue_manager.get_all_stats()

        if len(queue_stats) < 2:
            return {}

        total_pending = sum(stats.pending_count for stats in queue_stats.values())
        target_per_queue = total_pending // len(queue_stats)

        moves = {}

        for queue_name, stats in queue_stats.items():
            if stats.pending_count > target_per_queue * 1.5:
                excess = stats.pending_count - target_per_queue
                moves[queue_name] = -excess
            elif stats.pending_count < target_per_queue * 0.5:
                deficit = target_per_queue - stats.pending_count
                moves[queue_name] = deficit

        return moves


class WorkerHealthChecker:
    """Monitors worker health and handles failures."""

    def __init__(self, worker_manager: WorkerManager):
        self.worker_manager = worker_manager
        self.config = get_config()

        self._running = False
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """Start health checking."""
        self._running = True
        await self._health_check_loop()

    async def stop(self) -> None:
        """Stop health checking."""
        self._running = False
        self._shutdown_event.set()

    async def _health_check_loop(self) -> None:
        """Main health checking loop."""
        while self._running and not self._shutdown_event.is_set():
            try:
                dead_workers = await self.worker_manager.cleanup_dead_workers()

                if dead_workers:
                    await self._handle_dead_workers(dead_workers)

                await asyncio.sleep(self.config.worker.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(self.config.worker.heartbeat_interval)

    async def _handle_dead_workers(self, dead_workers: List[str]) -> None:
        """Handle dead worker cleanup and task recovery."""
        for worker_id in dead_workers:
            await self._recover_worker_tasks(worker_id)
            await self._notify_worker_failure(worker_id)

    async def _recover_worker_tasks(self, worker_id: str) -> None:
        """Recover tasks from a dead worker."""
        async with self.worker_manager.queue_manager.redis as redis:
            pattern = f"taskflow:processing:*"
            processing_keys = await redis.keys(pattern)

            for key in processing_keys:
                queue_name = key.split(":")[-1]
                queue = self.worker_manager.queue_manager.get_queue(queue_name)

                tasks = await queue.list_tasks(status=None)
                for task in tasks:
                    if task.worker_id == worker_id and task.status == "running":
                        task.status = "queued"
                        task.worker_id = None
                        task.started_at = None
                        await queue.requeue_task(task)

    async def _notify_worker_failure(self, worker_id: str) -> None:
        """Notify about worker failure."""
        async with self.worker_manager.queue_manager.redis as redis:
            failure_data = {
                "worker_id": worker_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event": "worker_failure",
            }
            await redis.publish("taskflow:events", str(failure_data))
