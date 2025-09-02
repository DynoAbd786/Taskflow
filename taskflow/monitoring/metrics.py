"""
Metrics collection and monitoring for TaskFlow system.
Provides Prometheus metrics, custom metrics, and system monitoring.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import psutil
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    Summary,
    generate_latest,
    start_http_server,
)

from taskflow.core.task import TaskStatus
from taskflow.utils.config import get_config


@dataclass
class MetricValue:
    """Container for metric values with metadata."""

    name: str
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metric_type: str = "gauge"


class PrometheusMetrics:
    """Prometheus metrics collector for TaskFlow."""

    def __init__(self, registry: Optional[CollectorRegistry] = None):
        self.registry = registry or CollectorRegistry()
        self.config = get_config()

        prefix = self.config.monitoring.metrics_prefix

        self.task_counter = Counter(
            f"{prefix}_tasks_total",
            "Total number of tasks processed",
            ["queue", "status", "worker"],
            registry=self.registry,
        )

        self.task_duration = Histogram(
            f"{prefix}_task_duration_seconds",
            "Task execution duration in seconds",
            ["queue", "task_name"],
            registry=self.registry,
        )

        self.task_wait_time = Histogram(
            f"{prefix}_task_wait_time_seconds",
            "Task wait time in queue in seconds",
            ["queue"],
            registry=self.registry,
        )

        self.queue_size = Gauge(
            f"{prefix}_queue_size",
            "Number of tasks in queue",
            ["queue", "status"],
            registry=self.registry,
        )

        self.worker_count = Gauge(
            f"{prefix}_workers_active",
            "Number of active workers",
            ["queue"],
            registry=self.registry,
        )

        self.worker_tasks = Gauge(
            f"{prefix}_worker_tasks_current",
            "Current number of tasks being processed by worker",
            ["worker_id", "queue"],
            registry=self.registry,
        )

        self.system_cpu = Gauge(
            f"{prefix}_system_cpu_percent",
            "System CPU usage percentage",
            registry=self.registry,
        )

        self.system_memory = Gauge(
            f"{prefix}_system_memory_percent",
            "System memory usage percentage",
            registry=self.registry,
        )

        self.system_disk = Gauge(
            f"{prefix}_system_disk_percent",
            "System disk usage percentage",
            registry=self.registry,
        )

        self.error_rate = Counter(
            f"{prefix}_errors_total",
            "Total number of errors",
            ["component", "error_type"],
            registry=self.registry,
        )

        self.throughput = Counter(
            f"{prefix}_throughput_total",
            "Total throughput (tasks per second)",
            ["queue"],
            registry=self.registry,
        )

    def record_task_completion(
        self,
        queue: str,
        task_name: str,
        status: TaskStatus,
        worker_id: str,
        duration: float,
        wait_time: float,
    ) -> None:
        """Record task completion metrics."""
        self.task_counter.labels(
            queue=queue, status=status.value, worker=worker_id
        ).inc()
        self.task_duration.labels(queue=queue, task_name=task_name).observe(duration)
        self.task_wait_time.labels(queue=queue).observe(wait_time)
        self.throughput.labels(queue=queue).inc()

    def update_queue_sizes(self, queue_sizes: Dict[str, Dict[str, int]]) -> None:
        """Update queue size metrics."""
        for queue_name, sizes in queue_sizes.items():
            for status, count in sizes.items():
                self.queue_size.labels(queue=queue_name, status=status).set(count)

    def update_worker_count(self, worker_counts: Dict[str, int]) -> None:
        """Update worker count metrics."""
        for queue, count in worker_counts.items():
            self.worker_count.labels(queue=queue).set(count)

    def update_worker_tasks(self, worker_tasks: Dict[str, Dict[str, int]]) -> None:
        """Update worker task counts."""
        for worker_id, queues in worker_tasks.items():
            for queue, count in queues.items():
                self.worker_tasks.labels(worker_id=worker_id, queue=queue).set(count)

    def update_system_metrics(self) -> None:
        """Update system resource metrics."""
        self.system_cpu.set(psutil.cpu_percent())
        self.system_memory.set(psutil.virtual_memory().percent)
        self.system_disk.set(psutil.disk_usage("/").percent)

    def record_error(self, component: str, error_type: str) -> None:
        """Record an error occurrence."""
        self.error_rate.labels(component=component, error_type=error_type).inc()

    def get_metrics(self) -> str:
        """Get Prometheus metrics in text format."""
        return generate_latest(self.registry)


class CustomMetricsCollector:
    """Custom metrics collector for application-specific metrics."""

    def __init__(self):
        self.metrics: Dict[str, List[MetricValue]] = {}
        self.aggregators: Dict[str, Callable] = {}
        self._lock = asyncio.Lock()

    async def record_metric(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
        metric_type: str = "gauge",
    ) -> None:
        """Record a custom metric value."""
        async with self._lock:
            metric = MetricValue(
                name=name, value=value, labels=labels or {}, metric_type=metric_type
            )

            if name not in self.metrics:
                self.metrics[name] = []

            self.metrics[name].append(metric)

            if len(self.metrics[name]) > 1000:
                self.metrics[name] = self.metrics[name][-500:]

    async def get_metric_values(
        self,
        name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        labels: Optional[Dict[str, str]] = None,
    ) -> List[MetricValue]:
        """Get metric values with filtering."""
        async with self._lock:
            if name not in self.metrics:
                return []

            values = self.metrics[name]

            if start_time:
                values = [v for v in values if v.timestamp >= start_time]

            if end_time:
                values = [v for v in values if v.timestamp <= end_time]

            if labels:
                values = [
                    v
                    for v in values
                    if all(v.labels.get(k) == v for k, v in labels.items())
                ]

            return values

    async def get_aggregated_metric(
        self,
        name: str,
        aggregation: str = "avg",
        labels: Optional[Dict[str, str]] = None,
    ) -> Optional[float]:
        """Get aggregated metric value."""
        values = await self.get_metric_values(name, labels=labels)

        if not values:
            return None

        numeric_values = [v.value for v in values]

        if aggregation == "avg":
            return sum(numeric_values) / len(numeric_values)
        elif aggregation == "sum":
            return sum(numeric_values)
        elif aggregation == "min":
            return min(numeric_values)
        elif aggregation == "max":
            return max(numeric_values)
        elif aggregation == "count":
            return len(numeric_values)

        return None

    def register_aggregator(self, name: str, func: Callable) -> None:
        """Register a custom aggregation function."""
        self.aggregators[name] = func

    async def cleanup_old_metrics(self, hours: int = 24) -> None:
        """Clean up old metric data."""
        cutoff_time = datetime.now(timezone.utc) - timezone.timedelta(hours=hours)

        async with self._lock:
            for name in self.metrics:
                self.metrics[name] = [
                    m for m in self.metrics[name] if m.timestamp >= cutoff_time
                ]


class PerformanceMonitor:
    """Performance monitoring and profiling."""

    def __init__(self):
        self.active_timers: Dict[str, float] = {}
        self.completed_timers: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def timer(self, operation: str, labels: Optional[Dict[str, str]] = None):
        """Context manager for timing operations."""
        start_time = time.time()
        timer_id = f"{operation}_{start_time}"

        async with self._lock:
            self.active_timers[timer_id] = start_time

        try:
            yield
        finally:
            end_time = time.time()
            duration = end_time - start_time

            async with self._lock:
                self.active_timers.pop(timer_id, None)
                self.completed_timers.append(
                    {
                        "operation": operation,
                        "duration": duration,
                        "start_time": start_time,
                        "end_time": end_time,
                        "labels": labels or {},
                    }
                )

                if len(self.completed_timers) > 1000:
                    self.completed_timers = self.completed_timers[-500:]

    async def get_performance_stats(
        self, operation: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get performance statistics."""
        async with self._lock:
            timers = self.completed_timers

            if operation:
                timers = [t for t in timers if t["operation"] == operation]

            if not timers:
                return {}

            durations = [t["duration"] for t in timers]

            return {
                "count": len(durations),
                "avg_duration": sum(durations) / len(durations),
                "min_duration": min(durations),
                "max_duration": max(durations),
                "total_duration": sum(durations),
                "operations": list(set(t["operation"] for t in timers)),
            }


class SystemMonitor:
    """System resource monitoring."""

    def __init__(self):
        self.config = get_config()
        self._running = False
        self._shutdown_event = asyncio.Event()

        self.cpu_history: List[float] = []
        self.memory_history: List[float] = []
        self.disk_history: List[float] = []

        self.process = psutil.Process()
        self.max_history_size = 100

    async def start(self) -> None:
        """Start system monitoring."""
        self._running = True
        await self._monitoring_loop()

    async def stop(self) -> None:
        """Stop system monitoring."""
        self._running = False
        self._shutdown_event.set()

    async def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        while self._running and not self._shutdown_event.is_set():
            try:
                await self._collect_system_metrics()
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(30)

    async def _collect_system_metrics(self) -> None:
        """Collect system resource metrics."""
        cpu_percent = psutil.cpu_percent(interval=1)
        memory_percent = psutil.virtual_memory().percent
        disk_percent = psutil.disk_usage("/").percent

        self.cpu_history.append(cpu_percent)
        self.memory_history.append(memory_percent)
        self.disk_history.append(disk_percent)

        if len(self.cpu_history) > self.max_history_size:
            self.cpu_history = self.cpu_history[-self.max_history_size :]

        if len(self.memory_history) > self.max_history_size:
            self.memory_history = self.memory_history[-self.max_history_size :]

        if len(self.disk_history) > self.max_history_size:
            self.disk_history = self.disk_history[-self.max_history_size :]

    def get_current_stats(self) -> Dict[str, Any]:
        """Get current system statistics."""
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage("/")

        process_memory = self.process.memory_info()

        return {
            "cpu_percent": psutil.cpu_percent(),
            "memory_percent": memory.percent,
            "memory_available_gb": memory.available / (1024**3),
            "memory_total_gb": memory.total / (1024**3),
            "disk_percent": (disk.used / disk.total) * 100,
            "disk_free_gb": disk.free / (1024**3),
            "disk_total_gb": disk.total / (1024**3),
            "process_memory_mb": process_memory.rss / (1024**2),
            "process_cpu_percent": self.process.cpu_percent(),
            "load_average": psutil.getloadavg()
            if hasattr(psutil, "getloadavg")
            else None,
        }

    def get_history(self) -> Dict[str, List[float]]:
        """Get historical system metrics."""
        return {
            "cpu_history": self.cpu_history.copy(),
            "memory_history": self.memory_history.copy(),
            "disk_history": self.disk_history.copy(),
        }


class MetricsManager:
    """Central metrics management."""

    def __init__(self):
        self.config = get_config()
        self.prometheus = PrometheusMetrics()
        self.custom = CustomMetricsCollector()
        self.performance = PerformanceMonitor()
        self.system = SystemMonitor()

        self._http_server = None
        self._running = False

    async def start(self) -> None:
        """Start metrics collection."""
        if self.config.monitoring.enabled:
            self._running = True

            if self.config.monitoring.prometheus_port:
                self._start_prometheus_server()

            if self.config.monitoring.collect_system_metrics:
                asyncio.create_task(self.system.start())

    async def stop(self) -> None:
        """Stop metrics collection."""
        self._running = False

        if self._http_server:
            self._http_server.shutdown()

        await self.system.stop()

    def _start_prometheus_server(self) -> None:
        """Start Prometheus HTTP server."""
        try:
            self._http_server = start_http_server(
                self.config.monitoring.prometheus_port,
                registry=self.prometheus.registry,
            )
        except Exception:
            pass

    async def record_task_metrics(
        self,
        queue: str,
        task_name: str,
        status: TaskStatus,
        worker_id: str,
        duration: float,
        wait_time: float,
    ) -> None:
        """Record task-related metrics."""
        self.prometheus.record_task_completion(
            queue, task_name, status, worker_id, duration, wait_time
        )

        await self.custom.record_metric(
            "task_duration",
            duration,
            {"queue": queue, "task_name": task_name, "status": status.value},
        )

        await self.custom.record_metric("task_wait_time", wait_time, {"queue": queue})

    async def update_queue_metrics(self, queue_data: Dict[str, Dict[str, int]]) -> None:
        """Update queue-related metrics."""
        self.prometheus.update_queue_sizes(queue_data)

        for queue_name, sizes in queue_data.items():
            for status, count in sizes.items():
                await self.custom.record_metric(
                    "queue_size", count, {"queue": queue_name, "status": status}
                )

    async def record_error(
        self, component: str, error_type: str, details: Optional[str] = None
    ) -> None:
        """Record error metrics."""
        self.prometheus.record_error(component, error_type)

        await self.custom.record_metric(
            "error_count",
            1,
            {"component": component, "error_type": error_type},
            metric_type="counter",
        )

    def get_prometheus_metrics(self) -> str:
        """Get Prometheus metrics."""
        return self.prometheus.get_metrics()

    async def get_dashboard_metrics(self) -> Dict[str, Any]:
        """Get metrics for dashboard display."""
        system_stats = self.system.get_current_stats()
        system_history = self.system.get_history()
        performance_stats = await self.performance.get_performance_stats()

        return {
            "system": system_stats,
            "history": system_history,
            "performance": performance_stats,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


_metrics_manager: Optional[MetricsManager] = None


def get_metrics_manager() -> MetricsManager:
    """Get the global metrics manager instance."""
    global _metrics_manager
    if _metrics_manager is None:
        _metrics_manager = MetricsManager()
    return _metrics_manager
