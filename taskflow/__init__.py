"""
TaskFlow - Distributed Task Processing System

A comprehensive system for managing background jobs, data processing pipelines,
and scheduled tasks across multiple workers with real-time monitoring.
"""

__version__ = "1.0.0"
__author__ = "TaskFlow Team"
__email__ = "team@taskflow.com"

from taskflow.core.engine import TaskEngine
from taskflow.core.queue import TaskQueue
from taskflow.core.task import Task, TaskStatus

# Import example tasks to register them
try:
    import taskflow.tasks
except ImportError:
    pass

__all__ = ["TaskEngine", "Task", "TaskStatus", "TaskQueue"]
