"""
Main FastAPI application for TaskFlow web interface.
Provides REST API and WebSocket endpoints for task management.
"""

import asyncio
import json
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from taskflow.api.models import (
    BatchTaskRequest,
    ErrorResponse,
    HealthCheckResponse,
    PaginatedResponse,
    QueueActionRequest,
    QueueStatsResponse,
    SystemEventMessage,
    SystemStatsResponse,
    TaskActionRequest,
    TaskEventMessage,
    TaskFilterParams,
    TaskResponse,
    TaskSubmissionRequest,
    WorkerActionRequest,
    WorkerResponse,
)
from taskflow.core.engine import TaskEngine, get_engine
from taskflow.core.task import TaskStatus, create_task
from taskflow.monitoring.metrics import get_metrics_manager
from taskflow.utils.config import get_config
from taskflow.workers.balancer import LoadBalancer
from taskflow.workers.worker import WorkerManager


async def broadcast_metrics(metrics_manager, websocket_manager):
    """Periodically broadcast system metrics to all connected clients."""
    while True:
        try:
            metrics = await metrics_manager.get_dashboard_metrics()
            await websocket_manager.broadcast({"type": "metrics", "metrics": metrics})
        except asyncio.CancelledError:
            break
        except Exception as e:
            # Only log errors that aren't connection-related during shutdown
            if "Error 111 connecting" not in str(e):
                print(f"Error broadcasting metrics: {e}")

        try:
            await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            break


async def pubsub_listener(websocket_manager):
    """Listen for Redis Pub/Sub messages and broadcast them to clients."""
    engine = get_engine()
    queue = engine.queue_manager.get_queue("default")
    pubsub = None

    try:
        async with queue.get_connection() as redis:
            pubsub = redis.pubsub()
            await pubsub.subscribe("task_events")

            while True:
                try:
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if message and message["type"] == "message":
                        data = json.loads(message["data"])
                        await websocket_manager.broadcast(data)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    # Only log errors that aren't connection-related during shutdown
                    if "Error 111 connecting" not in str(e):
                        print(f"Error in pubsub_listener: {e}")
    except asyncio.CancelledError:
        pass  # Normal shutdown
    except Exception as e:
        # Ignore Redis connection errors during shutdown
        if "Error 111 connecting" not in str(e):
            print(f"Error in pubsub_listener setup: {e}")
    finally:
        # Clean up pubsub connection
        if pubsub:
            try:
                await pubsub.unsubscribe("task_events")
                await pubsub.close()
            except Exception:
                pass  # Ignore cleanup errors


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    config = get_config()
    engine = get_engine()
    metrics_manager = get_metrics_manager()

    await engine.start()
    await metrics_manager.start()

    background_tasks = set()

    metrics_task = asyncio.create_task(
        broadcast_metrics(metrics_manager, websocket_manager)
    )
    background_tasks.add(metrics_task)

    pubsub_task = asyncio.create_task(pubsub_listener(websocket_manager))
    background_tasks.add(pubsub_task)

    app.state.background_tasks = background_tasks

    yield

    # Graceful shutdown of background tasks
    for task in app.state.background_tasks:
        if not task.done():
            task.cancel()

    # Wait for tasks to complete with timeout
    if app.state.background_tasks:
        try:
            await asyncio.wait_for(
                asyncio.gather(*app.state.background_tasks, return_exceptions=True),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            pass  # Tasks didn't finish in time, that's okay

    # Shutdown engine and metrics with error handling
    try:
        await engine.shutdown()
    except Exception:
        pass  # Ignore shutdown errors
        
    try:
        await metrics_manager.stop()
    except Exception:
        pass  # Ignore shutdown errors


app = FastAPI(
    title="TaskFlow API",
    description="Distributed task processing system with web interface",
    version="1.0.0",
    lifespan=lifespan,
)

config = get_config()

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.api.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")


class WebSocketManager:
    """Manages WebSocket connections for real-time updates."""

    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def send_personal_message(self, message: dict, websocket: WebSocket):
        try:
            await websocket.send_json(message)
        except Exception:
            self.disconnect(websocket)

    async def broadcast(self, message: dict):
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                disconnected.append(connection)

        for connection in disconnected:
            self.disconnect(connection)


websocket_manager = WebSocketManager()


def get_task_engine() -> TaskEngine:
    """Dependency to get task engine."""
    return get_engine()


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Serve the main dashboard."""
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/health", response_model=HealthCheckResponse)
async def health_check(engine: TaskEngine = Depends(get_task_engine)):
    """System health check endpoint."""
    health_data = await engine.health_check()
    return HealthCheckResponse(**health_data)


@app.post("/tasks", response_model=TaskResponse)
async def submit_task(
    request: TaskSubmissionRequest, engine: TaskEngine = Depends(get_task_engine)
):
    """Submit a new task for execution."""
    try:
        task_id = await engine.submit_task(
            name=request.name,
            args=request.args,
            kwargs=request.kwargs,
            queue=request.queue,
            delay=request.delay,
            scheduled_at=request.scheduled_at,
            priority=request.priority,
            timeout=request.timeout,
            max_retries=request.max_retries,
            tags=request.tags,
            **request.metadata,
        )

        task = await engine.get_task(task_id, request.queue)
        if not task:
            raise HTTPException(
                status_code=404, detail="Task not found after submission"
            )

        await websocket_manager.broadcast(
            {
                "type": "task_event",
                "task_id": task_id,
                "event": "submitted",
                "status": task.status.value,
                "queue_name": request.queue,
            }
        )

        return TaskResponse(
            id=task.id,
            name=task.name,
            status=task.status,
            created_at=task.created_at,
            started_at=task.started_at,
            completed_at=task.completed_at,
            queue_name=task.queue_name,
            worker_id=task.worker_id,
            retry_count=task.retry_count,
            args=task.args,
            kwargs=task.kwargs,
            tags=task.config.tags,
            metadata=task.metadata,
            result=task.result.to_dict() if task.result else None,
            execution_time=task.get_execution_time(),
            wait_time=task.get_wait_time(),
        )

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/tasks/batch", response_model=List[TaskResponse])
async def submit_batch_tasks(
    request: BatchTaskRequest, engine: TaskEngine = Depends(get_task_engine)
):
    """Submit a batch of tasks for execution."""
    try:
        tasks = []

        for i in range(0, len(request.items), request.chunk_size):
            chunk = request.items[i : i + request.chunk_size]

            task_id = await engine.submit_task(
                name=request.name,
                args=[chunk],
                queue=request.queue,
                priority=request.priority,
                timeout=request.timeout,
                max_retries=request.max_retries,
                batch_id=f"batch_{i // request.chunk_size}",
                batch_total=len(request.items),
            )

            task = await engine.get_task(task_id, request.queue)
            if task:
                tasks.append(
                    TaskResponse(
                        id=task.id,
                        name=task.name,
                        status=task.status,
                        created_at=task.created_at,
                        queue_name=task.queue_name,
                        args=task.args,
                        kwargs=task.kwargs,
                        metadata=task.metadata,
                    )
                )

        return tasks

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task(
    task_id: str, queue: str = "default", engine: TaskEngine = Depends(get_task_engine)
):
    """Get task details by ID."""
    task = await engine.get_task(task_id, queue)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    # Get task logs
    task_queue = engine.queue_manager.get_queue(queue)
    task_logs = []
    try:
        async with task_queue.get_connection() as redis:
            logs_data = await redis.hgetall(f"taskflow:task_logs:{task_id}")
            for key, value in logs_data.items():
                task_logs.append(f"{key}: {value}")
    except Exception:
        pass  # Logs are optional

    response_data = TaskResponse(
        id=task.id,
        name=task.name,
        status=task.status,
        created_at=task.created_at,
        started_at=task.started_at,
        completed_at=task.completed_at,
        queue_name=task.queue_name,
        worker_id=task.worker_id,
        retry_count=task.retry_count,
        args=task.args,
        kwargs=task.kwargs,
        tags=task.config.tags,
        metadata=task.metadata,
        result=task.result.to_dict() if task.result else None,
        execution_time=task.get_execution_time(),
        wait_time=task.get_wait_time(),
    ).dict()
    
    # Add logs to response
    response_data['logs'] = task_logs
    
    return response_data


@app.post("/tasks/{task_id}/actions")
async def task_action(
    task_id: str,
    action_request: TaskActionRequest,
    queue: str = "default",
    engine: TaskEngine = Depends(get_task_engine),
):
    """Perform actions on a task (cancel, retry, delete)."""
    try:
        if action_request.action == "cancel":
            success = await engine.cancel_task(task_id, queue)
            if not success:
                raise HTTPException(status_code=400, detail="Cannot cancel task")

        elif action_request.action == "retry":
            success = await engine.retry_task(task_id, queue)
            if not success:
                raise HTTPException(status_code=400, detail="Cannot retry task")

        else:
            raise HTTPException(status_code=400, detail="Invalid action")

        await websocket_manager.broadcast(
            {
                "type": "task_event",
                "task_id": task_id,
                "event": action_request.action,
                "queue_name": queue,
            }
        )

        return {"message": f"Task {action_request.action} successful"}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/tasks", response_model=PaginatedResponse)
async def list_tasks(
    filters: TaskFilterParams = Depends(), engine: TaskEngine = Depends(get_task_engine)
):
    """List tasks with filtering and pagination."""
    try:
        queue = filters.queue or "default"
        tasks = await engine.list_tasks(
            queue=queue,
            status=filters.status,
            limit=filters.limit,
            offset=filters.offset,
        )

        task_responses = []
        for task in tasks:
            if filters.worker_id and task.worker_id != filters.worker_id:
                continue

            if filters.tags and not set(filters.tags).intersection(
                set(task.config.tags)
            ):
                continue

            if filters.created_after and task.created_at < filters.created_after:
                continue

            if filters.created_before and task.created_at > filters.created_before:
                continue

            task_responses.append(
                TaskResponse(
                    id=task.id,
                    name=task.name,
                    status=task.status,
                    created_at=task.created_at,
                    started_at=task.started_at,
                    completed_at=task.completed_at,
                    queue_name=task.queue_name,
                    worker_id=task.worker_id,
                    retry_count=task.retry_count,
                    args=task.args,
                    kwargs=task.kwargs,
                    tags=task.config.tags,
                    metadata=task.metadata,
                    result=task.result.to_dict() if task.result else None,
                    execution_time=task.get_execution_time(),
                    wait_time=task.get_wait_time(),
                )
            )

        return PaginatedResponse(
            items=task_responses,
            total=len(task_responses),
            limit=filters.limit,
            offset=filters.offset,
            has_next=len(task_responses) == filters.limit,
            has_prev=filters.offset > 0,
        )

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/queues", response_model=List[str])
async def list_queues(engine: TaskEngine = Depends(get_task_engine)):
    """List all available queues."""
    return await engine.queue_manager.list_queues()


@app.get("/queues/{queue_name}/stats", response_model=QueueStatsResponse)
async def get_queue_stats(
    queue_name: str, engine: TaskEngine = Depends(get_task_engine)
):
    """Get statistics for a specific queue."""
    stats = await engine.get_queue_stats(queue_name)
    return QueueStatsResponse(**stats)


@app.post("/queues/{queue_name}/actions")
async def queue_action(
    queue_name: str,
    action_request: QueueActionRequest,
    engine: TaskEngine = Depends(get_task_engine),
):
    """Perform actions on a queue (purge, pause, resume)."""
    try:
        if action_request.action == "purge":
            count = await engine.purge_queue(queue_name)
            return {"message": f"Purged {count} tasks from queue {queue_name}"}

        else:
            raise HTTPException(status_code=400, detail="Invalid action")

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/workers", response_model=List[WorkerResponse])
async def list_workers(engine: TaskEngine = Depends(get_task_engine)):
    """List all active workers."""
    worker_manager = WorkerManager(engine.queue_manager)
    workers = await worker_manager.get_active_workers()

    worker_responses = []
    for worker in workers:
        worker_responses.append(
            WorkerResponse(
                worker_id=worker["worker_id"],
                status="active",
                queues=worker.get("queues", "").split(","),
                started_at=worker.get("registered_at"),
                last_heartbeat=worker.get("last_heartbeat"),
                tasks_processed=worker.get("tasks_processed", 0),
                tasks_succeeded=worker.get("tasks_succeeded", 0),
                tasks_failed=worker.get("tasks_failed", 0),
                current_task=worker.get("current_task"),
                cpu_percent=worker.get("cpu_percent", 0.0),
                memory_percent=worker.get("memory_percent", 0.0),
                memory_mb=worker.get("memory_mb", 0.0),
                uptime_seconds=worker.get("uptime_seconds", 0.0),
            )
        )

    return worker_responses


@app.get("/stats", response_model=SystemStatsResponse)
async def get_system_stats(engine: TaskEngine = Depends(get_task_engine)):
    """Get overall system statistics."""
    all_stats = await engine.get_all_queue_stats()
    worker_manager = WorkerManager(engine.queue_manager)
    workers = await worker_manager.get_active_workers()

    total_tasks = 0
    pending_tasks = 0
    running_tasks = 0
    completed_tasks = 0
    failed_tasks = 0
    total_execution_time = 0.0

    for queue_stats in all_stats.values():
        pending_tasks += queue_stats["pending_count"]
        running_tasks += queue_stats["running_count"]
        completed_tasks += queue_stats["completed_count"]
        failed_tasks += queue_stats["failed_count"]
        total_execution_time += queue_stats["avg_execution_time"]

    total_tasks = pending_tasks + running_tasks + completed_tasks + failed_tasks
    avg_execution_time = total_execution_time / len(all_stats) if all_stats else 0.0

    return SystemStatsResponse(
        total_tasks=total_tasks,
        pending_tasks=pending_tasks,
        running_tasks=running_tasks,
        completed_tasks=completed_tasks,
        failed_tasks=failed_tasks,
        active_workers=len(workers),
        active_queues=len(all_stats),
        avg_execution_time=avg_execution_time,
        system_uptime=0.0,
    )


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates."""
    await websocket_manager.connect(websocket)

    try:
        while True:
            data = await websocket.receive_text()

            if data == "ping":
                await websocket.send_json({"type": "pong"})

    except WebSocketDisconnect:
        websocket_manager.disconnect(websocket)


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions."""
    return ErrorResponse(
        error=exc.__class__.__name__,
        message=str(exc.detail),
        details={"status_code": exc.status_code},
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "taskflow.api.main:app",
        host=config.api.host,
        port=config.api.port,
        reload=config.api.reload,
        workers=config.api.workers,
        access_log=config.api.access_log,
    )
