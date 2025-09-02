"""
Task management commands for TaskFlow CLI.
Handle task submission, monitoring, and management.
"""

import asyncio
import json
import sys
from datetime import datetime
from typing import Optional

import click
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from taskflow.core.task import TaskStatus

console = Console()


@click.group(name="task")
def task_group():
    """Task management commands."""
    pass


@task_group.command()
@click.argument("name")
@click.option("--args", help="Task arguments as JSON string")
@click.option("--kwargs", help="Task keyword arguments as JSON string")
@click.option("--queue", default="default", help="Queue name")
@click.option(
    "--priority",
    type=click.Choice(["low", "normal", "high", "critical"]),
    default="normal",
    help="Task priority",
)
@click.option("--timeout", type=int, default=300, help="Task timeout in seconds")
@click.option("--max-retries", type=int, default=3, help="Maximum retry attempts")
@click.option("--delay", type=int, help="Delay execution by seconds")
@click.option("--tags", help="Comma-separated tags")
@click.pass_context
def submit(ctx, name, args, kwargs, queue, priority, timeout, max_retries, delay, tags):
    """Submit a new task for execution."""

    async def submit_task():
        try:
            from taskflow.core.engine import get_engine
            from taskflow.core.task import TaskPriority
            
            # Import tasks to ensure they are registered
            import taskflow.tasks

            engine = get_engine()
            await engine.start()

            # Parse arguments
            task_args = json.loads(args) if args else []
            task_kwargs = json.loads(kwargs) if kwargs else {}
            task_tags = [tag.strip() for tag in tags.split(",")] if tags else []

            # Submit task
            task_id = await engine.submit_task(
                name=name,
                args=task_args,
                kwargs=task_kwargs,
                queue=queue,
                delay=delay,
                priority=TaskPriority(priority),
                timeout=timeout,
                max_retries=max_retries,
                tags=task_tags,
            )

            console.print(f"[green]Task submitted successfully![/green]")
            console.print(f"Task ID: [cyan]{task_id}[/cyan]")
            console.print(f"Queue: [yellow]{queue}[/yellow]")
            console.print(f"Priority: [blue]{priority}[/blue]")

            await engine.shutdown()
            return 0

        except json.JSONDecodeError as e:
            console.print(f"[red]Invalid JSON in arguments: {e}[/red]")
            return 1
        except Exception as e:
            console.print(f"[red]Failed to submit task: {e}[/red]")
            return 1

    exit_code = asyncio.run(submit_task())
    sys.exit(exit_code)


@task_group.command()
@click.argument("task_id")
@click.option("--queue", default="default", help="Queue name")
@click.pass_context
def get(ctx, task_id, queue):
    """Get task details by ID."""

    async def get_task():
        try:
            from taskflow.core.engine import get_engine

            engine = get_engine()
            await engine.start()

            task = await engine.get_task(task_id, queue)

            if not task:
                console.print(f"[red]Task {task_id} not found in queue {queue}[/red]")
                return 1

            # Display task details
            table = Table(title=f"Task Details: {task_id}")
            table.add_column("Property", style="cyan")
            table.add_column("Value", style="green")

            table.add_row("ID", task.id)
            table.add_row("Name", task.name)
            table.add_row(
                "Status",
                task.status.value
                if hasattr(task.status, "value")
                else str(task.status),
            )
            table.add_row("Queue", task.queue_name or "N/A")
            table.add_row("Worker ID", task.worker_id or "N/A")
            table.add_row(
                "Priority",
                task.config.priority.value
                if hasattr(task.config.priority, "value")
                else str(task.config.priority),
            )
            table.add_row("Created At", task.created_at.isoformat())
            table.add_row(
                "Started At", task.started_at.isoformat() if task.started_at else "N/A"
            )
            table.add_row(
                "Completed At",
                task.completed_at.isoformat() if task.completed_at else "N/A",
            )
            table.add_row(
                "Retry Count", f"{task.retry_count}/{task.config.max_retries}"
            )
            table.add_row("Timeout", f"{task.config.timeout}s")

            if task.args:
                table.add_row("Arguments", json.dumps(task.args, indent=2))

            if task.kwargs:
                table.add_row("Keyword Args", json.dumps(task.kwargs, indent=2))

            if task.config.tags:
                table.add_row("Tags", ", ".join(task.config.tags))

            if task.result:
                table.add_row(
                    "Result Status",
                    task.result.status.value
                    if hasattr(task.result.status, "value")
                    else str(task.result.status),
                )
                if task.result.result:
                    table.add_row("Result Data", str(task.result.result))
                if task.result.error:
                    table.add_row("Error", task.result.error)
                if task.result.execution_time:
                    table.add_row(
                        "Execution Time", f"{task.result.execution_time:.2f}s"
                    )

            console.print(table)

            await engine.shutdown()
            return 0

        except Exception as e:
            console.print(f"[red]Failed to get task: {e}[/red]")
            return 1

    exit_code = asyncio.run(get_task())
    sys.exit(exit_code)


@task_group.command()
@click.option("--queue", help="Filter by queue name")
@click.option(
    "--status",
    type=click.Choice(["pending", "queued", "running", "success", "failure"]),
    help="Filter by status",
)
@click.option("--worker", help="Filter by worker ID")
@click.option("--limit", type=int, default=20, help="Number of tasks to show")
@click.option("--offset", type=int, default=0, help="Offset for pagination")
@click.option("--watch", is_flag=True, help="Watch for real-time updates")
@click.pass_context
def list(ctx, queue, status, worker, limit, offset, watch):
    """List tasks with filtering options."""

    async def list_tasks():
        try:
            from taskflow.core.engine import get_engine

            engine = get_engine()
            await engine.start()

            if watch:
                # Real-time monitoring
                with Live(console=console, refresh_per_second=1) as live:
                    while True:
                        try:
                            tasks = await engine.list_tasks(
                                queue=queue or "default",
                                status=TaskStatus(status) if status else None,
                                limit=limit,
                                offset=offset,
                            )

                            table = Table(
                                title=f"Tasks (Live - {datetime.now().strftime('%H:%M:%S')})"
                            )
                            table.add_column("ID", style="cyan", width=12)
                            table.add_column("Name", style="green")
                            table.add_column("Status", style="yellow")
                            table.add_column("Queue", style="blue")
                            table.add_column("Worker", style="magenta", width=12)
                            table.add_column("Created", style="white")

                            for task in tasks:
                                # Filter by worker if specified
                                if worker and task.worker_id != worker:
                                    continue

                                status_emoji = {
                                    "pending": "⏳",
                                    "queued": "📝",
                                    "running": "⚡",
                                    "success": "✅",
                                    "failure": "❌",
                                    "cancelled": "🚫",
                                    "timeout": "⏰",
                                }.get(
                                    task.status.value
                                    if hasattr(task.status, "value")
                                    else str(task.status),
                                    "❓",
                                )

                                task_status_str = (
                                    task.status.value
                                    if hasattr(task.status, "value")
                                    else str(task.status)
                                )
                                table.add_row(
                                    task.id[:12],
                                    task.name,
                                    f"{status_emoji} {task_status_str}",
                                    task.queue_name or "N/A",
                                    task.worker_id[:12] if task.worker_id else "N/A",
                                    task.created_at.strftime("%H:%M:%S"),
                                )

                            live.update(table)

                        except Exception as e:
                            error_panel = Panel(
                                f"[red]Error: {e}[/red]", title="List Error"
                            )
                            live.update(error_panel)

                        await asyncio.sleep(1)
            else:
                # One-time list
                tasks = await engine.list_tasks(
                    queue=queue or "default",
                    status=TaskStatus(status) if status else None,
                    limit=limit,
                    offset=offset,
                )

                if not tasks:
                    console.print("[yellow]No tasks found[/yellow]")
                    return 0

                table = Table(title=f"Tasks ({len(tasks)} found)")
                table.add_column("#", style="dim", width=3)
                table.add_column("Task ID", style="cyan", width=36)
                table.add_column("Name", style="green")
                table.add_column("Status", style="yellow")
                table.add_column("Queue", style="blue")
                table.add_column("Worker", style="magenta")
                table.add_column("Created", style="white")
                table.add_column("Duration", style="white")

                # Filter tasks by worker if specified
                filtered_tasks = []
                for task in tasks:
                    if worker and task.worker_id != worker:
                        continue
                    filtered_tasks.append(task)

                for i, task in enumerate(filtered_tasks, 1):
                    # Handle both string and enum status
                    status_str = (
                        task.status.value
                        if hasattr(task.status, "value")
                        else str(task.status)
                    )
                    status_emoji = {
                        "pending": "⏳",
                        "queued": "📝",
                        "running": "⚡",
                        "success": "✅",
                        "failure": "❌",
                        "cancelled": "🚫",
                        "timeout": "⏰",
                    }.get(status_str, "❓")

                    duration = "N/A"
                    if task.get_execution_time():
                        duration = f"{task.get_execution_time():.2f}s"
                    elif task.started_at:
                        duration = f"{(datetime.now() - task.started_at.replace(tzinfo=None)).total_seconds():.0f}s"

                    table.add_row(
                        str(i),
                        task.id,
                        task.name,
                        f"{status_emoji}",
                        task.queue_name or "N/A",
                        task.worker_id[:12] + "..." if task.worker_id and len(task.worker_id) > 12 else task.worker_id or "N/A",
                        task.created_at.strftime("%m-%d %H:%M:%S"),
                        duration,
                    )

                console.print(table)

            await engine.shutdown()
            return 0

        except KeyboardInterrupt:
            console.print("\n[yellow]Stopped by user[/yellow]")
            return 0
        except Exception as e:
            console.print(f"[red]Failed to list tasks: {e}[/red]")
            return 1

    exit_code = asyncio.run(list_tasks())
    sys.exit(exit_code)


@task_group.command()
@click.argument("task_id")
@click.option("--queue", default="default", help="Queue name")
@click.pass_context
def cancel(ctx, task_id, queue):
    """Cancel a pending task."""

    async def cancel_task():
        try:
            from taskflow.core.engine import get_engine

            engine = get_engine()
            await engine.start()

            success = await engine.cancel_task(task_id, queue)

            if success:
                console.print(f"[green]Task {task_id} cancelled successfully[/green]")
            else:
                console.print(f"[red]Failed to cancel task {task_id}[/red]")
                return 1

            await engine.shutdown()
            return 0

        except Exception as e:
            console.print(f"[red]Failed to cancel task: {e}[/red]")
            return 1

    exit_code = asyncio.run(cancel_task())
    sys.exit(exit_code)


@task_group.command()
@click.argument("task_id")
@click.option("--queue", default="default", help="Queue name")
@click.pass_context
def retry(ctx, task_id, queue):
    """Retry a failed task."""

    async def retry_task():
        try:
            from taskflow.core.engine import get_engine

            engine = get_engine()
            await engine.start()

            success = await engine.retry_task(task_id, queue)

            if success:
                console.print(f"[green]Task {task_id} queued for retry[/green]")
            else:
                console.print(f"[red]Failed to retry task {task_id}[/red]")
                return 1

            await engine.shutdown()
            return 0

        except Exception as e:
            console.print(f"[red]Failed to retry task: {e}[/red]")
            return 1

    exit_code = asyncio.run(retry_task())
    sys.exit(exit_code)


@task_group.command()
@click.option("--queue", help="Filter by queue name")
@click.option(
    "--status",
    type=click.Choice(["pending", "running", "completed", "failed"]),
    help="Filter by status",
)
@click.pass_context
def stats(ctx, queue, status):
    """Show task statistics."""

    async def show_stats():
        try:
            from taskflow.core.engine import get_engine

            engine = get_engine()
            await engine.start()

            if queue:
                # Queue-specific stats
                stats = await engine.get_queue_stats(queue)

                table = Table(title=f"Queue Statistics: {queue}")
                table.add_column("Metric", style="cyan")
                table.add_column("Value", style="green")

                table.add_row("Pending Tasks", str(stats["pending_count"]))
                table.add_row("Running Tasks", str(stats["running_count"]))
                table.add_row("Completed Tasks", str(stats["completed_count"]))
                table.add_row("Failed Tasks", str(stats["failed_count"]))
                table.add_row("Total Processed", str(stats["total_processed"]))
                table.add_row(
                    "Avg Execution Time", f"{stats['avg_execution_time']:.2f}s"
                )
                table.add_row("Avg Wait Time", f"{stats['avg_wait_time']:.2f}s")

                console.print(table)
            else:
                # Overall stats
                all_stats = await engine.get_all_queue_stats()

                table = Table(title="Overall Task Statistics")
                table.add_column("Queue", style="cyan")
                table.add_column("Pending", style="yellow")
                table.add_column("Running", style="blue")
                table.add_column("Completed", style="green")
                table.add_column("Failed", style="red")
                table.add_column("Avg Time", style="white")

                for queue_name, stats in all_stats.items():
                    table.add_row(
                        queue_name,
                        str(stats["pending_count"]),
                        str(stats["running_count"]),
                        str(stats["completed_count"]),
                        str(stats["failed_count"]),
                        f"{stats['avg_execution_time']:.2f}s",
                    )

                console.print(table)

            await engine.shutdown()
            return 0

        except Exception as e:
            console.print(f"[red]Failed to get statistics: {e}[/red]")
            return 1

    exit_code = asyncio.run(show_stats())
    sys.exit(exit_code)
