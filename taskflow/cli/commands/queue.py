"""
Queue management commands for TaskFlow CLI.
Handle queue operations, monitoring, and configuration.
"""

import asyncio
import sys

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


@click.group(name="queue")
def queue_group():
    """Queue management commands."""
    pass


@queue_group.command()
@click.pass_context
def list(ctx):
    """List all queues."""

    async def list_queues():
        try:
            from taskflow.core.engine import get_engine

            engine = get_engine()
            await engine.start()

            queue_names = await engine.queue_manager.list_queues()

            if not queue_names:
                console.print("[yellow]No queues found[/yellow]")
                return 0

            # Get stats for each queue
            table = Table(title=f"Queues ({len(queue_names)} found)")
            table.add_column("Queue Name", style="cyan")
            table.add_column("Pending", style="yellow")
            table.add_column("Running", style="blue")
            table.add_column("Completed", style="green")
            table.add_column("Failed", style="red")
            table.add_column("Total", style="white")
            table.add_column("Avg Time", style="magenta")

            for queue_name in queue_names:
                try:
                    stats = await engine.get_queue_stats(queue_name)

                    total = (
                        stats["pending_count"]
                        + stats["running_count"]
                        + stats["completed_count"]
                        + stats["failed_count"]
                    )

                    table.add_row(
                        queue_name,
                        str(stats["pending_count"]),
                        str(stats["running_count"]),
                        str(stats["completed_count"]),
                        str(stats["failed_count"]),
                        str(total),
                        f"{stats['avg_execution_time']:.2f}s",
                    )
                except Exception:
                    table.add_row(
                        queue_name, "Error", "Error", "Error", "Error", "Error", "Error"
                    )

            console.print(table)

            await engine.shutdown()
            return 0

        except Exception as e:
            console.print(f"[red]Failed to list queues: {e}[/red]")
            return 1

    exit_code = asyncio.run(list_queues())
    sys.exit(exit_code)


@queue_group.command()
@click.argument("queue_name")
@click.pass_context
def info(ctx, queue_name):
    """Show detailed information about a queue."""

    async def show_queue_info():
        try:
            from taskflow.core.engine import get_engine

            engine = get_engine()
            await engine.start()

            # Get queue stats
            stats = await engine.get_queue_stats(queue_name)

            # Display queue information
            table = Table(title=f"Queue Information: {queue_name}")
            table.add_column("Property", style="cyan")
            table.add_column("Value", style="green")

            table.add_row("Queue Name", queue_name)
            table.add_row("Pending Tasks", str(stats["pending_count"]))
            table.add_row("Running Tasks", str(stats["running_count"]))
            table.add_row("Completed Tasks", str(stats["completed_count"]))
            table.add_row("Failed Tasks", str(stats["failed_count"]))
            table.add_row("Total Processed", str(stats["total_processed"]))
            table.add_row(
                "Average Execution Time", f"{stats['avg_execution_time']:.2f}s"
            )
            table.add_row("Average Wait Time", f"{stats['avg_wait_time']:.2f}s")

            # Queue sizes breakdown
            if "queue_sizes" in stats:
                sizes = stats["queue_sizes"]
                table.add_row("Priority Breakdown", "")
                for priority, count in sizes.items():
                    if priority != "processing" and priority != "delayed":
                        table.add_row(f"  {priority.title()}", str(count))

                if "processing" in sizes:
                    table.add_row("Currently Processing", str(sizes["processing"]))

                if "delayed" in sizes:
                    table.add_row("Delayed Tasks", str(sizes["delayed"]))

            console.print(table)

            await engine.shutdown()
            return 0

        except Exception as e:
            console.print(f"[red]Failed to get queue info: {e}[/red]")
            return 1

    exit_code = asyncio.run(show_queue_info())
    sys.exit(exit_code)


@queue_group.command()
@click.argument("queue_name")
@click.option("--confirm", is_flag=True, help="Skip confirmation prompt")
@click.pass_context
def purge(ctx, queue_name, confirm):
    """Purge all tasks from a queue."""
    if not confirm:
        if not click.confirm(
            f"Are you sure you want to purge all tasks from queue '{queue_name}'?"
        ):
            console.print("[yellow]Purge cancelled[/yellow]")
            return

    async def purge_queue():
        try:
            from taskflow.core.engine import get_engine

            engine = get_engine()
            await engine.start()

            count = await engine.purge_queue(queue_name)

            console.print(
                f"[green]Purged {count} tasks from queue '{queue_name}'[/green]"
            )

            await engine.shutdown()
            return 0

        except Exception as e:
            console.print(f"[red]Failed to purge queue: {e}[/red]")
            return 1

    exit_code = asyncio.run(purge_queue())
    sys.exit(exit_code)


@queue_group.command()
@click.argument("queue_name")
@click.option(
    "--status",
    type=click.Choice(["pending", "running", "completed", "failed"]),
    help="Filter by task status",
)
@click.option("--limit", type=int, default=20, help="Number of tasks to show")
@click.pass_context
def tasks(ctx, queue_name, status, limit):
    """List tasks in a queue."""

    async def list_queue_tasks():
        try:
            from taskflow.core.engine import get_engine
            from taskflow.core.task import TaskStatus

            engine = get_engine()
            await engine.start()

            task_status = TaskStatus(status) if status else None
            tasks = await engine.list_tasks(
                queue=queue_name, status=task_status, limit=limit
            )

            if not tasks:
                console.print(
                    f"[yellow]No tasks found in queue '{queue_name}'[/yellow]"
                )
                return 0

            table = Table(title=f"Tasks in Queue: {queue_name} ({len(tasks)} found)")
            table.add_column("Task ID", style="cyan")
            table.add_column("Name", style="green")
            table.add_column("Status", style="yellow")
            table.add_column("Worker", style="blue")
            table.add_column("Created", style="white")
            table.add_column("Duration", style="magenta")

            for task in tasks:
                status_emoji = {
                    "pending": "⏳",
                    "queued": "📝",
                    "running": "⚡",
                    "success": "✅",
                    "failure": "❌",
                    "cancelled": "🚫",
                    "timeout": "⏰",
                }.get(task.status.value, "❓")

                duration = "N/A"
                if task.get_execution_time():
                    duration = f"{task.get_execution_time():.2f}s"

                table.add_row(
                    task.id[:12],
                    task.name,
                    f"{status_emoji} {task.status.value}",
                    task.worker_id[:12] if task.worker_id else "N/A",
                    task.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                    duration,
                )

            console.print(table)

            await engine.shutdown()
            return 0

        except Exception as e:
            console.print(f"[red]Failed to list queue tasks: {e}[/red]")
            return 1

    exit_code = asyncio.run(list_queue_tasks())
    sys.exit(exit_code)


@queue_group.command()
@click.argument("queue_name")
@click.option("--watch", is_flag=True, help="Watch for real-time updates")
@click.option("--interval", type=int, default=5, help="Update interval for watch mode")
@click.pass_context
def monitor(ctx, queue_name, watch, interval):
    """Monitor queue metrics."""

    async def monitor_queue():
        try:
            from datetime import datetime

            from rich.live import Live

            from taskflow.core.engine import get_engine

            engine = get_engine()
            await engine.start()

            if watch:
                with Live(console=console, refresh_per_second=1 / interval) as live:
                    while True:
                        try:
                            stats = await engine.get_queue_stats(queue_name)

                            # Create monitoring display
                            table = Table(
                                title=f"Queue Monitor: {queue_name} ({datetime.now().strftime('%H:%M:%S')})"
                            )
                            table.add_column("Metric", style="cyan")
                            table.add_column("Current", style="green")
                            table.add_column("Total", style="yellow")

                            table.add_row(
                                "Pending Tasks", str(stats["pending_count"]), ""
                            )
                            table.add_row(
                                "Running Tasks", str(stats["running_count"]), ""
                            )
                            table.add_row(
                                "Completed Tasks",
                                str(stats["completed_count"]),
                                str(stats["total_processed"]),
                            )
                            table.add_row(
                                "Failed Tasks", str(stats["failed_count"]), ""
                            )
                            table.add_row(
                                "Avg Execution Time",
                                f"{stats['avg_execution_time']:.2f}s",
                                "",
                            )
                            table.add_row(
                                "Avg Wait Time", f"{stats['avg_wait_time']:.2f}s", ""
                            )

                            # Calculate throughput (very basic)
                            throughput = stats["total_processed"] / max(1, interval)
                            table.add_row(
                                "Throughput", f"{throughput:.2f} tasks/sec", ""
                            )

                            # Queue health indicator
                            health = "🟢 Healthy"
                            if (
                                stats["failed_count"] > stats["completed_count"] * 0.1
                            ):  # >10% failure rate
                                health = "🔴 High Failure Rate"
                            elif stats["pending_count"] > 100:  # Large backlog
                                health = "🟡 High Backlog"

                            table.add_row("Health", health, "")

                            live.update(table)

                        except Exception as e:
                            error_panel = Panel(
                                f"[red]Error: {e}[/red]", title="Monitor Error"
                            )
                            live.update(error_panel)

                        await asyncio.sleep(interval)
            else:
                # One-time stats display
                stats = await engine.get_queue_stats(queue_name)

                table = Table(title=f"Queue Statistics: {queue_name}")
                table.add_column("Metric", style="cyan")
                table.add_column("Value", style="green")

                table.add_row("Pending Tasks", str(stats["pending_count"]))
                table.add_row("Running Tasks", str(stats["running_count"]))
                table.add_row("Completed Tasks", str(stats["completed_count"]))
                table.add_row("Failed Tasks", str(stats["failed_count"]))
                table.add_row("Total Processed", str(stats["total_processed"]))
                table.add_row(
                    "Average Execution Time", f"{stats['avg_execution_time']:.2f}s"
                )
                table.add_row("Average Wait Time", f"{stats['avg_wait_time']:.2f}s")

                console.print(table)

            await engine.shutdown()
            return 0

        except KeyboardInterrupt:
            console.print("\n[yellow]Monitoring stopped[/yellow]")
            return 0
        except Exception as e:
            console.print(f"[red]Failed to monitor queue: {e}[/red]")
            return 1

    exit_code = asyncio.run(monitor_queue())
    sys.exit(exit_code)


@queue_group.command()
@click.pass_context
def health(ctx):
    """Check health of all queues."""

    async def check_queue_health():
        try:
            from taskflow.core.engine import get_engine

            engine = get_engine()
            await engine.start()

            queue_names = await engine.queue_manager.list_queues()

            if not queue_names:
                console.print("[yellow]No queues to check[/yellow]")
                return 0

            table = Table(title="Queue Health Check")
            table.add_column("Queue", style="cyan")
            table.add_column("Status", style="green")
            table.add_column("Pending", style="yellow")
            table.add_column("Failed", style="red")
            table.add_column("Health Score", style="blue")

            overall_healthy = True

            for queue_name in queue_names:
                try:
                    queue = engine.queue_manager.get_queue(queue_name)
                    health_check = await queue.health_check()
                    stats = await engine.get_queue_stats(queue_name)

                    # Calculate health score (0-100)
                    health_score = 100

                    # Deduct points for high failure rate
                    total_tasks = stats["completed_count"] + stats["failed_count"]
                    if total_tasks > 0:
                        failure_rate = stats["failed_count"] / total_tasks
                        health_score -= int(failure_rate * 50)  # Max 50 point deduction

                    # Deduct points for large backlog
                    if stats["pending_count"] > 100:
                        backlog_penalty = min(25, stats["pending_count"] / 20)
                        health_score -= int(backlog_penalty)

                    # Determine status
                    if health_check["status"] == "healthy" and health_score >= 80:
                        status = "🟢 Healthy"
                    elif health_score >= 60:
                        status = "🟡 Warning"
                        overall_healthy = False
                    else:
                        status = "🔴 Unhealthy"
                        overall_healthy = False

                    table.add_row(
                        queue_name,
                        status,
                        str(stats["pending_count"]),
                        str(stats["failed_count"]),
                        f"{health_score}/100",
                    )

                except Exception as e:
                    table.add_row(queue_name, "🔴 Error", "N/A", "N/A", f"Error: {e}")
                    overall_healthy = False

            console.print(table)

            if overall_healthy:
                console.print("\n[green]All queues are healthy![/green]")
            else:
                console.print("\n[yellow]Some queues need attention[/yellow]")

            await engine.shutdown()
            return 0 if overall_healthy else 1

        except Exception as e:
            console.print(f"[red]Failed to check queue health: {e}[/red]")
            return 1

    exit_code = asyncio.run(check_queue_health())
    sys.exit(exit_code)
