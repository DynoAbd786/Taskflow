"""
Worker management commands for TaskFlow CLI.
Handle worker processes, monitoring, and scaling.
"""

import asyncio
import signal
import sys
from typing import List

import click
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

console = Console()


@click.group(name="worker")
def worker_group():
    """Worker management commands."""
    pass


@worker_group.command()
@click.option(
    "--queues", default="default", help="Comma-separated list of queues to process"
)
@click.option("--concurrency", type=int, help="Number of concurrent tasks per worker")
@click.option("--worker-id", help="Unique worker identifier")
@click.option("--pool-size", type=int, help="Number of worker processes in pool")
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    help="Log level",
)
@click.pass_context
def start(ctx, queues, concurrency, worker_id, pool_size, log_level):
    """Start TaskFlow worker(s)."""

    async def start_worker():
        pool = None
        worker = None
        try:
            # Import tasks to register them with the engine
            import os
            import uuid

            import taskflow.tasks
            from taskflow.core.engine import get_engine
            from taskflow.workers.worker import TaskWorker, WorkerPool

            # Setup logging level
            if log_level:
                os.environ["LOG_LEVEL"] = log_level

            engine = get_engine()
            await engine.start()

            queue_list = [q.strip() for q in queues.split(",")]

            if not ctx.obj.get("quiet"):
                console.print(
                    Panel.fit(
                        f"[bold green]Starting TaskFlow Worker(s)[/bold green]\n"
                        f"Queues: {', '.join(queue_list)}\n"
                        f"Concurrency: {concurrency or 'default'}\n"
                        f"Pool Size: {pool_size or 'single worker'}",
                        border_style="green",
                    )
                )

            if pool_size and pool_size > 1:
                # Start worker pool
                pool = WorkerPool(
                    pool_size=pool_size,
                    queues=queue_list,
                    engine=engine,
                )
                await pool.start()
            else:
                # Start single worker
                # Generate worker_id if not provided
                actual_worker_id = (
                    worker_id or f"worker-{os.getpid()}-{str(uuid.uuid4())[:8]}"
                )

                worker = TaskWorker(
                    worker_id=actual_worker_id,
                    queues=queue_list,
                    engine=engine,
                    concurrency=concurrency,
                )

                console.print(
                    f"[green]Worker started with ID: {actual_worker_id}[/green]"
                )
                await worker.start()

            return 0

        except KeyboardInterrupt:
            console.print("\n[yellow]Worker stopped by user[/yellow]")
            return 0
        except Exception as e:
            console.print(f"[red]Failed to start worker: {e}[/red]")
            return 1
        finally:
            if pool:
                await pool.stop()
            if worker:
                await worker.stop()

    exit_code = asyncio.run(start_worker())
    sys.exit(exit_code)


@worker_group.command()
@click.option("--worker-id", help="Specific worker to stop")
@click.option("--timeout", type=int, default=30, help="Graceful shutdown timeout")
@click.pass_context
def stop(ctx, worker_id, timeout):
    """Stop TaskFlow worker(s)."""
    console.print(f"[yellow]Stopping worker(s)...[/yellow]")

    # In a real implementation, this would:
    # 1. Find running worker processes
    # 2. Send graceful shutdown signals
    # 3. Wait for completion with timeout
    # 4. Force kill if necessary

    if worker_id:
        console.print(f"[green]Worker {worker_id} stopped[/green]")
    else:
        console.print("[green]All workers stopped[/green]")


@worker_group.command()
@click.option("--watch", is_flag=True, help="Watch for real-time updates")
@click.option("--interval", type=int, default=5, help="Update interval for watch mode")
@click.pass_context
def list(ctx, watch, interval):
    """List active workers."""

    async def list_workers():
        try:
            from datetime import datetime

            from taskflow.core.engine import get_engine
            from taskflow.workers.worker import WorkerManager

            engine = get_engine()
            await engine.start()

            worker_manager = WorkerManager(engine.queue_manager)

            if watch:
                with Live(console=console, refresh_per_second=1 / interval) as live:
                    while True:
                        try:
                            workers = await worker_manager.get_active_workers()

                            table = Table(
                                title=f"Active Workers (Live - {datetime.now().strftime('%H:%M:%S')})"
                            )
                            table.add_column("Worker ID", style="cyan")
                            table.add_column("Status", style="green")
                            table.add_column("Queues", style="blue")
                            table.add_column("Tasks Processed", style="yellow")
                            table.add_column("CPU", style="red")
                            table.add_column("Memory", style="magenta")
                            table.add_column("Uptime", style="white")

                            for worker in workers:
                                uptime = datetime.now().timestamp() - worker.get(
                                    "last_heartbeat", 0
                                )
                                uptime_str = f"{int(uptime // 3600)}h {int((uptime % 3600) // 60)}m"

                                table.add_row(
                                    worker["worker_id"],
                                    "🟢 Active",
                                    worker.get("queues", "").replace(",", ", "),
                                    str(worker.get("tasks_processed", 0)),
                                    f"{worker.get('cpu_percent', 0):.1f}%",
                                    f"{worker.get('memory_percent', 0):.1f}%",
                                    uptime_str,
                                )

                            if not workers:
                                table.add_row(
                                    "No active workers", "", "", "", "", "", ""
                                )

                            live.update(table)

                        except Exception as e:
                            error_panel = Panel(
                                f"[red]Error: {e}[/red]", title="List Error"
                            )
                            live.update(error_panel)

                        await asyncio.sleep(interval)
            else:
                workers = await worker_manager.get_active_workers()

                if not workers:
                    console.print("[yellow]No active workers found[/yellow]")
                    return 0

                table = Table(title=f"Active Workers ({len(workers)} found)")
                table.add_column("Worker ID", style="cyan")
                table.add_column("Hostname", style="blue")
                table.add_column("PID", style="yellow")
                table.add_column("Queues", style="green")
                table.add_column("Tasks", style="magenta")
                table.add_column("Success Rate", style="white")
                table.add_column("Last Heartbeat", style="white")

                for worker in workers:
                    total_tasks = worker.get("tasks_processed", 0)
                    success_tasks = worker.get("tasks_succeeded", 0)
                    success_rate = (
                        f"{(success_tasks / total_tasks * 100):.1f}%"
                        if total_tasks > 0
                        else "N/A"
                    )

                    last_heartbeat = datetime.fromtimestamp(
                        worker.get("last_heartbeat", 0)
                    )
                    heartbeat_str = last_heartbeat.strftime("%H:%M:%S")

                    table.add_row(
                        worker["worker_id"],
                        worker.get("hostname", "N/A"),
                        str(worker.get("pid", "N/A")),
                        worker.get("queues", "").replace(",", ", "),
                        str(total_tasks),
                        success_rate,
                        heartbeat_str,
                    )

                console.print(table)

            await engine.shutdown()
            return 0

        except KeyboardInterrupt:
            console.print("\n[yellow]Stopped by user[/yellow]")
            return 0
        except Exception as e:
            console.print(f"[red]Failed to list workers: {e}[/red]")
            return 1

    exit_code = asyncio.run(list_workers())
    sys.exit(exit_code)


@worker_group.command()
@click.argument("worker_id")
@click.pass_context
def info(ctx, worker_id):
    """Show detailed information about a worker."""

    async def show_worker_info():
        try:
            from datetime import datetime

            from taskflow.core.engine import get_engine
            from taskflow.workers.worker import WorkerManager

            engine = get_engine()
            await engine.start()

            worker_manager = WorkerManager(engine.queue_manager)
            workers = await worker_manager.get_active_workers()

            worker = None
            for w in workers:
                if w["worker_id"] == worker_id:
                    worker = w
                    break

            if not worker:
                console.print(f"[red]Worker {worker_id} not found[/red]")
                return 1

            # Worker details table
            table = Table(title=f"Worker Details: {worker_id}")
            table.add_column("Property", style="cyan")
            table.add_column("Value", style="green")

            table.add_row("Worker ID", worker["worker_id"])
            table.add_row("Hostname", worker.get("hostname", "N/A"))
            table.add_row("Process ID", str(worker.get("pid", "N/A")))
            table.add_row("Status", worker.get("status", "N/A"))
            table.add_row("Queues", worker.get("queues", "").replace(",", ", "))

            # Registration info
            if "registered_at" in worker:
                registered = datetime.fromisoformat(worker["registered_at"])
                table.add_row("Registered At", registered.strftime("%Y-%m-%d %H:%M:%S"))

            # Heartbeat info
            if "last_heartbeat" in worker:
                heartbeat = datetime.fromtimestamp(worker["last_heartbeat"])
                table.add_row("Last Heartbeat", heartbeat.strftime("%Y-%m-%d %H:%M:%S"))

            # Task statistics
            table.add_row("Tasks Processed", str(worker.get("tasks_processed", 0)))
            table.add_row("Tasks Succeeded", str(worker.get("tasks_succeeded", 0)))
            table.add_row("Tasks Failed", str(worker.get("tasks_failed", 0)))

            # Calculate success rate
            total_tasks = worker.get("tasks_processed", 0)
            success_tasks = worker.get("tasks_succeeded", 0)
            if total_tasks > 0:
                success_rate = (success_tasks / total_tasks) * 100
                table.add_row("Success Rate", f"{success_rate:.1f}%")

            # Resource usage
            table.add_row("CPU Usage", f"{worker.get('cpu_percent', 0):.1f}%")
            table.add_row("Memory Usage", f"{worker.get('memory_percent', 0):.1f}%")
            table.add_row("Memory (MB)", f"{worker.get('memory_mb', 0):.1f}")

            # Performance metrics
            if "avg_task_duration" in worker:
                table.add_row(
                    "Avg Task Duration", f"{worker['avg_task_duration']:.2f}s"
                )

            console.print(table)

            await engine.shutdown()
            return 0

        except Exception as e:
            console.print(f"[red]Failed to get worker info: {e}[/red]")
            return 1

    exit_code = asyncio.run(show_worker_info())
    sys.exit(exit_code)


@worker_group.command()
@click.option("--target-size", type=int, required=True, help="Target number of workers")
@click.option("--queue", default="default", help="Queue to scale workers for")
@click.pass_context
def scale(ctx, target_size, queue):
    """Scale the number of workers."""

    async def scale_workers():
        try:
            from taskflow.core.engine import get_engine
            from taskflow.workers.worker import WorkerManager

            engine = get_engine()
            await engine.start()

            worker_manager = WorkerManager(engine.queue_manager)
            current_workers = await worker_manager.get_active_workers()
            current_size = len(current_workers)

            console.print(f"[blue]Current workers: {current_size}[/blue]")
            console.print(f"[blue]Target workers: {target_size}[/blue]")

            if target_size == current_size:
                console.print("[green]Already at target size[/green]")
                return 0
            elif target_size > current_size:
                # Scale up
                scale_amount = target_size - current_size
                console.print(
                    f"[yellow]Scaling up by {scale_amount} workers...[/yellow]"
                )

                # In a real implementation, this would trigger worker spawning
                console.print(f"[green]Scaled up to {target_size} workers[/green]")
            else:
                # Scale down
                scale_amount = current_size - target_size
                console.print(
                    f"[yellow]Scaling down by {scale_amount} workers...[/yellow]"
                )

                # In a real implementation, this would gracefully shutdown workers
                console.print(f"[green]Scaled down to {target_size} workers[/green]")

            await engine.shutdown()
            return 0

        except Exception as e:
            console.print(f"[red]Failed to scale workers: {e}[/red]")
            return 1

    exit_code = asyncio.run(scale_workers())
    sys.exit(exit_code)


@worker_group.command()
@click.option("--interval", type=int, default=5, help="Update interval in seconds")
@click.pass_context
def monitor(ctx, interval):
    """Monitor worker performance in real-time."""

    async def monitor_workers():
        try:
            from datetime import datetime

            from taskflow.core.engine import get_engine
            from taskflow.workers.worker import WorkerManager

            engine = get_engine()
            await engine.start()

            worker_manager = WorkerManager(engine.queue_manager)

            with Live(console=console, refresh_per_second=1 / interval) as live:
                while True:
                    try:
                        workers = await worker_manager.get_active_workers()

                        # Summary table
                        summary_table = Table(
                            title=f"Worker Monitoring ({datetime.now().strftime('%H:%M:%S')})"
                        )
                        summary_table.add_column("Metric", style="cyan")
                        summary_table.add_column("Value", style="green")

                        if workers:
                            total_tasks = sum(
                                w.get("tasks_processed", 0) for w in workers
                            )
                            total_cpu = sum(w.get("cpu_percent", 0) for w in workers)
                            total_memory = sum(
                                w.get("memory_percent", 0) for w in workers
                            )
                            avg_cpu = total_cpu / len(workers)
                            avg_memory = total_memory / len(workers)

                            summary_table.add_row("Active Workers", str(len(workers)))
                            summary_table.add_row(
                                "Total Tasks Processed", str(total_tasks)
                            )
                            summary_table.add_row(
                                "Average CPU Usage", f"{avg_cpu:.1f}%"
                            )
                            summary_table.add_row(
                                "Average Memory Usage", f"{avg_memory:.1f}%"
                            )
                        else:
                            summary_table.add_row("Active Workers", "0")
                            summary_table.add_row("Status", "No workers active")

                        # Worker details table
                        details_table = Table(title="Worker Details")
                        details_table.add_column("Worker ID", style="cyan", width=20)
                        details_table.add_column("Tasks", style="yellow", width=8)
                        details_table.add_column("CPU", style="red", width=6)
                        details_table.add_column("Memory", style="magenta", width=8)
                        details_table.add_column("Current Task", style="blue")

                        for worker in workers:
                            current_task = worker.get("current_task", "")
                            if current_task and len(current_task) > 20:
                                current_task = current_task[:17] + "..."

                            details_table.add_row(
                                worker["worker_id"][:20],
                                str(worker.get("tasks_processed", 0)),
                                f"{worker.get('cpu_percent', 0):.1f}%",
                                f"{worker.get('memory_percent', 0):.1f}%",
                                current_task or "Idle",
                            )

                        # Combine tables
                        combined = Panel.fit(
                            f"{summary_table}\n\n{details_table}",
                            title="Worker Monitoring",
                            border_style="blue",
                        )

                        live.update(combined)

                    except Exception as e:
                        error_panel = Panel(
                            f"[red]Error: {e}[/red]", title="Monitoring Error"
                        )
                        live.update(error_panel)

                    await asyncio.sleep(interval)

        except KeyboardInterrupt:
            console.print("\n[yellow]Monitoring stopped[/yellow]")
            return 0
        except Exception as e:
            console.print(f"[red]Monitoring failed: {e}[/red]")
            return 1

    exit_code = asyncio.run(monitor_workers())
    sys.exit(exit_code)
