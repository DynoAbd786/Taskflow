"""
Main CLI entry point for TaskFlow administration.
Provides commands for system management, monitoring, and operations.
"""

import asyncio
import sys
from typing import Optional

import click
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from taskflow.cli.commands.config import config_group
from taskflow.cli.commands.monitor import monitor_group
from taskflow.cli.commands.queue import queue_group
from taskflow.cli.commands.server import server_group
from taskflow.cli.commands.task import task_group
from taskflow.cli.commands.worker import worker_group
from taskflow.utils.config import get_config
from taskflow.utils.logging import setup_logging

# Import tasks to ensure they are registered for CLI commands
import taskflow.tasks

console = Console()


@click.group()
@click.option("--config", "-c", help="Configuration file path")
@click.option("--debug", "-d", is_flag=True, help="Enable debug mode")
@click.option("--quiet", "-q", is_flag=True, help="Quiet mode (minimal output)")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json", "yaml"]),
    default="table",
    help="Output format",
)
@click.pass_context
def cli(ctx, config, debug, quiet, output_format):
    """
    TaskFlow - Distributed Task Processing System

    A comprehensive system for managing background jobs, data processing pipelines,
    and scheduled tasks across multiple workers with real-time monitoring.
    """
    # Ensure context object exists
    ctx.ensure_object(dict)

    # Store global options
    ctx.obj["config"] = config
    ctx.obj["debug"] = debug
    ctx.obj["quiet"] = quiet
    ctx.obj["output_format"] = output_format

    # Setup logging
    if debug:
        import os

        os.environ["LOG_LEVEL"] = "DEBUG"

    setup_logging()

    if not quiet:
        console.print(
            Panel.fit(
                "[bold blue]TaskFlow CLI[/bold blue]\n"
                "Distributed Task Processing System",
                border_style="blue",
            )
        )


@cli.command()
@click.pass_context
def version(ctx):
    """Show TaskFlow version information."""
    from taskflow import __author__, __version__

    config = get_config()

    info_table = Table(title="TaskFlow Version Information")
    info_table.add_column("Component", style="cyan")
    info_table.add_column("Value", style="green")

    info_table.add_row("Version", __version__)
    info_table.add_row("Author", __author__)
    info_table.add_row("Environment", config.environment)
    info_table.add_row("Debug Mode", str(config.debug))

    console.print(info_table)


@cli.command()
@click.pass_context
def status(ctx):
    """Show system status overview."""

    async def get_status():
        try:
            from taskflow.core.engine import get_engine
            from taskflow.workers.worker import WorkerManager

            engine = get_engine()
            await engine.start()

            # Get system health
            health = await engine.health_check()

            # Get basic stats
            stats = await engine.get_all_queue_stats()

            # Get worker info
            worker_manager = WorkerManager(engine.queue_manager)
            workers = await worker_manager.get_active_workers()

            # Display status
            status_table = Table(title="TaskFlow System Status")
            status_table.add_column("Component", style="cyan")
            status_table.add_column("Status", style="green")
            status_table.add_column("Details")

            # Overall status
            overall_status = (
                "🟢 Healthy" if health["status"] == "healthy" else "🔴 Unhealthy"
            )
            status_table.add_row(
                "System", overall_status, f"Timestamp: {health['timestamp']}"
            )

            # Queue status
            total_queues = len(stats)
            queue_status = (
                f"🟢 {total_queues} Active" if total_queues > 0 else "🟡 No Queues"
            )
            status_table.add_row(
                "Queues", queue_status, f"Active queues: {total_queues}"
            )

            # Worker status
            total_workers = len(workers)
            worker_status = (
                f"🟢 {total_workers} Active" if total_workers > 0 else "🟡 No Workers"
            )
            status_table.add_row(
                "Workers", worker_status, f"Active workers: {total_workers}"
            )

            # Task summary
            total_pending = sum(s.get("pending_count", 0) for s in stats.values())
            total_running = sum(s.get("running_count", 0) for s in stats.values())
            total_completed = sum(s.get("completed_count", 0) for s in stats.values())

            task_status = f"🟢 Processing" if total_running > 0 else "🟡 Idle"
            task_details = f"Pending: {total_pending}, Running: {total_running}, Completed: {total_completed}"
            status_table.add_row("Tasks", task_status, task_details)

            console.print(status_table)

            await engine.shutdown()

        except Exception as e:
            console.print(f"[red]Error getting status: {e}[/red]")
            return 1

        return 0

    exit_code = asyncio.run(get_status())
    sys.exit(exit_code)


@cli.command()
@click.option("--all", "show_all", is_flag=True, help="Show all configuration values")
@click.option("--section", help="Show specific configuration section")
@click.pass_context
def show_config(ctx, show_all, section):
    """Show current configuration."""
    config = get_config()
    config_dict = config.to_dict()

    if section:
        if section in config_dict:
            section_data = config_dict[section]
            console.print(f"[bold cyan]{section.upper()} Configuration:[/bold cyan]")

            if isinstance(section_data, dict):
                table = Table()
                table.add_column("Setting", style="cyan")
                table.add_column("Value", style="green")

                for key, value in section_data.items():
                    # Hide sensitive values
                    if "password" in key.lower() or "secret" in key.lower():
                        value = "***"
                    table.add_row(key, str(value))

                console.print(table)
            else:
                console.print(f"{section}: {section_data}")
        else:
            console.print(f"[red]Configuration section '{section}' not found[/red]")
    else:
        # Show overview
        table = Table(title="TaskFlow Configuration Overview")
        table.add_column("Section", style="cyan")
        table.add_column("Key Settings", style="green")

        sections = {
            "database": ["host", "port", "name"],
            "redis": ["host", "port", "db"],
            "api": ["host", "port", "workers"],
            "worker": ["concurrency", "max_retries"],
            "monitoring": ["enabled", "prometheus_port"],
        }

        for section_name, keys in sections.items():
            if section_name in config_dict:
                section_data = config_dict[section_name]
                key_values = []
                for key in keys:
                    if key in section_data:
                        value = section_data[key]
                        if "password" in key.lower() or "secret" in key.lower():
                            value = "***"
                        key_values.append(f"{key}: {value}")

                table.add_row(section_name.upper(), "\n".join(key_values))

        console.print(table)

        if show_all:
            console.print(
                "\n[yellow]Use --section <name> to see detailed configuration[/yellow]"
            )


@cli.command()
@click.pass_context
def health(ctx):
    """Perform comprehensive health check."""

    async def health_check():
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Performing health check...", total=None)

            try:
                from taskflow.core.engine import get_engine
                from taskflow.db.database import get_database_manager

                engine = get_engine()
                await engine.start()

                # Engine health
                progress.update(task, description="Checking engine health...")
                engine_health = await engine.health_check()

                # Database health
                progress.update(task, description="Checking database health...")
                db_manager = get_database_manager()
                db_health = await db_manager.health_check()

                # Queue health
                progress.update(task, description="Checking queue health...")
                queue_names = await engine.queue_manager.list_queues()
                queue_healths = {}
                for queue_name in queue_names:
                    queue = engine.queue_manager.get_queue(queue_name)
                    queue_healths[queue_name] = await queue.health_check()

                progress.remove_task(task)

                # Display results
                health_table = Table(title="Health Check Results")
                health_table.add_column("Component", style="cyan")
                health_table.add_column("Status", style="green")
                health_table.add_column("Details")

                # Engine
                engine_status = (
                    "🟢 Healthy"
                    if engine_health["status"] == "healthy"
                    else "🔴 Unhealthy"
                )
                engine_details = f"Queues: {engine_health.get('active_queues', 0)}, Tasks: {engine_health.get('running_tasks', 0)}"
                health_table.add_row("Engine", engine_status, engine_details)

                # Database
                db_status = (
                    "🟢 Connected"
                    if db_health["status"] == "healthy"
                    else "🔴 Disconnected"
                )
                db_details = db_health.get("url", "Unknown")
                health_table.add_row("Database", db_status, db_details)

                # Queues
                healthy_queues = sum(
                    1 for q in queue_healths.values() if q["status"] == "healthy"
                )
                total_queues = len(queue_healths)
                queue_status = (
                    f"🟢 {healthy_queues}/{total_queues} Healthy"
                    if healthy_queues == total_queues
                    else f"🟡 {healthy_queues}/{total_queues} Healthy"
                )
                health_table.add_row(
                    "Queues", queue_status, f"Total queues: {total_queues}"
                )

                console.print(health_table)

                # Show any unhealthy components
                unhealthy = []
                if engine_health["status"] != "healthy":
                    unhealthy.append("Engine")
                if db_health["status"] != "healthy":
                    unhealthy.append("Database")
                if healthy_queues < total_queues:
                    unhealthy.append("Queues")

                if unhealthy:
                    console.print(
                        f"\n[red]Unhealthy components: {', '.join(unhealthy)}[/red]"
                    )
                    return 1
                else:
                    console.print("\n[green]All systems healthy![/green]")
                    return 0

            except Exception as e:
                progress.remove_task(task)
                console.print(f"[red]Health check failed: {e}[/red]")
                return 1
            finally:
                try:
                    await engine.shutdown()
                except:
                    pass

    exit_code = asyncio.run(health_check())
    sys.exit(exit_code)


# Add command groups
cli.add_command(server_group)
cli.add_command(worker_group)
cli.add_command(task_group)
cli.add_command(queue_group)
cli.add_command(monitor_group)
cli.add_command(config_group)


if __name__ == "__main__":
    cli()
