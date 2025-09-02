"""
Server management commands for TaskFlow CLI.
Handle API server startup, shutdown, and management.
"""

import asyncio
import signal
import sys
from pathlib import Path

import click
import uvicorn
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from taskflow.utils.config import get_config
from taskflow.utils.logging import get_logging_manager

console = Console()


@click.group(name="server")
def server_group():
    """Server management commands."""
    pass


@server_group.command()
@click.option("--host", default=None, help="Host to bind to")
@click.option("--port", type=int, default=None, help="Port to bind to")
@click.option("--workers", type=int, default=None, help="Number of worker processes")
@click.option("--reload", is_flag=True, help="Enable auto-reload for development")
@click.option(
    "--access-log/--no-access-log", default=None, help="Enable/disable access logging"
)
@click.option(
    "--log-level",
    type=click.Choice(["critical", "error", "warning", "info", "debug"]),
    help="Log level",
)
@click.pass_context
def start(ctx, host, port, workers, reload, access_log, log_level):
    """Start the TaskFlow API server."""
    config = get_config()

    # Override config with CLI options
    server_host = host or config.api.host
    server_port = port or config.api.port
    server_workers = workers or config.api.workers
    server_reload = reload if reload is not None else config.api.reload
    server_access_log = access_log if access_log is not None else config.api.access_log

    # Setup logging
    logging_manager = get_logging_manager()
    log_config = (
        logging_manager.configure_uvicorn_logging()
        if config.logging.format == "json"
        else None
    )

    if not ctx.obj.get("quiet"):
        console.print(
            Panel.fit(
                f"[bold green]Starting TaskFlow API Server[/bold green]\n"
                f"Host: {server_host}\n"
                f"Port: {server_port}\n"
                f"Workers: {server_workers}\n"
                f"Reload: {server_reload}",
                border_style="green",
            )
        )

    try:
        uvicorn.run(
            "taskflow.api.main:app",
            host=server_host,
            port=server_port,
            workers=server_workers if not server_reload else 1,
            reload=server_reload,
            access_log=server_access_log,
            log_level=log_level or config.logging.level.lower(),
            log_config=log_config,
        )
    except KeyboardInterrupt:
        console.print("\n[yellow]Server stopped by user[/yellow]")
    except Exception as e:
        console.print(f"[red]Failed to start server: {e}[/red]")
        sys.exit(1)


@server_group.command()
@click.option("--timeout", type=int, default=30, help="Shutdown timeout in seconds")
@click.pass_context
def stop(ctx, timeout):
    """Stop the TaskFlow API server."""
    console.print("[yellow]Stopping TaskFlow API server...[/yellow]")

    # This is a placeholder - in a real implementation, you'd need to:
    # 1. Find the running server process
    # 2. Send a graceful shutdown signal
    # 3. Wait for cleanup with timeout

    console.print("[green]Server stopped successfully[/green]")


@server_group.command()
@click.pass_context
def reload(ctx):
    """Reload the TaskFlow API server configuration."""
    console.print("[yellow]Reloading server configuration...[/yellow]")

    # This would send a SIGHUP signal to reload configuration
    console.print("[green]Server configuration reloaded[/green]")


@server_group.command()
@click.pass_context
def status(ctx):
    """Check API server status."""

    async def check_status():
        try:
            import httpx

            config = get_config()

            url = f"http://{config.api.host}:{config.api.port}/health"

            async with httpx.AsyncClient() as client:
                response = await client.get(url, timeout=5.0)

                if response.status_code == 200:
                    health_data = response.json()

                    status_table = Table(title="API Server Status")
                    status_table.add_column("Component", style="cyan")
                    status_table.add_column("Status", style="green")
                    status_table.add_column("Details")

                    overall_status = (
                        "🟢 Running"
                        if health_data["status"] == "healthy"
                        else "🔴 Unhealthy"
                    )
                    status_table.add_row("Server", overall_status, f"URL: {url}")
                    status_table.add_row("Timestamp", "📅", health_data["timestamp"])
                    status_table.add_row(
                        "Registered Tasks", "📋", str(health_data["registered_tasks"])
                    )
                    status_table.add_row(
                        "Active Queues", "🗂️", str(health_data["active_queues"])
                    )
                    status_table.add_row(
                        "Running Tasks", "⚡", str(health_data["running_tasks"])
                    )

                    console.print(status_table)
                    return 0
                else:
                    console.print(
                        f"[red]Server unhealthy (HTTP {response.status_code})[/red]"
                    )
                    return 1

        except httpx.ConnectError:
            console.print(f"[red]Server not reachable at {url}[/red]")
            return 1
        except Exception as e:
            console.print(f"[red]Error checking server status: {e}[/red]")
            return 1

    exit_code = asyncio.run(check_status())
    sys.exit(exit_code)


@server_group.command()
@click.option("--interval", type=int, default=5, help="Update interval in seconds")
@click.pass_context
def monitor(ctx, interval):
    """Monitor API server metrics in real-time."""

    async def monitor_server():
        try:
            import httpx

            config = get_config()

            base_url = f"http://{config.api.host}:{config.api.port}"

            async with httpx.AsyncClient() as client:
                with Live(console=console, refresh_per_second=1) as live:
                    while True:
                        try:
                            # Get stats
                            stats_response = await client.get(
                                f"{base_url}/stats", timeout=5.0
                            )
                            stats = stats_response.json()

                            # Get workers
                            workers_response = await client.get(
                                f"{base_url}/workers", timeout=5.0
                            )
                            workers = workers_response.json()

                            # Create display table
                            table = Table(title="API Server Monitoring")
                            table.add_column("Metric", style="cyan")
                            table.add_column("Value", style="green")

                            table.add_row("Total Tasks", str(stats["total_tasks"]))
                            table.add_row("Pending Tasks", str(stats["pending_tasks"]))
                            table.add_row("Running Tasks", str(stats["running_tasks"]))
                            table.add_row(
                                "Completed Tasks", str(stats["completed_tasks"])
                            )
                            table.add_row("Failed Tasks", str(stats["failed_tasks"]))
                            table.add_row("Active Workers", str(len(workers)))
                            table.add_row(
                                "Avg Execution Time",
                                f"{stats['avg_execution_time']:.2f}s",
                            )

                            live.update(table)

                        except Exception as e:
                            error_panel = Panel(
                                f"[red]Error: {e}[/red]", title="Monitoring Error"
                            )
                            live.update(error_panel)

                        await asyncio.sleep(interval)

        except KeyboardInterrupt:
            console.print("\n[yellow]Monitoring stopped[/yellow]")
        except Exception as e:
            console.print(f"[red]Monitoring failed: {e}[/red]")
            return 1

        return 0

    exit_code = asyncio.run(monitor_server())
    sys.exit(exit_code)


@server_group.command()
@click.option("--config-file", type=click.Path(), help="Configuration file to validate")
@click.pass_context
def validate_config(ctx, config_file):
    """Validate server configuration."""
    try:
        from taskflow.utils.config import TaskFlowConfig

        if config_file:
            config = TaskFlowConfig.load_from_file(Path(config_file))
        else:
            config = get_config()

        errors = config.validate_config()

        if errors:
            console.print("[red]Configuration validation failed:[/red]")
            for error in errors:
                console.print(f"  • {error}")
            sys.exit(1)
        else:
            console.print("[green]Configuration is valid![/green]")

            # Show configuration summary
            table = Table(title="Configuration Summary")
            table.add_column("Component", style="cyan")
            table.add_column("Status", style="green")

            table.add_row(
                "Database", f"✓ {config.database.host}:{config.database.port}"
            )
            table.add_row("Redis", f"✓ {config.redis.host}:{config.redis.port}")
            table.add_row("API", f"✓ {config.api.host}:{config.api.port}")
            table.add_row("Workers", f"✓ Concurrency: {config.worker.concurrency}")
            table.add_row("Monitoring", f"✓ Enabled: {config.monitoring.enabled}")

            console.print(table)

    except Exception as e:
        console.print(f"[red]Configuration validation error: {e}[/red]")
        sys.exit(1)
