"""
Monitoring and metrics commands for TaskFlow CLI.
Handle system monitoring, alerts, and performance analysis.
"""

import asyncio
import sys
from datetime import datetime

import click
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

console = Console()


@click.group(name="monitor")
def monitor_group():
    """Monitoring and metrics commands."""
    pass


@monitor_group.command()
@click.option("--interval", type=int, default=5, help="Update interval in seconds")
@click.option("--components", help="Comma-separated list of components to monitor")
@click.pass_context
def dashboard(ctx, interval, components):
    """Live monitoring dashboard."""

    async def show_dashboard():
        try:
            from taskflow.core.engine import get_engine
            from taskflow.monitoring.metrics import get_metrics_manager
            from taskflow.workers.worker import WorkerManager

            engine = get_engine()
            await engine.start()

            worker_manager = WorkerManager(engine.queue_manager)
            metrics_manager = get_metrics_manager()

            with Live(console=console, refresh_per_second=1 / interval) as live:
                while True:
                    try:
                        # Get system overview
                        health = await engine.health_check()
                        all_stats = await engine.get_all_queue_stats()
                        workers = await worker_manager.get_active_workers()
                        dashboard_metrics = (
                            await metrics_manager.get_dashboard_metrics()
                        )

                        # System status table
                        status_table = Table(
                            title=f"System Overview ({datetime.now().strftime('%H:%M:%S')})"
                        )
                        status_table.add_column("Component", style="cyan")
                        status_table.add_column("Status", style="green")
                        status_table.add_column("Details", style="white")

                        # Overall health
                        overall_status = (
                            "🟢 Healthy"
                            if health["status"] == "healthy"
                            else "🔴 Unhealthy"
                        )
                        status_table.add_row(
                            "System",
                            overall_status,
                            f"Uptime: {dashboard_metrics.get('system', {}).get('uptime', 'N/A')}",
                        )

                        # Worker status
                        worker_status = (
                            f"🟢 {len(workers)} Active" if workers else "🟡 No Workers"
                        )
                        avg_cpu = (
                            sum(w.get("cpu_percent", 0) for w in workers) / len(workers)
                            if workers
                            else 0
                        )
                        status_table.add_row(
                            "Workers", worker_status, f"Avg CPU: {avg_cpu:.1f}%"
                        )

                        # Queue status
                        total_pending = sum(
                            s.get("pending_count", 0) for s in all_stats.values()
                        )
                        total_running = sum(
                            s.get("running_count", 0) for s in all_stats.values()
                        )
                        queue_status = (
                            f"🟢 {len(all_stats)} Queues" if all_stats else "🟡 No Queues"
                        )
                        status_table.add_row(
                            "Queues",
                            queue_status,
                            f"Pending: {total_pending}, Running: {total_running}",
                        )

                        # Task throughput
                        total_completed = sum(
                            s.get("completed_count", 0) for s in all_stats.values()
                        )
                        throughput = total_completed / max(1, interval)
                        status_table.add_row(
                            "Throughput",
                            f"{throughput:.2f} tasks/sec",
                            f"Total completed: {total_completed}",
                        )

                        # Resource usage table
                        resource_table = Table(title="Resource Usage")
                        resource_table.add_column("Resource", style="cyan")
                        resource_table.add_column("Usage", style="yellow")
                        resource_table.add_column("Status", style="green")

                        system_stats = dashboard_metrics.get("system", {})
                        cpu_percent = system_stats.get("cpu_percent", 0)
                        memory_percent = system_stats.get("memory_percent", 0)
                        disk_percent = system_stats.get("disk_percent", 0)

                        # CPU status
                        cpu_status = (
                            "🟢 Normal"
                            if cpu_percent < 70
                            else "🟡 High"
                            if cpu_percent < 90
                            else "🔴 Critical"
                        )
                        resource_table.add_row("CPU", f"{cpu_percent:.1f}%", cpu_status)

                        # Memory status
                        mem_status = (
                            "🟢 Normal"
                            if memory_percent < 70
                            else "🟡 High"
                            if memory_percent < 90
                            else "🔴 Critical"
                        )
                        resource_table.add_row(
                            "Memory", f"{memory_percent:.1f}%", mem_status
                        )

                        # Disk status
                        disk_status = (
                            "🟢 Normal"
                            if disk_percent < 80
                            else "🟡 High"
                            if disk_percent < 95
                            else "🔴 Critical"
                        )
                        resource_table.add_row(
                            "Disk", f"{disk_percent:.1f}%", disk_status
                        )

                        # Queue details table
                        queue_table = Table(title="Queue Details")
                        queue_table.add_column("Queue", style="cyan")
                        queue_table.add_column("Pending", style="yellow")
                        queue_table.add_column("Running", style="blue")
                        queue_table.add_column("Completed", style="green")
                        queue_table.add_column("Failed", style="red")
                        queue_table.add_column("Health", style="white")

                        for queue_name, stats in all_stats.items():
                            # Calculate health
                            total_tasks = stats.get("completed_count", 0) + stats.get(
                                "failed_count", 0
                            )
                            failure_rate = (
                                stats.get("failed_count", 0) / max(1, total_tasks)
                            ) * 100

                            if failure_rate < 5:
                                health_status = "🟢 Healthy"
                            elif failure_rate < 15:
                                health_status = "🟡 Warning"
                            else:
                                health_status = "🔴 Unhealthy"

                            queue_table.add_row(
                                queue_name,
                                str(stats.get("pending_count", 0)),
                                str(stats.get("running_count", 0)),
                                str(stats.get("completed_count", 0)),
                                str(stats.get("failed_count", 0)),
                                health_status,
                            )

                        # Combine all tables
                        combined = Panel.fit(
                            f"{status_table}\n\n{resource_table}\n\n{queue_table}",
                            title="TaskFlow Monitoring Dashboard",
                            border_style="blue",
                        )

                        live.update(combined)

                    except Exception as e:
                        error_panel = Panel(
                            f"[red]Error: {e}[/red]", title="Dashboard Error"
                        )
                        live.update(error_panel)

                    await asyncio.sleep(interval)

        except KeyboardInterrupt:
            console.print("\n[yellow]Dashboard stopped[/yellow]")
            return 0
        except Exception as e:
            console.print(f"[red]Dashboard failed: {e}[/red]")
            return 1

    exit_code = asyncio.run(show_dashboard())
    sys.exit(exit_code)


@monitor_group.command()
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["prometheus", "json"]),
    default="prometheus",
    help="Output format",
)
@click.pass_context
def metrics(ctx, output_format):
    """Export current metrics."""

    async def export_metrics():
        try:
            from taskflow.monitoring.metrics import get_metrics_manager

            metrics_manager = get_metrics_manager()

            if output_format == "prometheus":
                metrics_text = metrics_manager.get_prometheus_metrics()
                console.print(metrics_text)
            elif output_format == "json":
                dashboard_metrics = await metrics_manager.get_dashboard_metrics()
                import json

                console.print(json.dumps(dashboard_metrics, indent=2))

            return 0

        except Exception as e:
            console.print(f"[red]Failed to export metrics: {e}[/red]")
            return 1

    exit_code = asyncio.run(export_metrics())
    sys.exit(exit_code)


@monitor_group.command()
@click.option(
    "--severity",
    type=click.Choice(["low", "medium", "high", "critical"]),
    help="Filter by severity",
)
@click.option(
    "--acknowledged/--unacknowledged",
    default=None,
    help="Filter by acknowledgment status",
)
@click.option("--limit", type=int, default=20, help="Number of alerts to show")
@click.pass_context
def alerts(ctx, severity, acknowledged, limit):
    """Show active alerts."""

    async def show_alerts():
        try:
            from taskflow.monitoring.alerts import get_alert_manager

            alert_manager = get_alert_manager()
            active_alerts = alert_manager.get_active_alerts()

            # Filter alerts
            filtered_alerts = active_alerts
            if severity:
                filtered_alerts = [
                    a for a in filtered_alerts if a.severity.value == severity
                ]

            if acknowledged is not None:
                if acknowledged:
                    filtered_alerts = [
                        a for a in filtered_alerts if a.acknowledged_at is not None
                    ]
                else:
                    filtered_alerts = [
                        a for a in filtered_alerts if a.acknowledged_at is None
                    ]

            # Limit results
            filtered_alerts = filtered_alerts[:limit]

            if not filtered_alerts:
                console.print("[green]No alerts found[/green]")
                return 0

            table = Table(title=f"Active Alerts ({len(filtered_alerts)} found)")
            table.add_column("Severity", style="red")
            table.add_column("Rule", style="cyan")
            table.add_column("Status", style="yellow")
            table.add_column("Message", style="white")
            table.add_column("Fired At", style="blue")
            table.add_column("Acknowledged", style="green")

            for alert in filtered_alerts:
                severity_emoji = {
                    "low": "🟢",
                    "medium": "🟡",
                    "high": "🟠",
                    "critical": "🔴",
                }.get(alert.severity.value, "❓")

                status_emoji = {
                    "firing": "🚨",
                    "acknowledged": "✅",
                    "silenced": "🔇",
                    "resolved": "✅",
                }.get(alert.status.value, "❓")

                acknowledged_str = "Yes" if alert.acknowledged_at else "No"
                if alert.acknowledged_by:
                    acknowledged_str += f" ({alert.acknowledged_by})"

                table.add_row(
                    f"{severity_emoji} {alert.severity.value.upper()}",
                    alert.rule_name,
                    f"{status_emoji} {alert.status.value.upper()}",
                    alert.message[:50] + "..."
                    if len(alert.message) > 50
                    else alert.message,
                    alert.fired_at.strftime("%Y-%m-%d %H:%M:%S"),
                    acknowledged_str,
                )

            console.print(table)

            # Show summary
            summary = alert_manager.get_alert_summary()
            summary_table = Table(title="Alert Summary")
            summary_table.add_column("Status", style="cyan")
            summary_table.add_column("Count", style="green")

            summary_table.add_row("Total Alerts", str(summary["total_alerts"]))
            summary_table.add_row("Firing", str(summary["firing"]))
            summary_table.add_row("Acknowledged", str(summary["acknowledged"]))
            summary_table.add_row("Silenced", str(summary["silenced"]))

            console.print("\n")
            console.print(summary_table)

            return 0

        except Exception as e:
            console.print(f"[red]Failed to show alerts: {e}[/red]")
            return 1

    exit_code = asyncio.run(show_alerts())
    sys.exit(exit_code)


@monitor_group.command()
@click.argument("alert_id")
@click.option("--user", default="cli-user", help="User acknowledging the alert")
@click.pass_context
def ack(ctx, alert_id, user):
    """Acknowledge an alert."""

    async def acknowledge_alert():
        try:
            from taskflow.monitoring.alerts import get_alert_manager

            alert_manager = get_alert_manager()
            success = await alert_manager.acknowledge_alert(alert_id, user)

            if success:
                console.print(f"[green]Alert {alert_id} acknowledged by {user}[/green]")
            else:
                console.print(f"[red]Failed to acknowledge alert {alert_id}[/red]")
                return 1

            return 0

        except Exception as e:
            console.print(f"[red]Failed to acknowledge alert: {e}[/red]")
            return 1

    exit_code = asyncio.run(acknowledge_alert())
    sys.exit(exit_code)


@monitor_group.command()
@click.option("--days", type=int, default=7, help="Number of days to analyze")
@click.pass_context
def performance(ctx, days):
    """Show performance analysis."""

    async def show_performance():
        try:
            from taskflow.core.engine import get_engine
            from taskflow.monitoring.metrics import get_metrics_manager

            engine = get_engine()
            await engine.start()

            metrics_manager = get_metrics_manager()
            performance_stats = (
                await metrics_manager.performance.get_performance_stats()
            )

            # Performance overview
            table = Table(title=f"Performance Analysis (Last {days} days)")
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="green")
            table.add_column("Status", style="yellow")

            if performance_stats:
                avg_duration = performance_stats.get("avg_duration", 0)
                max_duration = performance_stats.get("max_duration", 0)
                min_duration = performance_stats.get("min_duration", 0)
                total_operations = performance_stats.get("count", 0)

                # Duration analysis
                duration_status = (
                    "🟢 Good"
                    if avg_duration < 1.0
                    else "🟡 Moderate"
                    if avg_duration < 5.0
                    else "🔴 Slow"
                )
                table.add_row(
                    "Average Duration", f"{avg_duration:.3f}s", duration_status
                )
                table.add_row("Max Duration", f"{max_duration:.3f}s", "")
                table.add_row("Min Duration", f"{min_duration:.3f}s", "")
                table.add_row("Total Operations", str(total_operations), "")

                # Throughput
                throughput = total_operations / (days * 24 * 3600) if days > 0 else 0
                throughput_status = (
                    "🟢 High"
                    if throughput > 10
                    else "🟡 Medium"
                    if throughput > 1
                    else "🔴 Low"
                )
                table.add_row(
                    "Avg Throughput", f"{throughput:.2f} ops/sec", throughput_status
                )
            else:
                table.add_row("No Data", "No performance data available", "🟡 N/A")

            console.print(table)

            # System resource trends
            system_stats = await metrics_manager.get_dashboard_metrics()
            system_data = system_stats.get("system", {})
            history = system_stats.get("history", {})

            if history:
                resource_table = Table(title="Resource Usage Trends")
                resource_table.add_column("Resource", style="cyan")
                resource_table.add_column("Current", style="green")
                resource_table.add_column("Trend", style="yellow")

                cpu_history = history.get("cpu_history", [])
                memory_history = history.get("memory_history", [])

                if cpu_history:
                    current_cpu = cpu_history[-1] if cpu_history else 0
                    avg_cpu = sum(cpu_history) / len(cpu_history)
                    cpu_trend = (
                        "📈 Rising"
                        if current_cpu > avg_cpu * 1.1
                        else "📉 Falling"
                        if current_cpu < avg_cpu * 0.9
                        else "➡️ Stable"
                    )
                    resource_table.add_row(
                        "CPU Usage", f"{current_cpu:.1f}%", cpu_trend
                    )

                if memory_history:
                    current_memory = memory_history[-1] if memory_history else 0
                    avg_memory = sum(memory_history) / len(memory_history)
                    memory_trend = (
                        "📈 Rising"
                        if current_memory > avg_memory * 1.1
                        else "📉 Falling"
                        if current_memory < avg_memory * 0.9
                        else "➡️ Stable"
                    )
                    resource_table.add_row(
                        "Memory Usage", f"{current_memory:.1f}%", memory_trend
                    )

                console.print("\n")
                console.print(resource_table)

            await engine.shutdown()
            return 0

        except Exception as e:
            console.print(f"[red]Failed to show performance analysis: {e}[/red]")
            return 1

    exit_code = asyncio.run(show_performance())
    sys.exit(exit_code)


@monitor_group.command()
@click.option("--output", type=click.Path(), help="Output file for the report")
@click.option(
    "--format",
    "report_format",
    type=click.Choice(["text", "json", "html"]),
    default="text",
    help="Report format",
)
@click.pass_context
def report(ctx, output, report_format):
    """Generate comprehensive system report."""

    async def generate_report():
        try:
            import json

            from taskflow.core.engine import get_engine
            from taskflow.monitoring.alerts import get_alert_manager
            from taskflow.monitoring.metrics import get_metrics_manager
            from taskflow.workers.worker import WorkerManager

            engine = get_engine()
            await engine.start()

            # Gather all data
            health = await engine.health_check()
            all_stats = await engine.get_all_queue_stats()
            worker_manager = WorkerManager(engine.queue_manager)
            workers = await worker_manager.get_active_workers()
            metrics_manager = get_metrics_manager()
            dashboard_metrics = await metrics_manager.get_dashboard_metrics()
            alert_manager = get_alert_manager()
            alerts = alert_manager.get_active_alerts()
            alert_summary = alert_manager.get_alert_summary()

            # Generate report
            report_data = {
                "timestamp": datetime.now().isoformat(),
                "system_health": health,
                "queue_statistics": all_stats,
                "worker_information": workers,
                "system_metrics": dashboard_metrics,
                "active_alerts": [
                    {
                        "id": alert.id,
                        "rule_name": alert.rule_name,
                        "severity": alert.severity.value,
                        "status": alert.status.value,
                        "message": alert.message,
                        "fired_at": alert.fired_at.isoformat(),
                    }
                    for alert in alerts
                ],
                "alert_summary": alert_summary,
            }

            if report_format == "json":
                report_content = json.dumps(report_data, indent=2)
            else:
                # Generate text report
                report_lines = [
                    "TaskFlow System Report",
                    "=" * 50,
                    f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    "",
                    "SYSTEM HEALTH",
                    "-" * 20,
                    f"Status: {health['status']}",
                    f"Active Queues: {health.get('active_queues', 0)}",
                    f"Running Tasks: {health.get('running_tasks', 0)}",
                    "",
                    "QUEUE STATISTICS",
                    "-" * 20,
                ]

                for queue_name, stats in all_stats.items():
                    report_lines.extend(
                        [
                            f"Queue: {queue_name}",
                            f"  Pending: {stats.get('pending_count', 0)}",
                            f"  Running: {stats.get('running_count', 0)}",
                            f"  Completed: {stats.get('completed_count', 0)}",
                            f"  Failed: {stats.get('failed_count', 0)}",
                            f"  Avg Execution Time: {stats.get('avg_execution_time', 0):.2f}s",
                            "",
                        ]
                    )

                report_lines.extend(
                    [
                        "WORKER INFORMATION",
                        "-" * 20,
                        f"Active Workers: {len(workers)}",
                        "",
                    ]
                )

                for worker in workers:
                    report_lines.extend(
                        [
                            f"Worker: {worker['worker_id']}",
                            f"  Tasks Processed: {worker.get('tasks_processed', 0)}",
                            f"  CPU Usage: {worker.get('cpu_percent', 0):.1f}%",
                            f"  Memory Usage: {worker.get('memory_percent', 0):.1f}%",
                            "",
                        ]
                    )

                report_lines.extend(
                    [
                        "ALERT SUMMARY",
                        "-" * 20,
                        f"Total Alerts: {alert_summary['total_alerts']}",
                        f"Firing: {alert_summary['firing']}",
                        f"Acknowledged: {alert_summary['acknowledged']}",
                        "",
                    ]
                )

                report_content = "\n".join(report_lines)

            if output:
                with open(output, "w") as f:
                    f.write(report_content)
                console.print(f"[green]Report saved to {output}[/green]")
            else:
                console.print(report_content)

            await engine.shutdown()
            return 0

        except Exception as e:
            console.print(f"[red]Failed to generate report: {e}[/red]")
            return 1

    exit_code = asyncio.run(generate_report())
    sys.exit(exit_code)
