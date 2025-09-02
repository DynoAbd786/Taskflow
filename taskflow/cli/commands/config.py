"""
Configuration management commands for TaskFlow CLI.
Handle configuration viewing, editing, and validation.
"""

import sys
from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


@click.group(name="config")
def config_group():
    """Configuration management commands."""
    pass


@config_group.command()
@click.option("--section", help="Show specific configuration section")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json", "yaml"]),
    default="table",
    help="Output format",
)
@click.pass_context
def show(ctx, section, output_format):
    """Show current configuration."""
    try:
        import json

        from taskflow.utils.config import get_config

        config = get_config()
        config_dict = config.to_dict()

        if section:
            if section in config_dict:
                section_data = config_dict[section]

                if output_format == "json":
                    console.print(json.dumps(section_data, indent=2))
                elif output_format == "yaml":
                    try:
                        import yaml

                        console.print(yaml.dump(section_data, default_flow_style=False))
                    except ImportError:
                        console.print(
                            "[red]PyYAML not installed. Use --format json instead.[/red]"
                        )
                        return 1
                else:
                    # Table format
                    console.print(
                        f"[bold cyan]{section.upper()} Configuration:[/bold cyan]"
                    )

                    if isinstance(section_data, dict):
                        table = Table()
                        table.add_column("Setting", style="cyan")
                        table.add_column("Value", style="green")
                        table.add_column("Type", style="yellow")

                        for key, value in section_data.items():
                            # Hide sensitive values
                            display_value = str(value)
                            if (
                                "password" in key.lower()
                                or "secret" in key.lower()
                                or "key" in key.lower()
                            ):
                                display_value = "***"

                            value_type = type(value).__name__
                            table.add_row(key, display_value, value_type)

                        console.print(table)
                    else:
                        console.print(f"{section}: {section_data}")
            else:
                console.print(f"[red]Configuration section '{section}' not found[/red]")
                available_sections = list(config_dict.keys())
                console.print(f"Available sections: {', '.join(available_sections)}")
                return 1
        else:
            # Show all configuration
            if output_format == "json":
                # Hide sensitive data
                safe_config = {}
                for section_name, section_data in config_dict.items():
                    if isinstance(section_data, dict):
                        safe_section = {}
                        for key, value in section_data.items():
                            if (
                                "password" in key.lower()
                                or "secret" in key.lower()
                                or "key" in key.lower()
                            ):
                                safe_section[key] = "***"
                            else:
                                safe_section[key] = value
                        safe_config[section_name] = safe_section
                    else:
                        safe_config[section_name] = section_data

                console.print(json.dumps(safe_config, indent=2))
            elif output_format == "yaml":
                try:
                    import yaml

                    # Hide sensitive data (same logic as JSON)
                    safe_config = {}
                    for section_name, section_data in config_dict.items():
                        if isinstance(section_data, dict):
                            safe_section = {}
                            for key, value in section_data.items():
                                if (
                                    "password" in key.lower()
                                    or "secret" in key.lower()
                                    or "key" in key.lower()
                                ):
                                    safe_section[key] = "***"
                                else:
                                    safe_section[key] = value
                            safe_config[section_name] = safe_section
                        else:
                            safe_config[section_name] = section_data

                    console.print(yaml.dump(safe_config, default_flow_style=False))
                except ImportError:
                    console.print(
                        "[red]PyYAML not installed. Use --format json instead.[/red]"
                    )
                    return 1
            else:
                # Table format overview
                table = Table(title="TaskFlow Configuration Overview")
                table.add_column("Section", style="cyan")
                table.add_column("Key Settings", style="green")
                table.add_column("Status", style="yellow")

                for section_name, section_data in config_dict.items():
                    if isinstance(section_data, dict):
                        key_count = len(section_data)
                        status = "✓ Configured" if key_count > 0 else "⚠ Empty"

                        # Show a few key settings
                        key_samples = list(section_data.keys())[:3]
                        key_display = ", ".join(key_samples)
                        if len(section_data) > 3:
                            key_display += f" (+ {len(section_data) - 3} more)"

                        table.add_row(section_name.upper(), key_display, status)
                    else:
                        table.add_row(section_name.upper(), str(section_data), "✓ Set")

                console.print(table)
                console.print(
                    "\n[yellow]Use --section <name> to see detailed configuration[/yellow]"
                )

        return 0

    except Exception as e:
        console.print(f"[red]Failed to show configuration: {e}[/red]")
        return 1


@config_group.command()
@click.option(
    "--config-file", type=click.Path(exists=True), help="Configuration file to validate"
)
@click.pass_context
def validate(ctx, config_file):
    """Validate configuration."""
    try:
        from taskflow.utils.config import TaskFlowConfig

        if config_file:
            config = TaskFlowConfig.load_from_file(Path(config_file))
            console.print(f"[blue]Validating configuration from: {config_file}[/blue]")
        else:
            from taskflow.utils.config import get_config

            config = get_config()
            console.print("[blue]Validating current configuration[/blue]")

        errors = config.validate_config()

        if errors:
            console.print("[red]Configuration validation failed:[/red]")
            table = Table()
            table.add_column("Error", style="red")

            for error in errors:
                table.add_row(f"• {error}")

            console.print(table)
            return 1
        else:
            console.print("[green]✓ Configuration is valid![/green]")

            # Show validation summary
            table = Table(title="Configuration Validation Summary")
            table.add_column("Component", style="cyan")
            table.add_column("Status", style="green")
            table.add_column("Details", style="white")

            table.add_row(
                "Database", "✓ Valid", f"{config.database.host}:{config.database.port}"
            )
            table.add_row(
                "Redis", "✓ Valid", f"{config.redis.host}:{config.redis.port}"
            )
            table.add_row("API", "✓ Valid", f"{config.api.host}:{config.api.port}")
            table.add_row(
                "Workers", "✓ Valid", f"Concurrency: {config.worker.concurrency}"
            )
            table.add_row(
                "Monitoring", "✓ Valid", f"Enabled: {config.monitoring.enabled}"
            )
            table.add_row("Logging", "✓ Valid", f"Level: {config.logging.level}")

            console.print(table)
            return 0

    except Exception as e:
        console.print(f"[red]Configuration validation error: {e}[/red]")
        return 1


@config_group.command()
@click.option("--output", type=click.Path(), help="Output file path")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["json", "yaml", "env"]),
    default="yaml",
    help="Output format",
)
@click.option(
    "--template",
    type=click.Choice(["minimal", "complete", "production"]),
    default="complete",
    help="Configuration template",
)
@click.pass_context
def generate(ctx, output, output_format, template):
    """Generate configuration template."""
    try:
        import json

        from taskflow.utils.config import TaskFlowConfig

        # Create default config
        config = TaskFlowConfig()

        if template == "minimal":
            # Minimal configuration
            config_data = {
                "database": {
                    "host": "localhost",
                    "port": 5432,
                    "name": "taskflow",
                    "user": "taskflow",
                    "password": "your_password_here",
                },
                "redis": {"host": "localhost", "port": 6379, "db": 0},
                "api": {"host": "0.0.0.0", "port": 8000},
                "security": {"secret_key": "your_secret_key_here"},
            }
        elif template == "production":
            # Production-ready configuration
            config_data = {
                "environment": "production",
                "debug": False,
                "database": {
                    "host": "db.example.com",
                    "port": 5432,
                    "name": "taskflow_prod",
                    "user": "taskflow_user",
                    "password": "secure_password_here",
                    "pool_size": 20,
                    "max_overflow": 40,
                },
                "redis": {
                    "host": "redis.example.com",
                    "port": 6379,
                    "db": 0,
                    "password": "redis_password_here",
                    "max_connections": 50,
                },
                "api": {
                    "host": "0.0.0.0",
                    "port": 8000,
                    "workers": 4,
                    "cors_origins": ["https://yourdomain.com"],
                },
                "worker": {"concurrency": 8, "max_retries": 3, "task_timeout": 600},
                "security": {
                    "secret_key": "your_very_secure_secret_key_here",
                    "access_token_expire_minutes": 60,
                },
                "logging": {
                    "level": "INFO",
                    "format": "json",
                    "file": "/var/log/taskflow/taskflow.log",
                    "max_size": 100,
                    "backup_count": 10,
                },
                "monitoring": {
                    "enabled": True,
                    "prometheus_port": 8001,
                    "collect_system_metrics": True,
                },
            }
        else:
            # Complete configuration
            config_data = config.to_dict()

            # Add example values for sensitive fields
            if "security" in config_data and "secret_key" in config_data["security"]:
                config_data["security"][
                    "secret_key"
                ] = "your_secret_key_here_change_this"

            if "database" in config_data:
                config_data["database"]["password"] = "your_database_password"

            if "redis" in config_data and "password" in config_data["redis"]:
                config_data["redis"]["password"] = "your_redis_password"

        # Generate output
        if output_format == "json":
            content = json.dumps(config_data, indent=2)
        elif output_format == "yaml":
            try:
                import yaml

                content = yaml.dump(config_data, default_flow_style=False, indent=2)
            except ImportError:
                console.print(
                    "[red]PyYAML not installed. Use --format json instead.[/red]"
                )
                return 1
        elif output_format == "env":
            # Generate environment variables
            env_lines = []

            def flatten_dict(d, prefix=""):
                for key, value in d.items():
                    env_key = f"{prefix}{key}".upper()

                    if isinstance(value, dict):
                        flatten_dict(value, f"{env_key}_")
                    else:
                        env_lines.append(f"{env_key}={value}")

            flatten_dict(config_data, "TASKFLOW_")
            content = "\n".join(env_lines)

        # Add header comment
        header = f"""# TaskFlow Configuration ({template} template)
# Generated by TaskFlow CLI
# 
# This is a {template} configuration template for TaskFlow.
# Please review and modify the values according to your environment.
#
# For more information, see: https://taskflow.readthedocs.io/configuration/

"""

        content = header + content

        if output:
            with open(output, "w") as f:
                f.write(content)
            console.print(f"[green]Configuration template saved to: {output}[/green]")
        else:
            console.print(content)

        return 0

    except Exception as e:
        console.print(f"[red]Failed to generate configuration: {e}[/red]")
        return 1


@config_group.command()
@click.argument("key")
@click.argument("value")
@click.option("--config-file", type=click.Path(), help="Configuration file to update")
@click.pass_context
def set(ctx, key, value, config_file):
    """Set a configuration value."""
    # This would be a more complex implementation that updates configuration
    # For now, we'll show what would be set

    console.print(f"[yellow]Setting configuration value:[/yellow]")
    console.print(f"Key: [cyan]{key}[/cyan]")
    console.print(f"Value: [green]{value}[/green]")

    if config_file:
        console.print(f"Target file: [blue]{config_file}[/blue]")
    else:
        console.print("[blue]Target: Environment variables[/blue]")

    console.print("\n[yellow]Note: This command is not yet implemented.[/yellow]")
    console.print(
        "[yellow]To set configuration values, edit your configuration file or set environment variables.[/yellow]"
    )

    return 0


@config_group.command()
@click.option("--env-file", type=click.Path(), help="Environment file to check")
@click.pass_context
def check_env(ctx, env_file):
    """Check environment variables."""
    try:
        import os

        # Environment variables that TaskFlow looks for
        taskflow_vars = [
            "ENVIRONMENT",
            "DEBUG",
            "TIMEZONE",
            "DB_HOST",
            "DB_PORT",
            "DB_NAME",
            "DB_USER",
            "DB_PASSWORD",
            "REDIS_HOST",
            "REDIS_PORT",
            "REDIS_DB",
            "REDIS_PASSWORD",
            "API_HOST",
            "API_PORT",
            "API_WORKERS",
            "WORKER_CONCURRENCY",
            "WORKER_MAX_RETRIES",
            "WORKER_TASK_TIMEOUT",
            "SECRET_KEY",
            "JWT_ALGORITHM",
            "ACCESS_TOKEN_EXPIRE_MINUTES",
            "LOG_LEVEL",
            "LOG_FORMAT",
            "LOG_FILE",
            "MONITORING_ENABLED",
            "PROMETHEUS_PORT",
        ]

        if env_file:
            # Load environment file
            env_vars = {}
            try:
                with open(env_file, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            key, value = line.split("=", 1)
                            env_vars[key] = value
                console.print(f"[blue]Checking environment file: {env_file}[/blue]")
            except FileNotFoundError:
                console.print(f"[red]Environment file not found: {env_file}[/red]")
                return 1
        else:
            env_vars = dict(os.environ)
            console.print("[blue]Checking current environment variables[/blue]")

        table = Table(title="Environment Variables Check")
        table.add_column("Variable", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Value", style="yellow")

        for var in taskflow_vars:
            if var in env_vars:
                value = env_vars[var]
                # Hide sensitive values
                if (
                    "password" in var.lower()
                    or "secret" in var.lower()
                    or "key" in var.lower()
                ):
                    display_value = "***"
                else:
                    display_value = value

                table.add_row(var, "✓ Set", display_value)
            else:
                table.add_row(var, "⚠ Not Set", "")

        console.print(table)

        # Count set variables
        set_count = sum(1 for var in taskflow_vars if var in env_vars)
        total_count = len(taskflow_vars)

        console.print(
            f"\n[blue]Summary: {set_count}/{total_count} variables set[/blue]"
        )

        if set_count < total_count:
            console.print(
                "[yellow]Some environment variables are not set. This may be normal if you're using configuration files.[/yellow]"
            )

        return 0

    except Exception as e:
        console.print(f"[red]Failed to check environment variables: {e}[/red]")
        return 1
