# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Quick Start Commands

### Environment Setup
```bash
# Setup with conda (recommended)
./setup.sh

# Simple start (requires conda environment activated)
conda activate taskflow
./simple_start.sh
```

### Development Commands
```bash
# Start API server only
python main.py server start --host 0.0.0.0 --port 8000

# Start worker process
python main.py worker start --queues default,high --concurrency 2

# Run tests
pytest tests/
pytest --cov=taskflow tests/  # with coverage

# Code formatting and quality
black taskflow/
isort taskflow/
```

### CLI Administration
```bash
# System status and health
python main.py status
python main.py health

# Task management
python main.py task submit my_task --args '[1, 2, 3]' --queue high_priority
python main.py task list --status running --limit 20
python main.py task get <task_id>

# Queue operations
python main.py queue list
python main.py queue info default
python main.py queue monitor default --watch

# Worker management
python main.py worker list
python main.py worker monitor
python main.py worker scale --target-size 8

# Configuration
python main.py config show --section database
python main.py config validate
```

## Architecture Overview

TaskFlow is a distributed task processing system with the following core components:

### Core Components
- **TaskEngine** (`taskflow/core/engine.py`): Central orchestrator that manages task lifecycle, registration, and execution
- **TaskQueue** (`taskflow/core/queue.py`): Redis-backed queue system supporting multiple named queues with priority handling
- **Worker** (`taskflow/workers/worker.py`): Async worker processes that consume and execute tasks from queues
- **API Server** (`taskflow/api/main.py`): FastAPI-based REST API with WebSocket support for real-time monitoring

### Key Patterns
- **Task Registration**: Functions decorated with `@task` are automatically registered in the TaskRegistry
- **Async/Sync Support**: Both async and synchronous task functions are supported via ThreadPoolExecutor
- **Queue-based Architecture**: Tasks are queued by name (default, high_priority, etc.) and processed by workers
- **Real-time Updates**: WebSocket connections broadcast task events to connected clients
- **Configuration Management**: Environment variables and YAML config files via Pydantic settings

### Database Layer
- **Models** (`taskflow/db/models.py`): SQLAlchemy models for tasks, queues, workers, and metrics
- **Repositories** (`taskflow/db/repositories.py`): Data access layer with async operations
- **Migrations**: Handled via Alembic (check for migration files in alembic/ directory)

### Monitoring & Operations
- **Metrics Export**: Prometheus metrics on configurable port (default: 8001)
- **Structured Logging**: JSON logging with correlation IDs using structlog
- **Health Checks**: Built-in endpoints for system, database, queue, and worker health
- **Web Dashboard**: Real-time web interface served at root path

## Configuration Notes

### Environment Variables
Key variables are defined in `.env` file or environment:
- `DB_*`: PostgreSQL database connection settings
- `REDIS_*`: Redis connection and configuration
- `API_*`: Web server host, port, and CORS settings
- `WORKER_*`: Concurrency, retry limits, and timeout settings
- `LOG_LEVEL`: Logging verbosity (DEBUG, INFO, WARN, ERROR)

### Development vs Production
- Development uses simplified Redis setup via `simple_start.sh`
- Production deployment examples in README.md include Docker Compose and Kubernetes manifests
- The `./setup.sh` script creates a conda environment with all dependencies

## Task Development Patterns

### Task Definition
```python
from taskflow.core.engine import task

@task(name="process_data", queue="data_processing", timeout=300)
async def process_data(data):
    # Task implementation
    return {"processed": len(data)}
```

### Task Submission
```python
from taskflow.core.engine import get_engine

engine = get_engine()
await engine.start()
task_id = await engine.submit_task("process_data", args=[data], queue="default")
```

## Testing Strategy

- **Unit tests** in `tests/unit/` directory
- **Integration tests** in `tests/integration/` directory  
- **Test configuration**: pytest with async support and mocking capabilities
- **Coverage**: Run with `pytest --cov=taskflow tests/`

## Key Entry Points

- **CLI**: `main.py` → `taskflow/cli/main.py`
- **API Server**: `taskflow/api/main.py`
- **Task Engine**: `taskflow/core/engine.py`
- **Worker Process**: `taskflow/workers/worker.py`