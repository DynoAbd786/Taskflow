# Gemini Context: TaskFlow Project

This document provides a comprehensive analysis of the TaskFlow project, intended as a guide for an AI assistant.

## 1. Project Overview

TaskFlow is a sophisticated, Python-based distributed task processing system. Its primary purpose is to manage, queue, and execute background jobs or tasks across a pool of workers. It is designed to be production-ready, featuring a web interface, real-time monitoring, a REST API, and a powerful command-line interface (CLI).

**Core Technologies:**
*   **Backend Framework:** FastAPI for the REST API and WebSocket communication.
*   **Task Queuing:** Redis is used as the message broker for queuing tasks. Celery is listed as a dependency, but the core logic seems to be custom-built around Redis.
*   **Database:** PostgreSQL for persistent storage of task data, with Alembic for migrations. The setup can also be configured to use SQLite for simpler local development.
*   **CLI:** Click is used to build the comprehensive command-line interface.
*   **Frontend:** Jinja2 templates for serving a server-rendered dashboard, with vanilla CSS and JS for the frontend.
*   **Tooling:** `setuptools` for packaging, `pytest` for testing, `black` and `isort` for code formatting.

**Architectural Components:**
*   **TaskFlow Engine:** The central component that manages the lifecycle of tasks.
*   **API Server (`taskflow/api/main.py`):** A FastAPI application that exposes endpoints for submitting and managing tasks, and serves a web dashboard.
*   **CLI (`taskflow/cli/main.py`):** The main entry point (`main.py`) for all administrative and operational commands. It provides groups for managing the server, workers, tasks, queues, and configuration.
*   **Workers (`taskflow/workers/`):** Separate processes that connect to Redis, pull tasks from queues, and execute them.
*   **Redis:** Acts as the message broker between the API/CLI and the workers.
*   **PostgreSQL/SQLite:** Stores historical data about tasks and system state.

## 2. Building and Running

The project uses a Conda environment defined in `environment.yml`.

### First-Time Setup

1.  **Create Conda Environment:** The `setup.sh` script automates this process.
    ```bash
    ./setup.sh
    ```
2.  **Activate Environment:**
    ```bash
    conda activate taskflow
    ```

### Running the Application

There are several scripts to start the services. The most straightforward is `simple_start.sh`.

**Recommended Method (Simple Start):**
This script starts Redis, the API server, and a worker in the background.
```bash
# Make sure the conda environment is active
conda activate taskflow

# Run the simple start script
./simple_start.sh
```
*   **Web Dashboard:** [http://localhost:8000](http://localhost:8000)
*   **Health Check:** [http://localhost:8000/health](http://localhost:8000/health)

**Manual Execution:**
Alternatively, start each component manually using the main CLI.

1.  **Start Redis (if not running):**
    ```bash
    redis-server --port 6379 --daemonize yes
    ```
2.  **Start the API Server:**
    ```bash
    python main.py server start
    ```
3.  **Start a Worker (in a new terminal):**
    ```bash
    conda activate taskflow
    python main.py worker start --queues default,high --concurrency 2
    ```

### Running Tests

The project uses `pytest`. Test files are located in the `tests/` directory.

```bash
# Install test dependencies if needed
pip install pytest pytest-asyncio pytest-mock

# Run the test suite
pytest tests/
```

## 3. Development Conventions

*   **Code Style:** The project uses `black` for code formatting and `isort` for import sorting. Configurations are present in `pyproject.toml`.
*   **Entry Point:** The main entry point for the application is `main.py`, which launches the Click-based CLI. All functionality (server, worker, etc.) is invoked through this CLI.
*   **Configuration:**
    *   Configuration is managed via environment variables and a `.env` file.
    *   A `.env.example` file provides a template.
    *   The `taskflow/utils/config.py` module, likely using Pydantic's `BaseSettings`, loads the configuration.
    *   The `main.py config` command group can be used to inspect the configuration.
*   **Project Structure:** The code is organized into functional modules within the `taskflow/` directory (e.g., `api`, `cli`, `core`, `db`, `workers`).
*   **Database Migrations:** Alembic is used for database migrations, suggesting that changes to `taskflow/db/models.py` should be accompanied by a new migration script.
