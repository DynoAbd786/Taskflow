# TaskFlow - Distributed Task Processing System

A comprehensive, production-ready distributed task processing system with web interface, real-time monitoring, and advanced management capabilities.

## Features

### Core Capabilities
- **Distributed Task Processing**: Execute tasks across multiple workers with load balancing
- **Multiple Queue Support**: Organize tasks by priority and category
- **Real-time Monitoring**: Live dashboard with metrics and system health
- **Comprehensive API**: RESTful API with WebSocket support for real-time updates
- **CLI Administration**: Powerful command-line tools for system management

### Advanced Features
- **Automatic Retry Logic**: Configurable retry strategies with exponential backoff
- **Circuit Breakers**: Prevent cascading failures with automatic recovery
- **Task Dependencies**: Chain tasks with dependency management
- **Scheduled Tasks**: Cron-like scheduling for recurring tasks
- **Batch Processing**: Efficient handling of bulk operations
- **Health Monitoring**: Automatic health checks with recovery actions

### Management & Operations
- **Web Dashboard**: Intuitive interface for monitoring and management
- **Prometheus Metrics**: Built-in metrics export for monitoring systems
- **Structured Logging**: JSON logging with correlation IDs
- **Database Persistence**: PostgreSQL backend with migrations
- **Configuration Management**: Environment-based configuration with validation

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Web Dashboard │    │    REST API      │    │   CLI Tools     │
│                 │    │                  │    │                 │
└─────────┬───────┘    └────────┬─────────┘    └─────────┬───────┘
          │                     │                        │
          └─────────────────────┼────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    │    TaskFlow Engine    │
                    │                       │
                    └───────────┬───────────┘
                                │
                ┌───────────────┼───────────────┐
                │               │               │
        ┌───────▼───────┐ ┌─────▼─────┐ ┌─────▼─────┐
        │ Queue Manager │ │  Workers  │ │   Redis   │
        │               │ │           │ │           │
        └───────────────┘ └───────────┘ └───────────┘
                │                               │
        ┌───────▼───────┐                 ┌─────▼─────┐
        │  PostgreSQL   │                 │ Monitoring│
        │   Database    │                 │ & Metrics │
        └───────────────┘                 └───────────┘
```

## Quick Start

### Prerequisites
- Python 3.11+
- PostgreSQL 12+
- Redis 6+

### Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd taskflow
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Setup environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

4. **Initialize database:**
   ```bash
   python main.py config validate
   # Setup your PostgreSQL database
   ```

### Running TaskFlow

1. **Start the API server:**
   ```bash
   python main.py server start
   ```

2. **Start workers (in another terminal):**
   ```bash
   python main.py worker start --queues default,high_priority --concurrency 4
   ```

3. **Access the dashboard:**
   Open http://localhost:8000 in your browser

### Example Usage

```python
import asyncio
from taskflow.core.engine import TaskEngine, task

# Define a task
@task(name="process_data", queue="data_processing")
async def process_data(data):
    # Your processing logic here
    await asyncio.sleep(1)
    return {"processed": len(data)}

# Submit tasks
async def main():
    engine = TaskEngine()
    await engine.start()
    
    # Submit a task
    task_id = await engine.submit_task(
        "process_data", 
        [{"items": [1, 2, 3, 4, 5]}]
    )
    
    print(f"Task submitted: {task_id}")
    await engine.shutdown()

asyncio.run(main())
```

## CLI Commands

TaskFlow provides comprehensive CLI tools for system administration:

### Server Management
```bash
# Start API server
python main.py server start --host 0.0.0.0 --port 8000

# Check server status
python main.py server status

# Monitor server metrics
python main.py server monitor
```

### Worker Management
```bash
# Start workers
python main.py worker start --queues default,high --concurrency 4

# List active workers
python main.py worker list

# Monitor worker performance
python main.py worker monitor

# Scale workers
python main.py worker scale --target-size 8
```

### Task Management
```bash
# Submit a task
python main.py task submit my_task --args '[1, 2, 3]' --queue high_priority

# List tasks
python main.py task list --status running --limit 20

# Get task details
python main.py task get <task_id>

# Cancel a task
python main.py task cancel <task_id>

# Task statistics
python main.py task stats --queue default
```

### Queue Management
```bash
# List all queues
python main.py queue list

# Queue information
python main.py queue info default

# Monitor queue in real-time
python main.py queue monitor default --watch

# Purge queue
python main.py queue purge test_queue --confirm
```

### System Monitoring
```bash
# Live dashboard
python main.py monitor dashboard

# Export metrics
python main.py monitor metrics --format prometheus

# View alerts
python main.py monitor alerts --severity high

# Generate system report
python main.py monitor report --output report.json
```

### Configuration
```bash
# Show configuration
python main.py config show --section database

# Validate configuration
python main.py config validate

# Generate config template
python main.py config generate --template production --output config.yaml
```

## Configuration

TaskFlow supports configuration through environment variables, configuration files, or a combination of both.

### Environment Variables

Key environment variables (see `.env.example` for complete list):

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=taskflow
DB_USER=taskflow
DB_PASSWORD=your_password

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379

# API
API_HOST=0.0.0.0
API_PORT=8000

# Workers
WORKER_CONCURRENCY=4
WORKER_MAX_RETRIES=3

# Security
SECRET_KEY=your_secret_key_here

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json

# Monitoring
MONITORING_ENABLED=true
PROMETHEUS_PORT=8001
```

### Configuration File

Create a `config.yaml` file:

```yaml
database:
  host: localhost
  port: 5432
  name: taskflow
  user: taskflow
  password: your_password

redis:
  host: localhost
  port: 6379

api:
  host: 0.0.0.0
  port: 8000
  workers: 4

worker:
  concurrency: 4
  max_retries: 3
  task_timeout: 300

monitoring:
  enabled: true
  prometheus_port: 8001
```

## API Reference

### Task Submission

```bash
POST /tasks
{
  "name": "process_data",
  "args": [{"key": "value"}],
  "kwargs": {},
  "queue": "default",
  "priority": "normal",
  "timeout": 300
}
```

### Task Management

```bash
GET /tasks/{task_id}           # Get task details
POST /tasks/{task_id}/actions  # Cancel/retry task
GET /tasks                     # List tasks
```

### Queue Operations

```bash
GET /queues                    # List queues
GET /queues/{name}/stats       # Queue statistics
POST /queues/{name}/actions    # Queue actions
```

### System Information

```bash
GET /health                    # Health check
GET /stats                     # System statistics
GET /workers                   # Active workers
```

### WebSocket

Connect to `/ws` for real-time updates:

```javascript
const ws = new WebSocket('ws://localhost:8000/ws');
ws.onmessage = function(event) {
    const data = JSON.parse(event.data);
    // Handle real-time updates
};
```

## Monitoring & Observability

### Prometheus Metrics

TaskFlow exports comprehensive metrics on port 8001 (configurable):

- Task execution metrics (duration, success rate, etc.)
- Queue depth and throughput
- Worker performance and resource usage
- System resource utilization

### Structured Logging

All components use structured JSON logging with correlation IDs for request tracing:

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "level": "INFO",
  "component": "task_engine",
  "message": "Task completed successfully",
  "task_id": "abc123",
  "worker_id": "worker-1",
  "execution_time": 1.234,
  "correlation_id": "req-456"
}
```

### Health Checks

Built-in health checks monitor:
- Database connectivity
- Redis availability
- Queue health
- Worker status
- System resources

## Development

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio pytest-mock

# Run tests
pytest tests/

# Run with coverage
pytest --cov=taskflow tests/
```

### Project Structure

```
taskflow/
├── api/           # FastAPI web interface
├── cli/           # Command-line tools
├── core/          # Core task processing engine
├── db/            # Database models and repositories
├── monitoring/    # Metrics and alerting
├── utils/         # Utilities and configuration
├── web/           # Web dashboard components
└── workers/       # Worker processes and management

examples/          # Usage examples
tests/            # Test suite
config/           # Configuration files
static/           # Static web assets
templates/        # HTML templates
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Production Deployment

### Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 8000

CMD ["python", "main.py", "server", "start"]
```

### Docker Compose

```yaml
version: '3.8'
services:
  taskflow-api:
    build: .
    ports:
      - "8000:8000"
    environment:
      - DB_HOST=postgres
      - REDIS_HOST=redis
    depends_on:
      - postgres
      - redis

  taskflow-worker:
    build: .
    command: python main.py worker start --concurrency 4
    environment:
      - DB_HOST=postgres
      - REDIS_HOST=redis
    depends_on:
      - postgres
      - redis

  postgres:
    image: postgres:13
    environment:
      - POSTGRES_DB=taskflow
      - POSTGRES_USER=taskflow
      - POSTGRES_PASSWORD=taskflow

  redis:
    image: redis:6-alpine
```

### Kubernetes

See `k8s/` directory for Kubernetes deployment manifests.

## Performance Tuning

### Worker Optimization
- Adjust `WORKER_CONCURRENCY` based on CPU cores
- Tune `WORKER_TASK_TIMEOUT` for your workload
- Use queue-specific workers for different task types

### Database Optimization
- Increase `DB_POOL_SIZE` for high-throughput scenarios
- Configure connection pooling
- Use read replicas for monitoring queries

### Redis Optimization
- Increase `REDIS_MAX_CONNECTIONS` for high concurrency
- Use Redis clustering for large deployments
- Configure persistence settings appropriately

## Troubleshooting

### Common Issues

**Workers not processing tasks:**
```bash
# Check worker status
python main.py worker list

# Check queue health
python main.py queue health

# Monitor system resources
python main.py monitor dashboard
```

**Database connection issues:**
```bash
# Validate configuration
python main.py config validate

# Test database connectivity
python main.py health
```

**High memory usage:**
```bash
# Monitor system metrics
python main.py monitor dashboard

# Check worker performance
python main.py worker monitor

# Review task complexity and optimize
```

### Logs Analysis

Enable debug logging:
```bash
export LOG_LEVEL=DEBUG
python main.py server start
```

Use correlation IDs to trace requests across components.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

- Documentation: [Link to docs]
- Issues: [GitHub Issues]
- Discussions: [GitHub Discussions]
- Chat: [Discord/Slack link]
