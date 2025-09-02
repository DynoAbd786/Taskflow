#!/bin/bash
set -e

echo "🚀 Setting up TaskFlow with conda..."

# Create conda environment
echo "📦 Creating conda environment..."
conda env create -f environment.yml

echo "🔧 Activating environment..."
eval "$(conda shell.bash hook)"
conda activate taskflow

# Create basic .env file
echo "⚙️ Creating configuration..."
cp .env.example .env

# Update .env with simpler local settings
cat > .env << EOF
# TaskFlow Local Development Configuration
ENVIRONMENT=development
DEBUG=true
TIMEZONE=UTC

# Database Configuration (using SQLite for simplicity)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=taskflow_dev
DB_USER=taskflow
DB_PASSWORD=taskflow_dev_pass

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# API Server Configuration
API_HOST=0.0.0.0
API_PORT=8000
API_WORKERS=1
API_RELOAD=true
API_ACCESS_LOG=true
API_CORS_ORIGINS=*

# Worker Configuration
WORKER_CONCURRENCY=2
WORKER_MAX_RETRIES=3
WORKER_TASK_TIMEOUT=300

# Security Configuration
SECRET_KEY=dev_secret_key_change_in_production

# Logging Configuration
LOG_LEVEL=INFO
LOG_FORMAT=console

# Monitoring Configuration
MONITORING_ENABLED=true
PROMETHEUS_PORT=8001
EOF

echo "✅ Setup complete!"
echo ""
echo "To start TaskFlow:"
echo "1. conda activate taskflow"
echo "2. ./start_services.sh"
echo ""
echo "Then open http://localhost:8000 in your browser"