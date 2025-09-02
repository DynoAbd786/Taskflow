#!/bin/bash
set -e

echo "🚀 Starting TaskFlow services..."

# Check if conda environment is activated
if [[ "$CONDA_DEFAULT_ENV" != "taskflow" ]]; then
    echo "❌ Please activate the taskflow conda environment first:"
    echo "   conda activate taskflow"
    exit 1
fi

# Function to cleanup on exit
cleanup() {
    echo "🧹 Cleaning up..."
    if [[ ! -z "$REDIS_PID" ]]; then
        kill $REDIS_PID 2>/dev/null || true
    fi
    if [[ ! -z "$API_PID" ]]; then
        kill $API_PID 2>/dev/null || true
    fi
    if [[ ! -z "$WORKER_PID" ]]; then
        kill $WORKER_PID 2>/dev/null || true
    fi
    exit 0
}

trap cleanup SIGINT SIGTERM

# Create data directories
mkdir -p data/redis data/logs

# Start Redis (using conda-installed Redis)
echo "🔴 Starting Redis server..."
redis-server --port 6379 --dir ./data/redis --logfile $(pwd)/data/logs/redis.log --daemonize yes
REDIS_PID=$(pgrep redis-server)
echo "   Redis started with PID: $REDIS_PID"

# Wait for Redis to be ready
echo "⏳ Waiting for Redis to be ready..."
sleep 2

# Test Redis connection
if redis-cli ping > /dev/null 2>&1; then
    echo "✅ Redis is running and responding"
else
    echo "❌ Redis connection failed"
    exit 1
fi

# Validate TaskFlow configuration
echo "🔧 Validating configuration..."
python main.py config validate || {
    echo "❌ Configuration validation failed"
    exit 1
}

echo "✅ Configuration is valid"

# Set Redis environment variables for the application
export REDIS_HOST="localhost"
export REDIS_PORT="6379"
export REDIS_DB="0"

# Start TaskFlow API server in background
echo "🌐 Starting TaskFlow API server..."
python main.py server start &
API_PID=$!
echo "   API server started with PID: $API_PID"

# Wait for API server to be ready
echo "⏳ Waiting for API server to be ready..."
sleep 5

# Test API server
if curl -s http://localhost:8000/health > /dev/null 2>&1; then
    echo "✅ API server is running and responding"
else
    echo "⚠️  API server may still be starting up..."
fi

# Start TaskFlow worker in background
echo "👷 Starting TaskFlow worker..."
python main.py worker start --queues default,high,data --concurrency 2 &
WORKER_PID=$!
echo "   Worker started with PID: $WORKER_PID"

echo ""
echo "🎉 TaskFlow is now running!"
echo ""
echo "📊 Web Dashboard: http://localhost:8000"
echo "📈 Metrics:       http://localhost:8001"
echo "🔧 Health Check:  http://localhost:8000/health"
echo ""
echo "📚 Try these commands in another terminal:"
echo "   conda activate taskflow"
echo "   python main.py task submit add_numbers --args '[10, 20]'"
echo "   python main.py task list"
echo "   python main.py worker list"
echo "   python main.py queue list"
echo "   python main.py monitor dashboard"
echo ""
echo "🔬 Run example script:"
echo "   python examples/basic_usage.py"
echo ""
echo "Press Ctrl+C to stop all services"

# Keep script running and wait for interrupt
wait