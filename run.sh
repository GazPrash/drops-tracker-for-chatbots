#!/bin/bash

if [ "$1" = "--scratch" ]; then
    echo "🧹 clearing database (scratch mode)..."
    rm -f tracker.db
fi

echo "building binaries..."
make build

echo "starting redis..."
# run redis in background
redis-server &
REDIS_PID=$!

echo "starting services..."
./bin/tracker &
TRACKER_PID=$!

# give tracker a second to boot and create the db
sleep 1

./bin/gateway &
GATEWAY_PID=$!

./bin/backend &
BACKEND_PID=$!

function cleanup() {
    echo ""
    echo "shutting down everything..."
    kill $TRACKER_PID $GATEWAY_PID $BACKEND_PID $REDIS_PID 2>/dev/null
    exit
}

# trap ctrl+c and kill all the background processes
trap cleanup SIGINT SIGTERM EXIT

echo ""
echo "🚀 all services are running!"
echo "📊 dashboard: http://localhost:8082"
echo "💬 chat client: http://localhost:8080"
echo ""
echo "press Ctrl+C to stop."

# wait forever so the script doesn't exit immediately
wait
