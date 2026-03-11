#!/bin/bash
# Production entry point: runs Flask dashboard + CH worker in parallel
# Flask serves the web dashboard on port 5000
# Worker runs the CH sweep and exits when done

python worker.py &
WORKER_PID=$!
echo "[deploy] Worker started (PID $WORKER_PID)"

echo "[deploy] Starting Flask dashboard on :5000"
exec python app.py
