#!/bin/bash
# Start the API server

cd "$(dirname "$0")"

# Install dependencies if needed
if ! python3 -c "import flask" 2>/dev/null; then
    echo "Installing API dependencies..."
    pip install -r requirements-api.txt
fi

echo "Starting Brownfield Cartographer API on http://localhost:5000"
python3 api_server.py
