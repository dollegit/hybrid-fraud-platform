#!/bin/bash

# This script automates the setup and execution of the Kafka payment stream producer.
# It ensures a dedicated virtual environment is used, with the correct dependencies installed.

# Exit immediately if a command exits with a non-zero status.
set -e

# Get the directory where the script is located to run everything relative to it.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
VENV_DIR="$SCRIPT_DIR/venv"
REQUIREMENTS_FILE="$SCRIPT_DIR/requirements.txt"
PRODUCER_SCRIPT="$SCRIPT_DIR/produce_payment_stream_prod.py"

echo "--- Kafka Producer Setup & Run ---"

# Create a virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating Python virtual environment at: $VENV_DIR"
    python3 -m venv "$VENV_DIR"
fi

# Activate the virtual environment and install dependencies
echo "Activating virtual environment and installing dependencies..."
source "$VENV_DIR/bin/activate"
pip install -r "$REQUIREMENTS_FILE"

echo "Starting the Kafka producer. Press Ctrl+C to stop."
python3 "$PRODUCER_SCRIPT"