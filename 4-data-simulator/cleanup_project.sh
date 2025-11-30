#!/bin/bash

# This script cleans up the project directory by moving misplaced files
# to their correct locations and deleting duplicates.

set -e

echo "🧹 Starting project cleanup..."

# --- Define file mappings ---
# Format: "correct_path:incorrect_path_1:incorrect_path_2:..."
declare -A file_map
file_map["3-spark-app/src/streaming_processor.py"]="4-data-simulator/streaming_processor.py:2-airflow/dags/jobs/streaming_processor.py"
file_map["4-data-simulator/produce_payment_stream.py"]="2-airflow/dags/produce_payment_stream.py:2-airflow/dags/jobs/produce_payment_stream.py"
file_map["2-airflow/dags/streaming_payment_dag.py"]="2-airflow/dags/jobs/streaming_payment_dag.py"
file_map["2-airflow/dags/jobs/streaming_process_payment.yaml"]="" # No known incorrect locations, but good for completeness

# --- Main Cleanup Logic ---

for correct_path in "${!file_map[@]}"; do
    filename=$(basename "$correct_path")
    echo "----------------------------------------"
    echo "Checking for '$filename'..."

    # Get the list of incorrect paths for the current file
    IFS=':' read -r -a incorrect_paths <<< "${file_map[$correct_path]}"

    # 1. Ensure the correct directory exists
    correct_dir=$(dirname "$correct_path")
    if [ ! -d "$correct_dir" ]; then
        echo "Creating directory: $correct_dir"
        mkdir -p "$correct_dir"
    fi

    # 2. Iterate over known incorrect locations
    for incorrect_path in "${incorrect_paths[@]}"; do
        if [ -z "$incorrect_path" ]; then
            continue
        fi

        if [ -f "$incorrect_path" ]; then
            # If the file is in an incorrect location...
            if [ ! -f "$correct_path" ]; then
                # ...and the correct location is empty, move it.
                echo "  -> Found in wrong location. Moving '$incorrect_path' to '$correct_path'"
                mv "$incorrect_path" "$correct_path"
            else
                # ...and the correct location already has the file, it's a duplicate. Delete it.
                echo "  -> Found duplicate at '$incorrect_path'. Deleting it."
                rm "$incorrect_path"
            fi
        fi
    done

    # 3. Final check
    if [ -f "$correct_path" ]; then
        echo "  -> OK: Found in correct location: '$correct_path'"
    else
        echo "  -> WARNING: Could not find '$filename' in any known location."
    fi
done

echo "----------------------------------------"
echo "✅ Cleanup complete."