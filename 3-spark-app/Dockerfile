# Use an official Spark base image
FROM apache/spark:3.4.1-python3

# Switch to root to install packages
USER root

# Set working directory
WORKDIR /app

# Copy source code and requirements
COPY src/ ./src/
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Switch back to spark user (recommended for security)
USER spark

# Done - main.py will be referenced by SparkApplication
