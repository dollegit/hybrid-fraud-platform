FROM apache/spark:3.5.1-python3

USER root

# Set the working directory for the application code
WORKDIR /opt/spark/work-dir

# Copy all Python source files from the 'src' directory into the image
COPY ./src/ .

# Copy the requirements file and install dependencies
# Note: Add any required Python packages for your jobs (e.g., boto3, kafka-python) to requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Switch back to the non-privileged spark user
USER spark