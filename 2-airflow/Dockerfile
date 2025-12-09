# Use an Airflow 2.x base image consistent with your DAGs
FROM apache/airflow:3.0.2-python3.12
USER root
RUN apt-get update && apt-get install -y default-jre-headless && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
# Add the user's local bin directory to the PATH to make dbt and other pip-installed tools available
ENV PATH="/home/airflow/.local/bin:${PATH}"
# Copy and install Python dependencies
COPY --chown=airflow:airflow requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt 
