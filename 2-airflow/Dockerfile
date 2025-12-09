FROM apache/airflow:3.0.2-python3.12

USER root
RUN apt-get update && apt-get install -y default-jre-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"

# Stage 1: Airflow providers with constraints
COPY --chown=airflow:airflow airflow-requirements.txt /airflow-requirements.txt
ARG AIRFLOW_VERSION=3.0.2
ARG PYTHON_VERSION=3.12
RUN pip install --upgrade pip setuptools wheel && \
    curl -sSL https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt \
      -o /home/airflow/constraints.txt && \
    pip install --no-cache-dir -r /airflow-requirements.txt -c /home/airflow/constraints.txt

# Stage 2: dbt packages without constraints
COPY --chown=airflow:airflow dbt-requirements.txt /dbt-requirements.txt
RUN pip install --no-cache-dir -r /dbt-requirements.txt


# # Use an Airflow 2.x base image consistent with your DAGs
# FROM apache/airflow:3.0.2-python3.12
# USER root
# RUN apt-get update && apt-get install -y default-jre-headless && apt-get clean && rm -rf /var/lib/apt/lists/*
# ENV JAVA_HOME=/usr/lib/jvm/default-java
# ENV PATH="${JAVA_HOME}/bin:${PATH}"

# USER airflow
# # Add the user's local bin directory to the PATH to make dbt and other pip-installed tools available
# ENV PATH="/home/airflow/.local/bin:${PATH}"
# # Copy and install Python dependencies
# COPY --chown=airflow:airflow requirements.txt /
# RUN pip install --no-cache-dir -r /requirements.txt
