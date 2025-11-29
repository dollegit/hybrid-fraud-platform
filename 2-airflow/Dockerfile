FROM apache/airflow:3.0.2-python3.12
USER root
RUN apt-get update && apt-get install -y default-jre-headless && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark apache-airflow-providers-cncf-kubernetes dbt-core dbt-postgres
