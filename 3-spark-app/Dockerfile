FROM apache/spark:3.5.1-python3

USER root

# --- Add Required Jars ---
# Spark-Kafka integration (connector + matching Kafka client)
RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o /opt/spark/jars/kafka-clients-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Hadoop AWS + AWS SDK (for S3/MinIO integration)
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.470.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.470/aws-java-sdk-bundle-1.12.470.jar

# Set working directory for application code
WORKDIR /opt/spark/work-dir

# Copy Python source files
COPY ./src/ .

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Switch back to non-privileged spark user
USER spark
