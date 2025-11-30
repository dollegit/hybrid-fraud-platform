FROM apache/spark:3.5.1-python3

USER root

# --- Add Required Jars ---
# Instead of using spark.jars.packages, we bake the jars into the image.
# These are the dependencies for Spark-Kafka integration.
# Kafka source (already handled earlier, keep if not baked yet)
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.0/kafka-clients-3.6.0.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

# S3A filesystem (Hadoop AWS)
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/hadoop-aws-3.3.4.jar

# AWS SDK v1 bundle compatible with Hadoop 3.3.4
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.470/aws-java-sdk-bundle-1.12.470.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.470.jar

# S3A filesystem for MinIO
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.470/aws-java-sdk-bundle-1.12.470.jar /opt/spark/jars/


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