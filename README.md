# Hybrid Fraud Detection Platform

This project demonstrates a complete, end-to-end data engineering platform for fraud detection, built on modern, cloud-native technologies. It uses Kubernetes (via Minikube) to orchestrate a suite of tools including Airflow, Spark, Kafka, Minio, and PostgreSQL.

## Core Components

*   **Orchestration**: Apache Airflow for scheduling and monitoring workflows.
*   **Data Processing**: Apache Spark for large-scale batch and streaming data processing.
*   **Messaging**: Apache Kafka (via Strimzi) for real-time event streaming.
*   **Storage**: Minio for S3-compatible object storage.
*   **Data Warehouse**: PostgreSQL for storing structured data and serving as the Airflow metadata database.
*   **Transformation**: dbt for SQL-based data transformation.
*   **Containerization**: Docker for creating custom application images.
*   **Deployment**: Helm & kubectl for deploying all components onto Kubernetes.

## Prerequisites

Before you begin, you need several tools installed on your local machine (tested on Ubuntu/Debian). A convenience script is provided to install them.

1.  Make the script executable:
    ```bash
    chmod +x install_prerequisites.sh
    ```

2.  Run the script with sudo:
    ```bash
    sudo ./install_prerequisites.sh
    ```

3.  **IMPORTANT**: For the Docker group changes to take effect, you must log out and log back in, or run `newgrp docker` in your terminal.

## Setup Instructions

1.  **Start Minikube**:
    This project is configured to run on a local Minikube cluster.
    ```bash
    minikube start --driver=docker --cpus=4 --memory=8g
    ```

2.  **Deploy the Platform**:
    The main setup script handles the deployment of all services. It builds the custom Airflow Docker image and uses Helm to deploy all charts.
    ```bash
    sudo ./setup-environment.sh
    ```
    This process will take several minutes as it downloads images and waits for all components to become ready.

## Accessing Services

To access the user interfaces for Airflow and Minio, you'll need to forward their ports from the Kubernetes cluster to your local machine.

### Accessing the Minio UI

Minio is used for object storage (e.g., Spark output, Airflow logs).

1.  **Port-forward the service** (run in a separate terminal):
    ```bash
    kubectl port-forward --namespace storage svc/minio 9001:9001
    ```

2.  **Open the UI** in your browser: http://localhost:9001

3.  **Log in** with the default credentials:
    *   **Username**: `minio`
    *   **Password**: `minio123`

---

# Architectural Overview: Data Consolidation Strategy

This document outlines the strategy for creating a unified payment intelligence table to support fraud detection modeling. It covers the data architecture for both on-premise and cloud environments, a data quality strategy, and the implementation details of the consolidation pipeline.

## Task One: Data Consolidation Strategy

### Data Architecture Design

The core of the architecture is a multi-layered approach that accommodates data at different velocities (real-time, batch) and serves different needs (analytics, model serving).

#### On-Premise Architecture

This architecture leverages well-established open-source technologies that can be managed within a company's own data centers.



**Components & Data Flow:**

1.  **Ingestion Layer:**
    *   **Real-time (Payments, Fraud Labels API):** Apache Kafka is the central nervous system, ingesting high-throughput event streams.
    *   **Batch (Account Details, Risk Feeds, File-based Fraud Labels):** Data is landed on a distributed file system like a network file share (NFS) or an S3-compatible object store (e.g., MinIO). An orchestration tool like Apache Airflow schedules the ingestion jobs.

2.  **Storage (Data Lake):**
    *   **S3-Compatible Object Storage (e.g., MinIO, Ceph):** Acts as the central data lake, storing raw data in its original format and processed data in an optimized format like Apache Parquet. This provides cheap, scalable, and API-driven storage that aligns with modern cloud-native practices.

3.  **Processing Layer (Batch & Stream):**
    *   **Apache Spark:** The unified processing engine.
        *   **Spark Streaming** connects to Kafka to process payment and fraud events in near real-time, enriching them with data from the serving layer.
        *   **Spark Batch** jobs run on a schedule (managed by Airflow) to process daily account updates, external risk feeds, and perform large-scale feature engineering.

4.  **Serving Layer:**
    *   **Data Warehouse (e.g., PostgreSQL, Greenplum):** The consolidated `unified_payment_intelligence` table is loaded here. It serves analytical queries for data scientists, analysts, and BI tools.
    *   **Real-time Feature Store (e.g., Redis, Cassandra):** For sub-second lookups required by live transaction scoring models. This store would hold pre-computed features like `account_transaction_count_1h` or `account_risk_flag`.

5.  **Orchestration:**
    *   **Apache Airflow:** Manages and schedules all batch workflows, ensuring dependencies are met and failures are handled.

---

### Cloud-Native Architectures: AWS

Cloud platforms offer managed services that increase agility, reduce operational overhead, and provide elastic scalability. The same architectural pattern can be implemented across any major cloud provider, with flexibility in choosing serverless (lower ops) vs. provisioned (more control) services.

#### Architectural Flexibility

*   **Serverless vs. Provisioned:** For processing, you can choose a serverless engine like AWS Glue, Azure Synapse Serverless, or Google BigQuery, where you only pay for the compute you use. Alternatively, you can use a provisioned cluster like Amazon EMR, Azure Databricks, or Google Dataproc for long-running, complex jobs where you need fine-grained control over the environment.
*   **Interoperability:** By using open data formats like Apache Parquet in the data lake, you can avoid vendor lock-in. A Spark job written for one cloud's Spark service can be easily migrated to another. Infrastructure as Code (IaC) tools like Terraform further enhance this flexibility, allowing you to define and manage infrastructure across multiple clouds with a single configuration language.
*   **Scalability:** All architectures below are designed to scale automatically. Object storage, serverless processing engines, and managed databases can handle massive increases in data volume and query load without manual intervention.

---

#### Amazon Web Services (AWS)(Favoured architecture)

AWS Architecture

*   **Ingestion:**
    *   **Batch:** Files are uploaded to **Amazon S3** via **AWS Transfer Family** (for SFTP) or direct API calls. S3 events can trigger processing.
    *   **Real-time:** **Amazon API Gateway** ingests API data, which is fed into **Amazon Kinesis Data Streams** or **Amazon MSK** (Managed Kafka).
*   **Storage (Data Lake):** **Amazon S3** is the central data lake, storing raw and processed data in formats like Parquet.
*   **Processing:**
    *   **Batch:** **AWS Glue** provides a serverless Spark environment for ETL. For more control, **Amazon EMR** offers managed Hadoop/Spark clusters.
    *   **Stream:** **Amazon Kinesis Data Analytics** (for Flink/SQL) or **AWS Lambda** for simple, event-driven transformations.
*   **Serving:**
    *   **Data Warehouse:** **Amazon Redshift** for fast analytical queries.
    *   **Real-time Feature Store:** **Amazon DynamoDB** (NoSQL) or **Amazon ElastiCache** (in-memory Redis/Memcached).
*   **Orchestration:** **AWS Step Functions** for serverless workflows or **Amazon MWAA** (Managed Workflows for Apache Airflow) for complex DAGs.

---

### Data Quality & Monitoring Strategy

A robust data quality strategy is crucial for building trust in the fraud detection system.

1.  **Validation at Ingestion:**
    *   **Schema Enforcement:** All data, whether streaming or batch, must be validated against a predefined schema upon ingestion. For Kafka, this can be done using a Schema Registry (e.g., Confluent Schema Registry). For batch, this is the first step in the Spark/Glue job.
    *   **Data Assertions:** Use libraries like **Great Expectations** (integrates with Spark) or **dbt tests** to define and run assertions on the data itself (e.g., `amount` must be > 0, `account_id` must not be null, `timestamp` must be recent).
    *   **Quarantine/Dead-Letter Queue (DLQ):** Records that fail validation are routed to a separate "dead-letter" location (a Kafka topic or S3 path) for investigation, preventing them from corrupting the main pipeline.

2.  **Monitoring & Alerting:**
    *   **Pipeline Health:** Monitor the health of ETL jobs (e.g., execution time, memory usage, failure rates) using tools like Prometheus/Grafana (On-Prem) or Amazon CloudWatch (Cloud). Set up alerts for job failures or long durations.
    *   **Data Freshness & Volume:** Track the freshness of each data source. Set up alerts if a daily batch file hasn't arrived on time or if the volume of streaming data drops unexpectedly.
    *   **Data Drift:** Monitor the statistical distribution of key features over time. A sudden shift in the distribution of `amount` or the percentage of `risk_flag > 0` could indicate an upstream data issue or a new fraud trend.

3.  **Lineage & Governance:**
    *   **Data Lineage:** Implement tools (e.g., Egeria, dbt docs) to track data from its source through all transformations to the final unified table and features. This is critical for debugging, impact analysis, and regulatory compliance.
    *   **Ownership:** Assign clear owners to each data source and pipeline. When a data quality issue arises, the owner is responsible for the investigation and resolution.

---

### Alternative Architecture: ELT with dbt for Transformation

For increased modularity, testability, and governance, an ELT (Extract, Load, Transform) pattern using dbt is a powerful alternative. In this model, the responsibility of the Processing Layer (Spark/Glue) is simplified.

**Core Principle:**
*   **Extract & Load (Spark/Glue):** The Spark/Glue job is only responsible for reading the raw data sources (CSVs, APIs) and loading them into the data warehouse as separate "staging" tables (e.g., `stg_payments`, `stg_accounts`). No complex joins or transformations are performed at this stage.
*   **Transform (dbt):** After the raw data is loaded, an orchestrator (Airflow/Step Functions) triggers dbt. dbt then runs SQL models *inside the data warehouse* to perform all the joins, cleaning, and business logic required to build the final `unified_payment_intelligence` table from the staging tables.

#### On-Premise ELT with dbt

1.  **Ingestion & Load:** An Airflow-orchestrated **Spark** job reads source data and loads it into staging tables in **Oracle/PostgreSQL**.
2.  **Transform:** Airflow triggers a `dbt run` command. **dbt** connects to the warehouse, runs its SQL models to join the staging tables, and creates the final analytics-ready tables.
3.  **Test:** Airflow triggers `dbt test` to validate the final tables.


---

#### Cloud-Native ELT with dbt (AWS Example)

1.  **Ingestion & Load:** An orchestrator (Step Functions/MWAA) triggers an **AWS Glue** job. The job reads source data from S3 and loads it into staging tables in **Amazon Redshift**.
2.  **Transform:** The orchestrator triggers a **dbt** job (e.g., dbt Cloud, or dbt running on a Fargate container). dbt connects to Redshift, runs its SQL models, and materializes the final tables.
3.  **Test:** The orchestrator triggers a `dbt test` command.


**Advantages of this ELT approach:**
*   **Separation of Concerns:** Ingestion logic (Python/Spark) is separate from business logic (SQL/dbt).
*   **Accessibility:** Analysts who are strong in SQL can own and develop the transformation logic without needing to know Spark.
*   **Built-in Quality & Docs:** dbt provides an integrated framework for testing and documenting data models, improving data trust and governance.

---

## Task Two: Data Consolidation Pipeline

The implementation for the on-premise batch scenario is provided in the following Python scripts:

*   `generate_sample_data.py`: A utility to create mock CSV data for testing.
*   `consolidate_data.py`: The main script that reads the CSVs and produces the `unified_table.csv`.

### How to Run

1.  **Install dependencies:**
    ```bash
    pip install pandas
    ```
2.  **Generate sample data:**
    ```bash
    python spark_app/generate_sample_data.py
    ```
3.  **Run the consolidation script:**
    ```bash
    # This would typically be run via 'spark-submit'
    python spark_app/consolidate_data.py
    ```

This will produce the `unified_table.csv` file in the `spark_app` directory.

### Note on ETL vs. ELT Implementation

It is important to understand the distinction between the implemented script and the target architecture:

*   **`consolidate_data.py` (ETL Pattern):** The provided Python script performs an **ETL (Extract, Transform, Load)** process. It extracts data from four CSVs, transforms it all in memory using pandas, and loads a single, final, consolidated CSV file. This fulfills the direct requirements of Task Two.

*   **`fraud_detection_pipeline.py` (ELT Pattern):** The Airflow DAG is designed for a more advanced **ELT (Extract, Load, Transform)** architecture. In this pattern, the Spark job's role would be simplified to only **Extract** the raw data and **Load** it into four separate staging tables in a data warehouse. The **Transform** step (all the joins and cleaning) would then be handled by dbt, which is what the DAG orchestrates.

The DAG file explicitly notes this distinction in its comments. To fully realize the ELT architecture, the `consolidate_data.py` script would need to be modified to write to a database instead of a CSV, thereby preparing the data for the dbt transformation step.

## Bonus Challenge: Streaming & Batch Consolidation

The script `streaming_consolidation_bonus.py` demonstrates an approach to handle the bonus challenge requirements.

### Approach

1.  **Simulating Streams:** Python generators (`yield`) are used to simulate live API streams for payments and fraud cases. This mimics reading data one record at a time as it arrives.
2.  **Stateful Processing:** The core of the solution is a "main processor" function that maintains state. It holds payments in a temporary dictionary (a "waiting room") until their corresponding fraud labels arrive or a timeout is reached.
3.  **Handling Different Cadences:**
    *   **Account & Risk Data:** This "reference data" is loaded into memory at the start and can be periodically refreshed (simulated by a simple function call). In a real system, this would be a scheduled job.
    *   **Payment Stream:** As payments arrive, they are immediately enriched with the available account and risk data.
    *   **Fraud Stream:** As fraud labels arrive, the script looks up the corresponding payment in its state. If found, it enriches the payment with fraud info and "finalizes" it. If not, it might hold the label for a short period in case the payment arrives late.
4.  **Timeouts (Watermarking):** A simple timeout mechanism is implemented to ensure payments don't stay in memory indefinitely. In a real streaming engine like Spark or Flink, this is handled formally with "watermarking," which defines how long the system should wait for late-arriving data.

This simulation demonstrates the principles of stateful stream processing, which is essential for joining streams with different timings and characteristics.

---

## Future Proposal: AI/LLM Integration

To evolve the platform from a data consolidation system into an intelligent fraud detection ecosystem, the next logical step is to integrate AI and Large Language Models (LLMs). This can be done in two key areas: enhancing the fraud detection models themselves and improving the analytics workflow for the teams using the data.

### 1. AI/LLM for Advanced Feature Engineering

LLMs can extract insights from unstructured text data that is typically ignored by traditional models.

*   **Semantic Analysis of `payment_reference`:**
    *   **Proposal:** Use an LLM to analyze the `payment_reference` field to generate new features. This could involve classifying the payment's intent (`Invoice`, `Gift`, `Urgent Request`), performing sentiment analysis to detect urgency or distress, and extracting named entities.
    *   **Impact:** This would provide powerful new signals for detecting Authorized Push Payment (APP) fraud and social engineering scams.

*   **Summarization and Extraction from Analyst Notes:**
    *   **Proposal:** For historical fraud cases, an LLM could process unstructured analyst notes to automatically standardize fraud sub-types and extract all associated entities (accounts, names, etc.).
    *   **Impact:** This would help build a structured knowledge graph of fraud networks, making it easier to identify connected money mule accounts.

### 2. AI/LLM for Improving the Analytics Workflow

LLMs can make the platform more accessible and efficient for data scientists and fraud analysts.

*   **Natural Language to SQL (Text-to-SQL):**
    *   **Proposal:** Implement a "Text-to-SQL" interface where users can ask questions about the data in plain English (e.g., "What is the total value of payments to accounts with a high risk flag in the last 24 hours?"). The LLM would translate this into a valid SQL query to run against the data warehouse.
    *   **Impact:** This dramatically lowers the barrier to entry for data exploration, empowering less technical analysts and speeding up investigations.

*   **Generative AI for Synthetic Data:**
    *   **Proposal:** Use a generative AI model to create realistic, synthetic fraud data.
    *   **Impact:** This helps address the "imbalanced data" problem where fraud events are rare. By augmenting training datasets with high-quality synthetic examples, the fraud detection models can become more robust and better at identifying novel or infrequent fraud patterns.

### Implementation Strategy

These features would be integrated as new services or jobs within the existing architecture:
*   **Feature Engineering** would become a new Spark or batch job in the pipeline, orchestrated by Airflow.
*   **Text-to-SQL** could be a standalone web application that connects to the data warehouse.
*   **Synthetic Data Generation** would be an offline tool for the data science team.

By incorporating these AI/LLM capabilities, the platform can deliver significantly more value, moving beyond data consolidation to proactive, intelligent fraud detection and prevention.

### APPENDIX

#### Microsoft Azure

Azure Architecture

*   **Ingestion:**
    *   **Batch:** Files are uploaded to **Azure Data Lake Storage (ADLS) Gen2**. **Azure Data Factory** provides connectors and SFTP capabilities.
    *   **Real-time:** **Azure API Management** ingests API data, which is streamed into **Azure Event Hubs** (Kafka-compatible).
*   **Storage (Data Lake):** **Azure Data Lake Storage (ADLS) Gen2** serves as the scalable data lake.
*   **Processing:**
    *   **Batch:** **Azure Synapse Analytics** provides serverless Spark pools. **Azure Databricks** offers a highly optimized, collaborative Spark environment.
    *   **Stream:** **Azure Stream Analytics** for real-time SQL-based analysis or **Azure Functions** for event-driven code.
*   **Serving:**
    *   **Data Warehouse:** **Azure Synapse Analytics** (Dedicated SQL Pools) for enterprise data warehousing.
    *   **Real-time Feature Store:** **Azure Cosmos DB** (multi-model NoSQL) or **Azure Cache for Redis**.
*   **Orchestration:** **Azure Data Factory** for building and scheduling data pipelines.
