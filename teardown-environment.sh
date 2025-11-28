#!/bin/bash

echo "--- 1. Uninstalling Helm Releases ---"

# Uninstall Airflow
echo "Uninstalling Airflow..."
helm uninstall airflow --namespace airflow --wait

# Uninstall Spark Operator
echo "Uninstalling Spark Operator..."
helm uninstall spark-operator --namespace spark-operator --wait

# Uninstall Spark Standalone Cluster
echo "Uninstalling Spark Standalone Cluster..."
helm uninstall spark --namespace spark --wait

# Uninstall Strimzi Kafka Operator
echo "Uninstalling Strimzi Kafka Operator..."
helm uninstall strimzi-kafka-operator --namespace kafka --wait

echo "--- 2. Deleting Kubernetes Resources ---"

# Delete MinIO resources
echo "Deleting MinIO resources..."
kubectl delete -f 1-kubernetes-manifests/03-minio/minio-statefulset.yaml -n storage --ignore-not-found=true

# Delete Kafka Cluster (Strimzi will handle pod/service cleanup)
echo "Deleting Kafka Cluster..."
kubectl delete -f 1-kubernetes-manifests/02-kafka-strimzi/kafka-cluster.yaml -n kafka --ignore-not-found=true

echo "--- 3. Deleting Kubernetes Namespaces ---"
echo "This will remove all remaining resources in these namespaces."
kubectl delete -f 1-kubernetes-manifests/01-namespaces.yaml --ignore-not-found=true

echo "--- Teardown Complete! ---"