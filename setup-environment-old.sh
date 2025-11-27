#!/bin/bash
set -euo pipefail

# --------------- CONFIG ---------------
NAMESPACE="airflow"
PG_RELEASE="airflow-postgresql"
AIRFLOW_RELEASE="airflow"
PG_CHART="oci://registry-1.docker.io/bitnamicharts/postgresql"
PG_IMAGE_REPO="bitnami/postgresql"
PG_IMAGE_TAG="latest"            # you can pin to 16.11.x or a specific version if desired
PG_PERSISTENCE_SIZE="8Gi"
AIRFLOW_CHART="apache-airflow/airflow"
MIGRATE_TIMEOUT="300"            # seconds; increased wait for migrations
HELM_TIMEOUT="20m"
# --------------------------------------

echo
echo "===== AIRFLOW + POSTGRES DEPLOY SCRIPT ====="
echo

# helper: check command exists
for cmd in kubectl helm; do
  if ! command -v $cmd >/dev/null 2>&1 ; then
    echo "ERROR: '$cmd' is required but not installed/available in PATH."
    exit 1
  fi
done

# ------------------ 0. CLEANUP ------------------
echo "=== 0. CLEANUP: Remove previous resources that may block helm ==="
set +e
kubectl delete secret airflow-db-secret -n "${NAMESPACE}" --ignore-not-found
kubectl delete secret "${PG_RELEASE}" -n "${NAMESPACE}" --ignore-not-found
# Remove Helm release secrets left behind
kubectl get secrets -n "${NAMESPACE}" -o name 2>/dev/null | grep -E "sh.helm.release.v1.${PG_RELEASE}" | xargs -r kubectl delete -n "${NAMESPACE}" --ignore-not-found
kubectl get secrets -n "${NAMESPACE}" -o name 2>/dev/null | grep -E "sh.helm.release.v1.${AIRFLOW_RELEASE}" | xargs -r kubectl delete -n "${NAMESPACE}" --ignore-not-found

kubectl delete statefulset "${PG_RELEASE}" -n "${NAMESPACE}" --ignore-not-found
kubectl delete svc "${PG_RELEASE}" -n "${NAMESPACE}" --ignore-not-found
kubectl delete svc "${PG_RELEASE}-hl" -n "${NAMESPACE}" --ignore-not-found
kubectl delete svc "${PG_RELEASE}-postgresql" -n "${NAMESPACE}" --ignore-not-found 2>/dev/null || true

# optionally delete PVCs if you want a fresh DB (UNCOMMENT if you want to wipe stored DB data)
# Warning: this permanently deletes DB data
# kubectl delete pvc -l app.kubernetes.io/name=postgresql -n "${NAMESPACE}" --ignore-not-found

helm uninstall "${PG_RELEASE}" -n "${NAMESPACE}" --ignore-not-found
helm uninstall "${AIRFLOW_RELEASE}" -n "${NAMESPACE}" --ignore-not-found
set -e
echo "Cleanup done."
echo

# ------------------ 1. NAMESPACE ------------------
echo "=== 1. Ensure namespace exists ==="
kubectl apply -f 1-kubernetes-manifests/01-namespaces.yaml || {
  # fallback: create namespace if manifest not present
  kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
}
echo

# ------------------ 2. Optional infra (Strimzi / Kafka / MinIO / Spark) ------------------
# If you already have these installed you can skip, otherwise the below commands will install them.
echo "=== 2. Install Strimzi (Kafka operator) ==="
helm repo add strimzi https://strimzi.io/charts/ || true
helm repo update
helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
  -n kafka --create-namespace --version 0.49.0 --wait --timeout 30m
echo

echo "=== 3. Deploy Kafka cluster YAML (if present) ==="
kubectl apply -f 1-kubernetes-manifests/02-kafka-strimzi/kafka-cluster.yaml -n kafka || true
echo

echo "=== 4. Deploy MinIO (if present) ==="
kubectl apply -f 1-kubernetes-manifests/03-minio/minio-statefulset.yaml -n storage || true
echo

echo "=== 5. Install Spark-operator ==="
helm repo add spark-operator https://kubeflow.github.io/spark-operator || true
helm repo update
helm upgrade --install spark-operator spark-operator/spark-operator \
  --namespace spark-operator --create-namespace \
  --set sparkJobNamespace=spark-jobs \
  --set webhook.healthProbe.port=8080 \
  --wait --timeout 30m || true
echo

# ------------------ 6. PostgreSQL (Bitnami) ------------------
echo "=== 6. Deploy PostgreSQL (Bitnami chart, OCI) ==="
helm repo add bitnami https://charts.bitnami.com/bitnami || true
helm repo update

# Use the Bitnami postgresql chart (OCI registry) and create the airflow DB + user.
# Set both the application user (airflow) and enable postgres user to avoid password mismatch with old PVs if present.
helm upgrade --install "${PG_RELEASE}" "${PG_CHART}" \
  --namespace "${NAMESPACE}" --create-namespace \
  --set image.repository="${PG_IMAGE_REPO}" \
  --set image.tag="${PG_IMAGE_TAG}" \
  --set auth.username=airflow \
  --set auth.password=airflow \
  --set auth.postgresPassword=postgres \
  --set auth.enablePostgresUser=true \
  --set auth.database=airflow \
  --set primary.persistence.enabled=true \
  --set primary.persistence.size="${PG_PERSISTENCE_SIZE}" \
  --wait --timeout 15m

echo "Waiting for PostgreSQL pod to become Ready..."
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=postgresql -n "${NAMESPACE}" --timeout=5m
echo "Postgres should be ready (check pods with: kubectl get pods -n ${NAMESPACE})"
echo

# ------------------ 7. Create Airflow DB secret (single 'connection' key) ------------------
echo "=== 7. Create Airflow DB secret with 'connection' key (what the chart expects) ==="
kubectl delete secret airflow-db-secret -n "${NAMESPACE}" --ignore-not-found
# build connection string: postgresql+psycopg2://user:pass@host:port/db
CONN="postgresql+psycopg2://airflow:airflow@${PG_RELEASE}:5432/airflow"
kubectl create secret generic airflow-db-secret -n "${NAMESPACE}" \
  --from-literal=connection="${CONN}"

echo "Secret 'airflow-db-secret' created with connection: ${CONN}"
echo


# Navigate to the directory with the Dockerfile and requirements.txt
cd 2-airflow

# Build the Docker image
# Replace 'your-dockerhub-username' with your actual username
sudo docker build . -t psalmprax/airflow-custom:2.9.2-p1

# Push the image to Docker Hub
sudo docker push psalmprax/airflow-custom:2.9.2-p1

# ------------------ Optional: manual migration (commented) ------------------
# If you prefer to run migration manually instead of letting Helm run jobs.migrateDatabase,
# uncomment this block and set jobs.migrateDatabase.enabled=false in the Airflow chart below.
#
# echo "=== OPTIONAL: Manual DB migration using Airflow image ==="
# kubectl run airflow-db-migrate -n "${NAMESPACE}" --rm -i --tty \
#   --image apache/airflow:3.0.2 \
#   --restart=Never \
#   --env AIRFLOW__CORE__SQL_ALCHEMY_CONN="${CONN}" \
#   --env AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="${CONN}" \
#   --command -- airflow db migrate
#
# Note: If you run manual migration, set jobs.migrateDatabase.enabled=false in the Helm install below.
# ------------------------------------------------------------

# ------------------ 9. Deploy Airflow (Helm) ------------------
echo "=== 9. Deploy Apache Airflow Helm chart (pointing to external DB) ==="
helm repo add apache-airflow https://airflow.apache.org || true
helm repo update

helm upgrade --install "${AIRFLOW_RELEASE}" apache-airflow/airflow \
  --namespace "${NAMESPACE}" \
  --set executor=KubernetesExecutor \
  --set airflow.image.repository="psalmprax/airflow-custom" \
  --set airflow.image.tag="2.9.2-p1" \
  --set dags.persistence.enabled=false \
  --set dags.gitSync.enabled=true \
  --set dags.gitSync.repo="https://github.com/psalmprax/hybrid-fraud-platform.git" \
  --set dags.gitSync.subPath="2-airflow/dags" \
  --set dags.gitSync.branch="master" \
  --set logs.persistence.enabled=false \
  --set workers.persistence.enabled=false \
  --set jobs.migrateDatabase.enabled=true \
  --set waitForMigrations.timeoutSeconds="${MIGRATE_TIMEOUT}" \
  --set resources.migrations.requests.memory=1Gi \
  --set resources.migrations.requests.cpu=500m \
  --set resources.migrations.limits.memory=2Gi \
  --set resources.migrations.limits.cpu=1 \
  --set postgresql.enabled=false \
  --set redis.enabled=false \
  --set externalDatabase.existingSecret=airflow-db-secret \
  --set webserver.defaultUser.enabled=true \
  --set webserver.defaultUser.username=admin \
  --set webserver.defaultUser.password=admin \
  --set webserver.defaultUser.role=Admin \
  --set webserver.defaultUser.email=admin@example.com
  # --wait --timeout "${HELM_TIMEOUT}"

echo
echo "Deployment triggered. Give pods some minutes to come up."
echo "Check status with: kubectl get pods -n ${NAMESPACE}"
echo "To view logs for broken components, e.g.: kubectl logs -n ${NAMESPACE} <pod-name> -c <container>"
echo
echo "If the migration job times out, you can run a manual migration (example):"
echo "kubectl run airflow-db-migrate -n ${NAMESPACE} --rm -i --tty \\"
echo "  --image apache/airflow:3.0.2 --restart=Never \\"
echo "  --env AIRFLOW__CORE__SQL_ALCHEMY_CONN=${CONN} \\"
echo "  --env AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=${CONN} \\"
echo "  -- airflow db migrate"
echo

echo "===== FINISHED ====="
