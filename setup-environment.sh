#!/bin/bash
set -euo pipefail

# =============================================================================
# AIRFLOW + INFRASTRUCTURE DEPLOYMENT SCRIPT
# =============================================================================

# -------- CONFIGURATION ---------
NAMESPACE="airflow"
PG_RELEASE="airflow-postgresql"
AIRFLOW_RELEASE="airflow"

# PostgreSQL
PG_CHART="bitnami/postgresql"
PG_IMAGE_REPO="bitnami/postgresql"
PG_IMAGE_TAG="latest"
PG_PERSISTENCE_SIZE="8Gi"

# Airflow Helm Chart
AIRFLOW_CHART="apache-airflow/airflow"

# Custom Airflow Image
DOCKER_IMAGE="psalmprax/airflow-custom"
DOCKER_TAG="2.9.2-p1"
USE_MINIKUBE_DOCKER_ENV="${USE_MINIKUBE_DOCKER_ENV:-true}"
PUSH_TO_REGISTRY="${PUSH_TO_REGISTRY:-true}"
DOCKER_USERNAME="${DOCKER_USERNAME:-psalmprax}"

# GitSync
GIT_REPO="https://github.com/dollegit/hybrid-fraud-platform.git"
GIT_BRANCH="master"
GIT_SUBPATH="2-airflow/dags"
GIT_SYNC_SECRET_NAME=""

# Timeouts
MIGRATE_TIMEOUT="300"
HELM_TIMEOUT="20m"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

info() { echo -e "${GREEN}==> $1${NC}"; }
warn() { echo -e "${YELLOW}==> $1${NC}"; }
error() { echo -e "${RED}==> $1${NC}"; }

# =============================================================================
# PREFLIGHT CHECKS
# =============================================================================

info "Starting Airflow + Infrastructure deployment..."

# Check prerequisites
for cmd in kubectl helm docker; do
  if ! command -v "$cmd" &>/dev/null; then
    error "'$cmd' is required but not installed/available in PATH"
    exit 1
  fi
done

# =============================================================================
# 0. CLEANUP
# =============================================================================

info "0. Cleaning up previous resources..."
set +e
kubectl delete secret airflow-db-secret -n "${NAMESPACE}" --ignore-not-found=true
kubectl delete secret "${PG_RELEASE}" -n "${NAMESPACE}" --ignore-not-found=true
kubectl delete secret minio-credentials -n default --ignore-not-found=true

# Cleanup Helm release secrets
kubectl get secrets -n "${NAMESPACE}" -o name 2>/dev/null | 
  grep -E "sh.helm.release.v1.${PG_RELEASE}|sh.helm.release.v1.${AIRFLOW_RELEASE}" | 
  xargs -r kubectl delete -n "${NAMESPACE}" --ignore-not-found=true

kubectl delete statefulset "${PG_RELEASE}" -n "${NAMESPACE}" --ignore-not-found=true
kubectl delete svc "${PG_RELEASE}" -n "${NAMESPACE}" --ignore-not-found=true

helm uninstall "${PG_RELEASE}" -n "${NAMESPACE}" --ignore-not-found=true
helm uninstall "${AIRFLOW_RELEASE}" -n "${NAMESPACE}" --ignore-not-found=true
helm uninstall spark -n spark --ignore-not-found=true

helm uninstall spark-operator -n spark-operator --ignore-not-found=true
set -e

# ------------------ 1. NAMESPACE ------------------
echo "=== 1. Ensure namespace exists ==="
if [[ -f 1-kubernetes-manifests/01-namespaces.yaml ]]; then
  kubectl apply -f 1-kubernetes-manifests/01-namespaces.yaml
else
  kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
fi
echo

# =============================================================================
# 2. INFRASTRUCTURE (Optional)
# =============================================================================

install_strimzi() {
  info "2. Installing Strimzi Kafka Operator..."
  helm repo add strimzi https://strimzi.io/charts/ || true
  helm repo update
  helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
    -n kafka --create-namespace --version 0.49.0 --wait --timeout 30m
}

install_kafka() {
  info "3. Deploying Kafka cluster..."
  kubectl apply -f 1-kubernetes-manifests/02-kafka-strimzi/kafka-cluster.yaml -n kafka || true
}

install_minio() {
  info "4. Deploying MinIO..."
  kubectl apply -f 1-kubernetes-manifests/03-minio/minio-statefulset.yaml -n storage || true
  kubectl create secret generic minio-credentials -n default \
  --from-literal=accesskey=minio \
  --from-literal=secretkey=minio123

  # Verify MinIO secret exists
  kubectl get secret minio-credentials -n spark-jobs || echo "Create minio-credentials secret"
}

install_spark_operator() {
  info "5. Installing Spark Operator..."
  helm repo add spark-operator https://kubeflow.github.io/spark-operator || true
  helm repo update
  helm upgrade --install spark-operator spark-operator/spark-operator \
    --namespace spark-operator --create-namespace \
    --set namespaces="default,spark-jobs" \
    --set webhook.healthProbe.port=8080 \
    --wait --timeout 5m

  
  kubectl create namespace spark-jobs --dry-run=client -o yaml | kubectl apply -f -

  info "5.1. Granting Spark Operator permissions in spark-jobs namespace..."
  cat <<EOF | kubectl apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-operator-role
  namespace: spark-jobs
rules:
- apiGroups: [""]
  resources: ["pods", "services", "configmaps", "secrets"]
  verbs: ["create", "get", "watch", "list", "delete", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-operator-role-binding
  namespace: spark-jobs
subjects:
- kind: ServiceAccount
  name: spark-operator # Assumes default SA name from Helm chart
  namespace: spark-operator
roleRef:
  kind: Role
  name: spark-operator-role
  apiGroup: rbac.authorization.k8s.io
EOF
}

install_spark_cluster() {
  info "5.1 Installing Spark Standalone Cluster..."
  helm repo add bitnami https://charts.bitnami.com/bitnami || true
  helm repo update
  
  kubectl create namespace spark --dry-run=client -o yaml | kubectl apply -f -

  info "5.2 Pulling Spark image to host and loading into Minikube..."
  # Pull the image to the host's Docker daemon first
  docker pull docker.io/bitnami/spark:3.5.1-debian-12-r13
  # Now, load the image from the host into the Minikube cluster
  minikube image load docker.io/bitnami/spark:3.5.1-debian-12-r13

  info "5.3 Installing Spark Standalone Cluster via Helm..."
  # Using the Bitnami chart with the compatible Bitnami image
  helm upgrade --install spark bitnami/spark \
    --namespace spark \
    --set image.registry=docker.io \
    --set image.repository=bitnami/spark \
    --set image.tag=3.5.1-debian-12-r13 \
    --set image.pullPolicy=IfNotPresent \
    --set global.security.allowInsecureImages=true \
    --set worker.replicas=2 \
    --set master.resources.requests.cpu=1 \
    --set master.resources.requests.memory=2Gi \
    --set master.resources.limits.cpu=2 \
    --set master.resources.limits.memory=4Gi \
    --set worker.resources.requests.cpu=1 \
    --set worker.resources.requests.memory=2Gi \
    --set worker.resources.limits.cpu=2 \
    --set worker.resources.limits.memory=4Gi \
    --wait --timeout 10m
}

# Run infrastructure installs
install_strimzi
install_kafka
install_minio
# The following are disabled to use local Spark within Airflow workers
install_spark_operator
# Create token secret for spark SA
kubectl apply -n spark-jobs -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: spark-token
  namespace: spark-jobs
  annotations:
    kubernetes.io/service-account.name: spark
type: kubernetes.io/service-account-token
EOF
# install_spark_cluster

# =============================================================================
# 6. POSTGRESQL
# =============================================================================

info "6. Deploying PostgreSQL..."
helm repo add bitnami https://charts.bitnami.com/bitnami || true
helm repo update

helm upgrade --install "${PG_RELEASE}" "${PG_CHART}" \
  --namespace "${NAMESPACE}" \
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

kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=postgresql -n "${NAMESPACE}" --timeout=5m

# =============================================================================
# 7. AIRFLOW DB SECRET
# =============================================================================

info "7. Creating Airflow DB secret..."
kubectl delete secret airflow-db-secret -n "${NAMESPACE}" --ignore-not-found=true
CONN="postgresql+psycopg2://airflow:airflow@${PG_RELEASE}:5432/airflow"
kubectl create secret generic airflow-db-secret \
  -n "${NAMESPACE}" --from-literal=connection="${CONN}"

# =============================================================================
# 8. BUILD CUSTOM AIRFLOW IMAGE
# =============================================================================

info "8. Building custom Airflow image..."
pushd 2-airflow >/dev/null

# Create Dockerfile if missing
if [[ ! -f Dockerfile ]]; then
  cat > Dockerfile <<'EOF'
FROM apache/airflow:3.0.2-python3.12

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Use the Python venv pip to install packages inside virtualenv as airflow user
USER airflow

RUN python -m pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-cncf-kubernetes \
    apache-airflow-providers-dbt-cloud \
    dbt-core \
    dbt-postgres
EOF
fi

IMAGE_FULL="${DOCKER_IMAGE}:${DOCKER_TAG}"

# Minikube docker-env setup
if [[ "${USE_MINIKUBE_DOCKER_ENV}" == "true" ]]; then
  eval "$(minikube -p minikube docker-env)"
fi

docker build -t "${IMAGE_FULL}" .

if [[ "${PUSH_TO_REGISTRY}" == "true" ]]; then
  docker push "${IMAGE_FULL}"
fi

popd >/dev/null

# Cleanup docker-env
if [[ "${USE_MINIKUBE_DOCKER_ENV}" == "true" ]]; then
  eval "$(minikube -p minikube docker-env --unset)" || true
fi

# =============================================================================
# 9. MANUAL DB MIGRATION
# =============================================================================

info "9. Running manual DB migration..."
kubectl run airflow-db-migrate -n "${NAMESPACE}" --rm -i --tty \
  --image "${IMAGE_FULL}" --restart=Never \
  --env "AIRFLOW__CORE__SQL_ALCHEMY_CONN=${CONN}" \
  --env "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=${CONN}" \
  --command -- airflow db migrate

# ---------------------------------------
# 10_Prebuild. CREATE RBAC FOR AIRFLOW + SPARK OPERATOR
# ---------------------------------------
echo "10_Prebuild. Creating RBAC for Airflow SparkKubernetesOperator..."

cat <<EOF | kubectl apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: airflow-spark-role
  namespace: spark-jobs
rules:
- apiGroups: [""]
  resources: ["pods", "pods/log", "services"]
  verbs: ["get", "list", "watch", "create", "delete", "patch"]
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications", "sparkapplications/status"]
  verbs: ["get", "list", "watch", "create", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: airflow-spark-rolebinding
  namespace: spark-jobs
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: airflow-spark-role
subjects:
- kind: ServiceAccount
  name: airflow-worker
  namespace: airflow
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  namespace: spark-jobs
  name: airflow-spark-viewer
rules:
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  namespace: spark-jobs
  name: airflow-spark-viewer-binding
subjects:
- kind: ServiceAccount
  name: airflow-worker
  namespace: airflow
roleRef:
  kind: Role
  name: airflow-spark-viewer
  apiGroup: rbac.authorization.k8s.io
EOF

echo "✅ RBAC for spark-jobs namespace created"

# =============================================================================
# 10. AIRFLOW HELM CHART
# =============================================================================

info "10. Deploying Airflow Helm chart..."
helm repo add apache-airflow https://airflow.apache.org || true
helm repo update

helm upgrade --install "${AIRFLOW_RELEASE}" "${AIRFLOW_CHART}" \
  -n "${NAMESPACE}" \
  -f ~/dmolle_project/hybrid-fraud-platform/1-kubernetes-manifests/04-airflow/custom-values.yaml \
  --timeout "${HELM_TIMEOUT}"

# ---------------------------------------
# 10.1. CREATE AIRFLOW KUBERNETES CONNECTION
# ---------------------------------------
echo "10.1. Creating kubernetes_default connection..."

kubectl exec -n airflow deploy/airflow-scheduler -- bash -c "
airflow connections add kubernetes_default \
  --conn-type kubernetes \
  --conn-extra '{\"in_cluster\": true, \"disable_verify_ssl\": true}' || true

airflow connections add spark_default \
  --conn-type spark \
  --conn-host 'local[*]' \
  --conn-port 0 \
  --conn-extra '{}' || true
"

echo "✅ kubernetes_default connection created"

# =============================================================================
# SUMMARY
# =============================================================================

info "Deployment completed successfully!"
echo
echo "Check status:"
echo "  kubectl get pods -n ${NAMESPACE}"
echo "  kubectl get all -n spark-jobs"
echo "Check logs:"
echo "  kubectl logs -n ${NAMESPACE} <pod-name> -c tainer>"
echo "Access Airflow UI: $(minikube service airflow-webserver -n ${NAMESPACE} --url)"
