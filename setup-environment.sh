#!/bin/bash
set -euo pipefail

NAMESPACE="airflow"
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
SPARK_APP_SRC_DIR="${SCRIPT_DIR}/3-spark-app/src"
PG_RELEASE="airflow-postgresql"
AIRFLOW_RELEASE="airflow"
DOCKER_IMAGE="psalmprax/airflow-custom"
DOCKER_TAG="2.9.2-p1"
USE_MINIKUBE_DOCKER_ENV="${USE_MINIKUBE_DOCKER_ENV:-true}"
RED='\033[0;31m' GREEN='\033[0;32m' YELLOW='\033[1;33m' NC='\033[0m'

info() { echo -e "${GREEN}==> $1${NC}"; }
warn() { echo -e "${YELLOW}==> $1${NC}"; }
error() { echo -e "${RED}==> $1${NC}"; }

# FIXED: Label selector or regex pattern
wait_ready() {
  local ns=$1 selector=$2 max=40
  for i in $(seq 1 $max); do
    if kubectl get po -n "$ns" -l "$selector" --no-headers 2>/dev/null | grep Running | grep -v "0/"; then
      info "$selector: READY ✓"
      return 0
    fi
    printf "⏳ %s... (%ds/%ds)\r" "$selector" $i $max
    sleep 3
  done
  error "$selector TIMEOUT"
  kubectl get po -n "$ns" -A
  exit 1
}

info "🚀 HYBRID FRAUD PLATFORM - SPARK SELECTOR FIXED"

# # Stop and delete any existing Minikube cluster
# echo "⏹️ Stopping Minikube..."
# minikube stop || true

echo "🗑️ Deleting Minikube..."
minikube delete || true

# # Ensure current user is in the docker group
# echo "👤 Adding $USER to docker group..."
# sudo usermod -aG docker "$USER"

# Refresh group membership for current shell
echo "🔄 Refreshing docker group membership..."
# newgrp docker <<EONG

# # Start Minikube with custom resources
# echo "🚀 Starting Minikube with 6 CPUs, 16GB RAM, 50GB disk..."
minikube start \
  --cpus=6 \
  --memory=16384 \
  --disk-size=50g \
  --driver=docker \
  --kubernetes-version=v1.30.0

# Enable ingress addon
echo "🌐 Enabling Minikube ingress addon..."
minikube addons enable ingress

# # Run environment setup script
# echo "⚙️ Running setup-environment.sh..."
# ./setup-environment.sh

# EONG

echo "✅ Minikube environment setup complete!"


# Prerequisites
for cmd in kubectl helm docker; do command -v "$cmd" >/dev/null || { error "$cmd missing"; exit 1; }; done

# --- Install MinIO Client (mcli) if not present ---
info "Checking for MinIO client (mcli)..."
LOCAL_BIN_DIR="${SCRIPT_DIR}/.bin"
export PATH="${LOCAL_BIN_DIR}:${PATH}" # Add local bin to PATH for this script's execution

if ! command -v mcli &> /dev/null; then
    warn "MinIO client 'mcli' not found. Attempting to install it locally to ${LOCAL_BIN_DIR}"
    mkdir -p "${LOCAL_BIN_DIR}"
    if curl -Lo "${LOCAL_BIN_DIR}/mcli" https://dl.min.io/client/mc/release/linux-amd64/mc; then
        chmod +x "${LOCAL_BIN_DIR}/mcli"
        info "✅ MinIO client 'mcli' installed successfully."
    else
        error "❌ Failed to download MinIO client. Please install it manually." && exit 1
    fi
fi
# =============================================================================
# 0. CLEANUP
# =============================================================================
info "0. Nuclear cleanup..."
set +e
helm uninstall spark-operator -n spark-operator --ignore-not-found || true
helm uninstall "${PG_RELEASE}" -n "${NAMESPACE}" --ignore-not-found || true
helm uninstall "${AIRFLOW_RELEASE}" -n "${NAMESPACE}" --ignore-not-found || true
kubectl delete ns spark-operator spark-jobs storage kafka airflow --ignore-not-found || true
kubectl delete crd sparkapplications.sparkoperator.k8s.io scheduledsparkapplications.sparkoperator.k8s.io --ignore-not-found || true
set -e

# =============================================================================
# 1. NAMESPACES
# =============================================================================
info "1. Namespaces..."
for ns in airflow spark-jobs spark-operator storage kafka; do
  kubectl create ns "$ns" --dry-run=client -o yaml | kubectl apply -f -
done

# =============================================================================
# 2. MINIO
# =============================================================================
info "2. MinIO..."
kubectl apply -f 1-kubernetes-manifests/03-minio/minio-statefulset.yaml -n storage || true
kubectl create secret generic minio-credentials -n spark-jobs \
  --from-literal=accesskey=minio --from-literal=secretkey=minio123 --dry-run=client -o yaml | kubectl apply -f -
wait_ready storage "app=minio"

# =============================================================================
# 2.1 KAFKA (STRIMZI)
# =============================================================================
info "2.1. Kafka (Strimzi)..."
helm repo add strimzi https://strimzi.io/charts/ || true
helm repo update
helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
  -n kafka --create-namespace --version 0.49.0 --wait --timeout 30m

info "Deploying Kafka cluster..."
# Using a dedicated YAML file is cleaner and more maintainable.
kubectl apply -f 1-kubernetes-manifests/02-kafka-strimzi/kafka-cluster.yaml -n kafka

info "Waiting for Kafka cluster to be ready..."
kubectl wait kafka/my-kafka-cluster --for=condition=Ready -n kafka --timeout=15m

info "Creating Kafka topic 'payment-events'..."
kubectl apply -f 1-kubernetes-manifests/02-kafka-strimzi/kafka-topic-payments.yaml -n kafka
kubectl wait kafkatopic/paymentevents --for=condition=Ready -n kafka --timeout=2m

# =============================================================================
# 3. SPARK OPERATOR - FIXED SELECTOR!
# =============================================================================
info "3. Spark Operator..."
helm repo add spark-operator https://kubeflow.github.io/spark-operator || true
helm repo update

# Force fresh install
kubectl delete deployment spark-operator-controller -n spark-operator --ignore-not-found || true
helm uninstall spark-operator -n spark-operator --ignore-not-found || true
sleep 3

# The default installation can time out waiting for the webhook to become ready.
# Disabling the webhook is a common and reliable fix for local/Minikube environments.
helm upgrade --install spark-operator spark-operator/spark-operator \
  -n spark-operator --create-namespace \
  -f "${SCRIPT_DIR}/1-kubernetes-manifests/04-airflow/values.yml" \
  --wait --timeout=275m

info "Restarting Spark Operator deployment to pick up namespace changes..."
kubectl rollout restart deployment/spark-operator-controller -n spark-operator
# FIXED: Correct label selector!
wait_ready spark-operator "app.kubernetes.io/name=spark-operator"

info "✅ Spark Operator: --namespaces=spark-jobs ✓"
kubectl get deployment spark-operator-controller -n spark-operator -o yaml | grep namespaces

# =============================================================================
# 4. POSTGRESQL
# =============================================================================
info "4. PostgreSQL..."
helm repo add bitnami https://charts.bitnami.com/bitnami || true
helm repo update
helm upgrade --install "${PG_RELEASE}" bitnami/postgresql -n "${NAMESPACE}" \
  --set auth.username=airflow --set auth.password=airflow \
  --set auth.postgresPassword=postgres --set auth.database=airflow \
  --set primary.persistence.enabled=true --set primary.persistence.size=8Gi \
  --wait --timeout=10m

wait_ready "${NAMESPACE}" "app.kubernetes.io/name=postgresql"

# =============================================================================
# 5. CUSTOM AIRFLOW IMAGE
# =============================================================================
info "5. CUSTOM AIRFLOW BUILD..."

# Set the Docker environment to Minikube's daemon.
# All subsequent 'docker build' commands will build images directly inside Minikube.
if [[ "${USE_MINIKUBE_DOCKER_ENV}" == "true" ]]; then
  info "Setting Docker environment to Minikube's context..."
  eval "$(minikube -p minikube docker-env)"
  info "Logging into Docker Hub from Minikube's Docker daemon..."
  # This will prompt for your Docker Hub username and password.
  # Ensure you are logged in to Docker Hub on your host machine first,
  # or provide credentials via environment variables if running non-interactively.
  docker login
fi

# --- 5.1 Build Custom Airflow Image ---
info "5.1. Building custom Airflow image..."
AIRFLOW_IMAGE_FULL="${DOCKER_IMAGE}:${DOCKER_TAG}"
AIRFLOW_BUILD_CONTEXT="${SCRIPT_DIR}/2-airflow" # Use the existing Dockerfile
info "Building ${AIRFLOW_IMAGE_FULL} inside Minikube..."
info "Using Dockerfile from: ${AIRFLOW_BUILD_CONTEXT}/Dockerfile"
docker build -t "${AIRFLOW_IMAGE_FULL}" "${AIRFLOW_BUILD_CONTEXT}"

# --- 5.2 Build Spark Test Image ---
info "5.2. Building Spark test image..."
SPARK_TEST_CONTEXT_DIR="${SCRIPT_DIR}/spark-test-app"
SPARK_TEST_IMAGE_NAME="psalmprax/spark-test:3.4.1"
info "Building ${SPARK_TEST_IMAGE_NAME} inside Minikube..."
docker build -t "${SPARK_TEST_IMAGE_NAME}" -f "${SPARK_TEST_CONTEXT_DIR}/Dockerfile" "${SPARK_TEST_CONTEXT_DIR}"

# --- 5.3 Build Spark Main App Image ---
info "5.3. Building Spark main application image..."
SPARK_APP_CONTEXT_DIR="${SCRIPT_DIR}/3-spark-app"
SPARK_APP_IMAGE_NAME="psalmprax/spark-app:1.0.0"
info "Building ${SPARK_APP_IMAGE_NAME} inside Minikube..."
docker build -t "${SPARK_APP_IMAGE_NAME}" -f "${SPARK_APP_CONTEXT_DIR}/Dockerfile" "${SPARK_APP_CONTEXT_DIR}"

# Unset the Minikube Docker environment to return to the host's daemon
if [[ "${USE_MINIKUBE_DOCKER_ENV}" == "true" ]]; then
  info "Unsetting Minikube Docker environment."
  eval "$(minikube docker-env --unset)"
fi

# =============================================================================
# 6. CREATE DB SECRET & DEPLOY AIRFLOW
# =============================================================================
info "6. Creating DB secret and deploying Airflow..."
CONN="postgresql+psycopg2://airflow:airflow@${PG_RELEASE}.airflow.svc.cluster.local:5432/airflow"
kubectl create secret generic airflow-db-secret -n "${NAMESPACE}" \
  --from-literal=connection="${CONN}" --dry-run=client -o yaml | kubectl apply -f -

# =============================================================================
# 7. AIRFLOW DEPLOY
# =============================================================================
info "7. Airflow deploy..."
helm repo add apache-airflow https://airflow.apache.org || true
helm repo update
helm upgrade --install "${AIRFLOW_RELEASE}" apache-airflow/airflow -n "${NAMESPACE}" \
  --set postgresql.enabled=false \
  --set redis.enabled=false \
  -f "${SCRIPT_DIR}/1-kubernetes-manifests/04-airflow/custom-values.yaml" --wait --timeout=30m


# =============================================================================
# 8. 🔥 COMPLETE RBAC - AIRFLOW → SPARK-JOBS!
# =============================================================================
info "8. COMPLETE RBAC..."
# Spark job permissions
kubectl apply -n spark-jobs -f - <<'EOF'
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-role
rules:
- apiGroups: [""]
  resources: ["pods", "services", "persistentvolumeclaims", "configmaps"]
  verbs: ["*"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-rolebinding
subjects:
- kind: ServiceAccount
  name: spark
roleRef:
  kind: Role
  name: spark-role
  apiGroup: rbac.authorization.k8s.io
EOF

# 🔥 CRITICAL: Airflow worker → spark-jobs permissions
kubectl apply -f - <<EOF
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: airflow-spark-job-role
  namespace: spark-jobs
rules:
- apiGroups: [""]
  resources: ["pods","services","events"]
  verbs: ["*"]
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications","sparkapplications/status"]
  verbs: ["*","watch","list","get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: airflow-spark-job-binding
  namespace: spark-jobs
subjects:
- kind: ServiceAccount
  name: airflow-worker
  namespace: ${NAMESPACE}
roleRef:
  kind: Role
  name: airflow-spark-job-role
  apiGroup: rbac.authorization.k8s.io
EOF

# Cluster-wide Spark CRD access
kubectl apply -f - <<EOF
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: airflow-spark-crd-role
rules:
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications","sparkapplications/status"]
  verbs: ["*"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: airflow-spark-crd-binding
subjects:
- kind: ServiceAccount
  name: airflow-worker
  namespace: ${NAMESPACE}
roleRef:
  kind: ClusterRole
  name: airflow-spark-crd-role
  apiGroup: rbac.authorization.k8s.io
EOF

# =============================================================================
# 9. TEST SPARK JOB
# =============================================================================
info "9. TEST SPARK JOB..."
kubectl delete configmap on-prem-etl-script -n spark-jobs --ignore-not-found || true
kubectl delete configmap consolidate-data-script -n spark-jobs --ignore-not-found || true
kubectl delete configmap generate-sample-data-script -n spark-jobs --ignore-not-found || true

./scripts/create_spark_configmaps.sh --namespace spark-jobs --root "$SCRIPT_DIR"/

kubectl create clusterrolebinding spark-operator-binding \
  --clusterrole=cluster-admin \
  --serviceaccount=spark-operator:spark-operator \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl create clusterrolebinding airflow-spark-binding \
  --clusterrole=edit \
  --serviceaccount=airflow:airflow-worker \
  --dry-run=client -o yaml | kubectl apply -f -

# =============================================================================
# 10. CREATE AIRFLOW KUBERNETES CONNECTION
# =============================================================================

# Create kubernetes_default
kubectl exec -n airflow deploy/airflow-scheduler -- bash -c "
  airflow connections delete kubernetes_default || true
  airflow connections add kubernetes_default \
    --conn-type kubernetes \
    --conn-extra '{\"in_cluster\": true}'
" || true

# Create spark_default
kubectl exec -n airflow deploy/airflow-scheduler -- bash -c "
  airflow connections delete spark_default || true
  airflow connections add spark_default \
    --conn-type spark \
    --conn-host \"local[*]\" \
    --conn-port 0 \
    --conn-extra '{}'
" || true

# Create postgres_default for Spark jobs to connect to the DB
kubectl exec -n airflow deploy/airflow-scheduler -- bash -c "
  airflow connections delete postgres_default || true
  airflow connections add postgres_default \
    --conn-type postgres \
    --conn-host airflow-postgresql.airflow.svc.cluster.local \
    --conn-login airflow \
    --conn-password airflow \
    --conn-schema airflow
" || true

# =============================================================================
# CONFIGURE MINIO CLIENT AND CREATE BUCKETS
# =============================================================================
echo ">>> Configuring MinIO client and creating buckets..."
info "Checking for MinIO client (mc or mcli)..."
MC_COMMAND=""
if command -v mcli &> /dev/null; then
    MC_COMMAND="mcli"
    info "✅ Found MinIO client as 'mcli'."
elif command -v mc &> /dev/null; then
    # Verify 'mc' is not Midnight Commander by checking its help output
    if mc --help 2>&1 | grep -q "MinIO"; then
        MC_COMMAND="mc"
        info "✅ Found MinIO client as 'mc'."
    else
        warn "'mc' command is installed but appears to be Midnight Commander, not the MinIO client."
    fi
fi

if [ -z "$MC_COMMAND" ]; then
    error "MinIO client not found."
    error "Please install the MinIO client, preferably as 'mcli' to avoid conflicts."
    error "See: https://min.io/docs/minio/linux/reference/minio-client.html"
    exit 1
fi

# 1. Get MinIO service URL
export MINIO_URL=$(minikube service minio -n storage --url)
if [ -z "$MINIO_URL" ]; then
    echo "❌ Could not get MinIO service URL. Please check if MinIO is running."
    exit 1
fi
info "MinIO URL found: $MINIO_URL"

# 2. Configure MinIO Client (mc) alias
$MC_COMMAND alias set myminio "$MINIO_URL" minio minio123 --api S3v4

# 3. Create 'bronze' and 'silver' buckets if they don't exist
for bucket in bronze silver datalake; do
    if $MC_COMMAND ls myminio/$bucket > /dev/null 2>&1; then
        echo "✅ Bucket '$bucket' already exists."
    else
        echo "Creating bucket: $bucket"
        $MC_COMMAND mb myminio/$bucket
        $MC_COMMAND policy set public myminio/$bucket
        echo "✅ Bucket '$bucket' created and set to public."
    fi
done

echo ">>> MinIO configuration complete."

# =============================================================================
# Create spark pvc for data generation
# =============================================================================
kubectl apply -f 1-kubernetes-manifests/05-spark/spark-pvc.yaml

# =============================================================================
# SUCCESS
# =============================================================================
info "🎉 PRODUCTION LIVE! 🎉"
echo
echo "📊 STATUS:"
echo "  Airflow:     kubectl get po -n airflow"
echo "  Spark:       kubectl get po -n spark-operator"
echo "  Jobs:        kubectl get all -n spark-jobs"
echo
URL=$(minikube service airflow-api-server -n airflow --url | head -1 | awk '{print $4}')
echo "🌐 AIRFLOW UI: $URL"
echo
echo "🧪 TEST 3 PODS:"
echo "kubectl get pods -n spark-jobs -w &"
echo "kubectl apply -f - <<EOF"
cat <<'EOF'
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: test-hybrid
  namespace: spark-jobs
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: "apache/spark:3.5.1-python3"
  mainApplicationFile: "local:///opt/spark/work-dir/main.py"
  sparkVersion: "3.5.1"
  restartPolicy:
    type: Never
  volumes:
  - name: spark-job-script
    configMap:
      name: on-prem-etl-script
  driver:
    cores: 1
    memory: "1g"
    serviceAccount: spark
    volumeMounts:
    - name: spark-job-script
      mountPath: /opt/spark/work-dir
    env:
    - name: AWS_ACCESS_KEY_ID
      valueFrom:
        secretKeyRef:
          name: minio-credentials
          key: accesskey
    - name: AWS_SECRET_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: minio-credentials
          key: secretkey
  executor:
    cores: 1
    instances: 2
    memory: "1g"
  sparkConf:
    "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000"
    "spark.hadoop.fs.s3a.path.style.access": "true"
    # spark.jars.packages is no longer needed as the JARs are baked into the image.
    # "spark.kubernetes.executor.volumes.configMap.spark-job-script.mount.path": "/opt/spark/work-dir"
    # "spark.kubernetes.executor.volumes.configMap.spark-job-script.mount.readOnly": "true"
    # "spark.kubernetes.executor.volumes.configMap.spark-job-script.options.name": "on-prem-etl-script"
EOF
echo "EOF"
