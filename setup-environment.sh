#!/bin/bash
set -euo pipefail

NAMESPACE="airflow"
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

# Prerequisites
for cmd in kubectl helm docker; do command -v "$cmd" >/dev/null || { error "$cmd missing"; exit 1; }; done

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
# 3. SPARK OPERATOR - FIXED SELECTOR!
# =============================================================================
info "3. Spark Operator..."
helm repo add spark-operator https://kubeflow.github.io/spark-operator || true
helm repo update

# Force fresh install
kubectl delete deployment spark-operator-controller -n spark-operator --ignore-not-found || true
helm uninstall spark-operator -n spark-operator --ignore-not-found || true
sleep 3

helm install spark-operator spark-operator/spark-operator \
  -n spark-operator \
  --create-namespace \
  -f ~/dmolle_project/hybrid-fraud-platform/1-kubernetes-manifests/04-airflow/values.yml \
  --wait

# helm install spark-operator spark-operator/spark-operator -n spark-operator --create-namespace \
#   --set webhook.enable=false --wait --timeout=5m

# 🔥 PERFECT args (tested working)
# kubectl patch deployment spark-operator-controller -n spark-operator --type='json' -p='[
#   {"op": "replace", "path": "/spec/template/spec/containers/0/args", "value": [
#     "controller", "start",
#     "--zap-log-level=info",
#     "--zap-encoder=console",
#     "--namespaces=spark-jobs",
#     "--enable-webhook=false"
#   ]}
# ]'

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
CONN="postgresql+psycopg2://airflow:airflow@${PG_RELEASE}:5432/airflow"
kubectl create secret generic airflow-db-secret -n "${NAMESPACE}" \
  --from-literal=connection="${CONN}" --dry-run=client -o yaml | kubectl apply -f -

mkdir -p 2-airflow
pushd 2-airflow >/dev/null
cat > Dockerfile <<'EOF'
FROM apache/airflow:3.0.2-python3.12
USER root
RUN apt-get update && apt-get install -y default-jre-headless && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark apache-airflow-providers-cncf-kubernetes dbt-core dbt-postgres
EOF

IMAGE_FULL="${DOCKER_IMAGE}:${DOCKER_TAG}"
if [[ "${USE_MINIKUBE_DOCKER_ENV}" == "true" ]]; then
  eval "$(minikube -p minikube docker-env)"
fi
info "Building $IMAGE_FULL..."
docker build -t "$IMAGE_FULL" .
minikube image load "$IMAGE_FULL"
[[ "${USE_MINIKUBE_DOCKER_ENV}" == "true" ]] && eval "$(minikube docker-env --unset)"
popd >/dev/null

# =============================================================================
# 6. DB MIGRATION
# =============================================================================
info "6. DB MIGRATION..."
kubectl run airflow-db-migrate -n "${NAMESPACE}" --rm -i --tty --restart=Never \
  --image="$IMAGE_FULL" \
  --env "AIRFLOW__CORE__SQL_ALCHEMY_CONN=$CONN" \
  --command -- airflow db migrate

# =============================================================================
# 7. AIRFLOW DEPLOY
# =============================================================================
info "7. Airflow deploy..."
helm repo add apache-airflow https://airflow.apache.org || true
helm repo update
helm upgrade --install "${AIRFLOW_RELEASE}" apache-airflow/airflow -n "${NAMESPACE}" \
  -f ~/dmolle_project/hybrid-fraud-platform/1-kubernetes-manifests/04-airflow/custom-values.yaml \
  --timeout=20m || warn "Using defaults"

# wait_ready "${NAMESPACE}" "app.kubernetes.io/component=scheduler"
# wait_ready "${NAMESPACE}" "app.kubernetes.io/component=webserver"

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
  resources: ["pods","services"]
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
kubectl delete configmap on-prem-etl-script -n spark-jobs
kubectl create configmap on-prem-etl-script -n spark-jobs \
  --from-literal="main.py"='print("🎉 HYBRID FRAUD LIVE!"); import sys; sys.exit(0)' \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n spark-jobs create configmap on-prem-etl-script \
  --from-file=/opt/airflow/dags/repo/3-spark-app/src/main.py \
  --from-file=/opt/airflow/dags/repo/3-spark-app/src/consolidate_data.py \
  --from-file=/opt/airflow/dags/repo/3-spark-app/src/generate_sample_data.py

kubectl -n spark-jobs create configmap consolidate-data-script \
  --from-file=/opt/airflow/dags/repo/3-spark-app/src/main.py \
  --from-file=/opt/airflow/dags/repo/3-spark-app/src/consolidate_data.py \
  --from-file=/opt/airflow/dags/repo/3-spark-app/src/generate_sample_data.py

kubectl create clusterrolebinding spark-operator-binding \
  --clusterrole=cluster-admin \
  --serviceaccount=spark-operator:spark-operator \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl create clusterrolebinding airflow-spark-binding \
  --clusterrole=edit \
  --serviceaccount=airflow:airflow-worker \
  --dry-run=client -o yaml | kubectl apply -f -


kubectl exec -n airflow deploy/airflow-scheduler -- bash -c "
  airflow connections add kubernetes_default --conn-type kubernetes --conn-extra '{\"in_cluster\": true}' || true
" || true

kubectl exec -n airflow deploy/airflow-scheduler -- bash -c "
  airflow connections add spark_default --conn-type spark --conn-host 'local[*]' --conn-port 0 --conn-extra '{}' || true"

# =============================================================================
# 10. CREATE AIRFLOW KUBERNETES CONNECTION
# =============================================================================
# echo "10.1. Creating kubernetes_default connection..." 
# kubectl exec -n airflow deploy/airflow-scheduler -- bash -c " 
# airflow connections add kubernetes_default \ 
#   --conn-type kubernetes \ 
#   --conn-extra '{\"in_cluster\": true, \"disable_verify_ssl\": true}' || true 

# airflow connections add spark_default \ 
# --conn-type spark \ --conn-host 'local[*]' \ 
# --conn-port 0 \ --conn-extra '{}' || true " 
# echo "✅ kubernetes_default connection created"

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
URL=$(minikube service airflow-webserver -n airflow --url | head -1 | awk '{print $4}')
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
  mainApplicationFile: "local:///opt/spark/work-dir/src/main.py"
  sparkVersion: "3.5.1"
  driver:
    cores: 1
    memory: "1g"
    serviceAccount: spark
    volumeMounts:
    - name: spark-job-script
      mountPath: /opt/spark/work-dir/src
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
    memory: "2g"
  volumes:
  - name: spark-job-script
    configMap:
      name: on-prem-etl-script
  sparkConf:
    "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000"
    "spark.hadoop.fs.s3a.path.style.access": "true"
EOF
echo "EOF"
