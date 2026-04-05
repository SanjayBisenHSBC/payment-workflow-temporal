# OpenShift On-Premises Deployment Guide
## Payment Workflow Service — Temporal + Spring Boot + PostgreSQL + Kafka

---

## Table of Contents
1. [Architecture on OpenShift](#1-architecture-on-openshift)
2. [Prerequisites](#2-prerequisites)
3. [Step-by-Step Deployment](#3-step-by-step-deployment)
4. [Build and Push the Container Image](#4-build-and-push-the-container-image)
5. [Deploy Infrastructure (Postgres, Kafka, Temporal)](#5-deploy-infrastructure)
6. [Deploy the Application](#6-deploy-the-application)
7. [Verify the Deployment](#7-verify-the-deployment)
8. [CI/CD Pipeline (Jenkins / Tekton)](#8-cicd-pipeline)
9. [Operations Runbook](#9-operations-runbook)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Architecture on OpenShift

```
                        ┌──────────────────────────────────────────────────┐
                        │           OpenShift Cluster (On-Premises)        │
                        │           Namespace: payment-system              │
                        │                                                  │
  External Traffic      │  ┌─────────────┐    ┌─────────────────────────┐ │
  ─────────────────────►│  │   HAProxy   │    │  Temporal Web UI Route  │ │
  HTTPS                 │  │   Router    │    │  temporal-ui.apps.xxx   │ │
                        │  └──────┬──────┘    └────────────┬────────────┘ │
                        │         │                         │              │
                        │  ┌──────▼──────────────────────┐ │              │
                        │  │   payment-workflow Service   │ │              │
                        │  └──────┬──────────────────────┘ │              │
                        │         │  (ClusterIP)            │              │
                        │  ┌──────▼──────────────────────┐ │              │
                        │  │  payment-workflow Deployment │ │              │
                        │  │  (2-10 replicas via HPA)    │ │              │
                        │  │                             │ │              │
                        │  │  ┌─────────────────────┐   │ │              │
                        │  │  │  Spring Boot App     │   │ │              │
                        │  │  │  REST API            │   │ │              │
                        │  │  │  Temporal Worker     │   │ │              │
                        │  │  └──────────┬──────────┘   │ │              │
                        │  └────────────┼───────────────┘ │              │
                        │               │                  │              │
                        │    ┌──────────▼──┐  ┌───────────▼──────────┐  │
                        │    │  PostgreSQL  │  │  Temporal Server     │  │
                        │    │  StatefulSet │  │  Deployment          │  │
                        │    │  (paymentdb) │  │  gRPC :7233          │  │
                        │    └─────────────┘  └──────────────────────┘  │
                        │                                                  │
                        │    ┌────────────────────────────────────────┐   │
                        │    │  Kafka Cluster (Strimzi Operator)      │   │
                        │    │  3 brokers  |  3 topics                │   │
                        │    │  payment.fraud.request                 │   │
                        │    │  payment.fraud.response                │   │
                        │    │  payment.events                        │   │
                        │    └────────────────────────────────────────┘   │
                        └──────────────────────────────────────────────────┘
```

### Component Summary

| Component | Type | Replicas | Storage |
|---|---|---|---|
| payment-workflow | Deployment | 2–10 (HPA) | None (stateless) |
| payment-postgres | StatefulSet | 1 | 20 Gi PVC |
| payment-temporal | Deployment | 1 | ConfigMap volume |
| payment-kafka | Strimzi Kafka | 3 brokers | 50 Gi PVC each |
| Zookeeper | Strimzi managed | 3 | 10 Gi PVC each |

---

## 2. Prerequisites

### On your workstation

```bash
# OpenShift CLI
brew install openshift-cli        # Mac
# OR download from: https://mirror.openshift.com/pub/openshift-v4/clients/ocp/

# Docker or Podman (for building the container image)
brew install --cask docker
# OR: brew install podman

# Verify
oc version
docker version
```

### On the OpenShift cluster (ask your cluster admin)

- OpenShift 4.10+ cluster with admin or namespace-admin access
- **Strimzi / AMQ Streams Operator** installed cluster-wide or in `payment-system` namespace
- A StorageClass available for PersistentVolumeClaims (NFS, Ceph, vSphere, etc.)
- Access to an internal container registry (OpenShift's built-in: `image-registry.openshift-image-registry.svc:5000`)
- Sufficient quota in `payment-system` namespace: ~8 vCPU, ~12 Gi RAM

### Verify cluster access

```bash
oc login https://api.your-cluster.example.com:6443 \
    --username=your-username \
    --password=your-password

oc whoami           # Should print your username
oc get nodes        # Should list cluster nodes
```

---

## 3. Step-by-Step Deployment

The deployment order matters — each component depends on the previous one being healthy:

```
Step 1 → Namespace + RBAC
Step 2 → PostgreSQL  (Temporal and the app both need it)
Step 3 → Kafka       (Strimzi operator creates the cluster)
Step 4 → Temporal    (depends on PostgreSQL)
Step 5 → Application (depends on PostgreSQL + Kafka + Temporal)
```

---

## 4. Build and Push the Container Image

### Option A — Build locally, push to OpenShift internal registry

```bash
# From the project root directory
cd payment-workflow

# 1. Build the JAR
mvn clean package -DskipTests

# 2. Log in to OpenShift internal registry
# First, expose the registry route (ask admin if not already done):
#   oc patch configs.imageregistry.operator.openshift.io/cluster \
#     --type merge --patch '{"spec":{"defaultRoute":true}}'
REGISTRY=$(oc get route default-route -n openshift-image-registry \
    -o jsonpath='{.spec.host}')
echo "Registry: $REGISTRY"

docker login -u $(oc whoami) -p $(oc whoami --show-token) $REGISTRY

# 3. Build the Docker image
docker build -t $REGISTRY/payment-system/payment-workflow:1.0.0 .

# 4. Push to OpenShift registry
docker push $REGISTRY/payment-system/payment-workflow:1.0.0
```

### Option B — Build using OpenShift BuildConfig (server-side build)

```bash
# Create a BuildConfig that builds from the Git repo
oc new-build \
    --name=payment-workflow \
    --binary=true \
    --strategy=docker \
    --to=payment-workflow:1.0.0 \
    -n payment-system

# Trigger a build by pushing the local directory
oc start-build payment-workflow \
    --from-dir=. \
    --follow \
    -n payment-system
```

### Update the Deployment image reference

Edit `openshift/app/deployment.yaml` and replace the image placeholder:

```yaml
# Before:
image: payment-workflow:1.0.0

# After (internal registry):
image: image-registry.openshift-image-registry.svc:5000/payment-system/payment-workflow:1.0.0
```

---

## 5. Deploy Infrastructure

### 5.1 Create Namespace and RBAC

```bash
oc apply -f openshift/namespace-rbac.yaml

# Verify namespace is Active
oc get namespace payment-system
```

### 5.2 Deploy PostgreSQL

```bash
# Apply all PostgreSQL resources
oc apply -f openshift/postgres/postgres.yaml -n payment-system

# Watch the pod come up
oc get pods -l app=payment-postgres -n payment-system -w

# Wait until READY: 1/1
# NAME                  READY   STATUS    RESTARTS   AGE
# payment-postgres-0    1/1     Running   0          45s

# Verify the database is accessible
oc exec -it payment-postgres-0 -n payment-system -- \
    psql -U payment_user -d paymentdb -c "SELECT version();"
```

### 5.3 Deploy Kafka (Strimzi Operator)

```bash
# Step 1 — Install the Strimzi operator (if not already installed by cluster admin)
# From OperatorHub in the OpenShift web console:
#   Operators → OperatorHub → search "AMQ Streams" or "Strimzi" → Install
#   Select namespace: payment-system

# OR via CLI (community Strimzi):
oc apply -f "https://strimzi.io/install/latest?namespace=payment-system" \
    -n payment-system

# Wait for operator pod to be running
oc get pods -n payment-system -l name=strimzi-cluster-operator -w

# Step 2 — Create the Kafka cluster and topics
oc apply -f openshift/kafka/kafka.yaml -n payment-system

# Watch the Kafka cluster provision (takes 3-5 minutes)
oc get kafka -n payment-system -w

# Look for READY: True
# NAME            DESIRED KAFKA REPLICAS   DESIRED ZK REPLICAS   READY
# payment-kafka   3                        3                     True

# Verify topics were created
oc exec -it payment-kafka-kafka-0 -n payment-system -- \
    bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### 5.4 Deploy Temporal Server

```bash
oc apply -f openshift/temporal/temporal.yaml -n payment-system

# Watch Temporal come up (it runs schema migrations on first start — allow 2 min)
oc get pods -l app=payment-temporal -n payment-system -w

# Verify gRPC port is listening
oc exec -it deployment/payment-temporal -n payment-system -- \
    sh -c "nc -zv localhost 7233 && echo 'Temporal gRPC OK'"
```

---

## 6. Deploy the Application

### 6.1 Update Secrets with real values

```bash
# Generate base64-encoded values
echo -n "payment_user" | base64    # cGF5bWVudF91c2Vy
echo -n "payment_pass" | base64    # cGF5bWVudF9wYXNz
echo -n "jdbc:postgresql://payment-postgres:5432/paymentdb" | base64

# Edit openshift/app/secret.yaml with the correct base64 values, then:
oc apply -f openshift/app/secret.yaml -n payment-system

# OR create directly from literals (simpler):
oc create secret generic payment-workflow-secrets \
    --from-literal=DB_USERNAME=payment_user \
    --from-literal=DB_PASSWORD=payment_pass \
    --from-literal=DB_URL="jdbc:postgresql://payment-postgres:5432/paymentdb" \
    -n payment-system
```

### 6.2 Apply ConfigMap

```bash
oc apply -f openshift/app/configmap.yaml -n payment-system
```

### 6.3 Deploy the Application

```bash
# Deploy
oc apply -f openshift/app/deployment.yaml    -n payment-system
oc apply -f openshift/app/service-route.yaml -n payment-system
oc apply -f openshift/app/hpa.yaml           -n payment-system

# Watch pods come up
oc get pods -l app=payment-workflow -n payment-system -w
# NAME                               READY   STATUS    RESTARTS   AGE
# payment-workflow-7d9f8b6c4-kxp2m   1/1     Running   0          30s
# payment-workflow-7d9f8b6c4-n8qt1   1/1     Running   0          30s
```

### 6.4 Check the startup logs for Worker confirmation

```bash
oc logs -l app=payment-workflow -n payment-system --tail=50

# Look for these lines:
# [Temporal] Connecting to Temporal Server at: payment-temporal:7233
# [Temporal] gRPC channel established → payment-temporal:7233
# [Temporal] Initialising WorkerFactory...
# [Temporal] Registered workflow: PaymentWorkflowImpl
# [Temporal] Registered 5 activities: Initiation, Validation, FraudCheck, Accounting, Completion
# [Temporal] ✓ Worker started on task queue: payment-task-queue
```

---

## 7. Verify the Deployment

### 7.1 Get the application URL

```bash
APP_URL=$(oc get route payment-workflow -n payment-system \
    -o jsonpath='{.spec.host}')
echo "App URL: https://$APP_URL"
```

### 7.2 Health check

```bash
curl -s https://$APP_URL/actuator/health | python3 -m json.tool
# Expected: {"status":"UP", ...}

curl -s https://$APP_URL/api/payments/health
# Expected: {"service":"payment-workflow-service","status":"UP"}
```

### 7.3 Submit a test payment

```bash
curl -s -X POST https://$APP_URL/api/payments \
    -H "Content-Type: application/json" \
    -d '{
      "sourceAccountId": "ACC-1001",
      "destinationAccountId": "ACC-2002",
      "amount": 500.00,
      "currency": "USD",
      "paymentType": "WIRE",
      "description": "OpenShift deployment test"
    }' | python3 -m json.tool
```

### 7.4 Verify data in PostgreSQL

```bash
oc exec -it payment-postgres-0 -n payment-system -- \
    psql -U payment_user -d paymentdb -c \
    "SELECT payment_id, status, amount, currency, transaction_reference
     FROM payment_records
     ORDER BY initiated_at DESC LIMIT 5;"

oc exec -it payment-postgres-0 -n payment-system -- \
    psql -U payment_user -d paymentdb -c \
    "SELECT payment_id, step_name, event_type, status, executed_at
     FROM workflow_event_log
     ORDER BY executed_at DESC LIMIT 10;"
```

### 7.5 Verify Kafka messages

```bash
# Check payment.events topic has messages
oc exec -it payment-kafka-kafka-0 -n payment-system -- \
    bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic payment.events \
    --from-beginning \
    --max-messages 5
```

### 7.6 Open Temporal Web UI

```bash
TEMPORAL_URL=$(oc get route temporal-ui -n payment-system \
    -o jsonpath='{.spec.host}')
echo "Temporal UI: https://$TEMPORAL_UI"
# Open in browser — you should see completed workflow executions
```

### 7.7 Check Flyway ran migrations

```bash
curl -s https://$APP_URL/actuator/flyway | python3 -m json.tool
# Should show V1__create_payment_tables migration as "SUCCESS"
```

---

## 8. CI/CD Pipeline

### Tekton Pipeline (OpenShift Pipelines Operator)

```bash
# Install OpenShift Pipelines Operator from OperatorHub first, then:

cat <<'EOF' | oc apply -f - -n payment-system
apiVersion: tekton.dev/v1beta1
kind: Pipeline
metadata:
  name: payment-workflow-pipeline
spec:
  params:
    - name: GIT_REPO
      type: string
    - name: IMAGE_TAG
      type: string
      default: "latest"
  workspaces:
    - name: source
  tasks:
    - name: clone
      taskRef:
        name: git-clone
        kind: ClusterTask
      workspaces:
        - name: output
          workspace: source
      params:
        - name: url
          value: $(params.GIT_REPO)

    - name: build-and-test
      runAfter: [clone]
      taskRef:
        name: maven
        kind: ClusterTask
      workspaces:
        - name: source
          workspace: source
      params:
        - name: GOALS
          value: ["clean", "package"]

    - name: build-image
      runAfter: [build-and-test]
      taskRef:
        name: buildah
        kind: ClusterTask
      workspaces:
        - name: source
          workspace: source
      params:
        - name: IMAGE
          value: "image-registry.openshift-image-registry.svc:5000/payment-system/payment-workflow:$(params.IMAGE_TAG)"

    - name: deploy
      runAfter: [build-image]
      taskRef:
        name: openshift-client
        kind: ClusterTask
      params:
        - name: SCRIPT
          value: |
            oc set image deployment/payment-workflow \
              payment-workflow=image-registry.openshift-image-registry.svc:5000/payment-system/payment-workflow:$(params.IMAGE_TAG) \
              -n payment-system
            oc rollout status deployment/payment-workflow -n payment-system
EOF
```

---

## 9. Operations Runbook

### Scale the worker up/down manually

```bash
# Scale to 5 replicas for high-volume batch
oc scale deployment/payment-workflow --replicas=5 -n payment-system

# Scale back to 2 replicas
oc scale deployment/payment-workflow --replicas=2 -n payment-system
```

### Rolling update (zero-downtime redeploy)

```bash
# Update image tag to new version
oc set image deployment/payment-workflow \
    payment-workflow=image-registry.../payment-workflow:1.1.0 \
    -n payment-system

# Watch rolling update
oc rollout status deployment/payment-workflow -n payment-system
```

### Roll back to previous version

```bash
oc rollout undo deployment/payment-workflow -n payment-system
oc rollout status deployment/payment-workflow -n payment-system
```

### View logs (live)

```bash
# All pods
oc logs -l app=payment-workflow -n payment-system -f

# Specific pod
oc logs payment-workflow-7d9f8b6c4-kxp2m -n payment-system -f

# Filter for errors only
oc logs -l app=payment-workflow -n payment-system | grep -E "ERROR|WARN|✗"
```

### Debug a failing pod

```bash
# Describe pod to see events and error messages
oc describe pod -l app=payment-workflow -n payment-system

# Open a shell in the container
oc exec -it deployment/payment-workflow -n payment-system -- sh

# Check the JVM heap inside the pod
oc exec deployment/payment-workflow -n payment-system -- \
    jcmd 1 VM.flags | grep -i heap
```

### Check HPA status

```bash
oc get hpa payment-workflow-hpa -n payment-system
# NAME                     REFERENCE                        TARGETS          MINPODS   MAXPODS   REPLICAS
# payment-workflow-hpa     Deployment/payment-workflow      45%/70%, 60%/80% 2         10        2
```

### Database backups

```bash
# Create a logical dump
oc exec payment-postgres-0 -n payment-system -- \
    pg_dump -U payment_user paymentdb > backup-$(date +%Y%m%d).sql

# Restore from dump
oc exec -i payment-postgres-0 -n payment-system -- \
    psql -U payment_user paymentdb < backup-20240315.sql
```

### Temporal namespace health

```bash
# Check workflow execution counts
oc exec deployment/payment-temporal -n payment-system -- \
    sh -c "tctl --address localhost:7233 namespace describe default"
```

### Kafka consumer lag monitoring

```bash
oc exec payment-kafka-kafka-0 -n payment-system -- \
    bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group payment-workflow-service \
    --describe
# Look for LAG column — should be 0 or near 0 during normal operation
```

---

## 10. Troubleshooting

### Pod stuck in `Pending`

```bash
oc describe pod <pod-name> -n payment-system
# Check Events section for:
# - "Insufficient cpu/memory" → increase cluster quota or reduce resource requests
# - "no persistent volumes available" → check PVC status: oc get pvc -n payment-system
```

### `CrashLoopBackOff` on application pod

```bash
oc logs <pod-name> -n payment-system --previous
# Common causes:
# - "Connection refused: payment-postgres:5432" → postgres pod not ready yet
# - "Connection refused: payment-temporal:7233" → temporal not ready yet
# - "Error creating bean" → check Secret values are correct base64 encoding
# - "Flyway migration failed" → check SQL syntax in V1__create_payment_tables.sql
```

### Temporal worker not polling

```bash
# Check logs for worker startup lines
oc logs -l app=payment-workflow -n payment-system | grep -i temporal

# If you see "UNAVAILABLE: Connection refused" the service name is wrong.
# Verify:
oc get svc payment-temporal -n payment-system
# The service name in configmap APP_TEMPORAL_TARGET must match exactly.
```

### Kafka connection refused

```bash
# Verify Kafka bootstrap service exists
oc get svc -n payment-system | grep kafka-bootstrap

# Verify pods in ConfigMap bootstrap address matches the service name
oc get configmap payment-workflow-config -n payment-system -o yaml \
    | grep KAFKA_BOOTSTRAP
```

### Database schema out of sync

```bash
# Check Flyway migration history in the DB
oc exec payment-postgres-0 -n payment-system -- \
    psql -U payment_user -d paymentdb -c \
    "SELECT version, description, success, installed_on
     FROM flyway_schema_history ORDER BY installed_rank;"
```

---

## Quick Reference — All oc Commands

```bash
# Full deployment (run once in order)
oc apply -f openshift/namespace-rbac.yaml
oc apply -f openshift/postgres/postgres.yaml         -n payment-system
oc apply -f openshift/kafka/kafka.yaml               -n payment-system
oc apply -f openshift/temporal/temporal.yaml         -n payment-system
oc create secret generic payment-workflow-secrets \
    --from-literal=DB_USERNAME=payment_user \
    --from-literal=DB_PASSWORD=payment_pass \
    --from-literal=DB_URL="jdbc:postgresql://payment-postgres:5432/paymentdb" \
    -n payment-system
oc apply -f openshift/app/configmap.yaml             -n payment-system
oc apply -f openshift/app/deployment.yaml            -n payment-system
oc apply -f openshift/app/service-route.yaml         -n payment-system
oc apply -f openshift/app/hpa.yaml                   -n payment-system

# Get app URL
oc get route payment-workflow -n payment-system -o jsonpath='{.spec.host}'

# Check all pods
oc get pods -n payment-system

# Tail logs
oc logs -l app=payment-workflow -n payment-system -f

# Delete everything (careful!)
oc delete namespace payment-system
```
