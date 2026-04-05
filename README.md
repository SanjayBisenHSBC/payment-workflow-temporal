# Payment Workflow Service
### Temporal + Spring Boot — Complete Developer Guide

---

## Table of Contents
1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Project Structure](#3-project-structure)
4. [Temporal Concepts Explained](#4-temporal-concepts-explained)
5. [Workflow Exit Points (The 5 Steps)](#5-workflow-exit-points)
6. [Running Locally on Mac](#6-running-locally-on-mac)
7. [API Reference](#7-api-reference)
8. [Testing](#8-testing)
9. [Configuration Reference](#9-configuration-reference)
10. [Extending the Workflow](#10-extending-the-workflow)
11. [Troubleshooting](#11-troubleshooting)

---

## 1. Overview

This service implements a **durable, fault-tolerant payment processing pipeline** using:

| Technology | Purpose |
|---|---|
| **Spring Boot 3.2** | REST API, dependency injection, service layer |
| **Temporal SDK 1.22** | Workflow orchestration, durability, retries |
| **Java 17** | Language runtime |
| **Swagger/OpenAPI** | Interactive API documentation |

### What Problem Does Temporal Solve?

Without Temporal, a multi-step payment flow like this would require:
- Manual retry logic for each step
- Distributed transaction management
- Complex state machines stored in a database
- Cron jobs to resume failed payments
- Custom idempotency keys everywhere

**With Temporal**, you write plain Java code. Temporal handles:
- ✅ Automatic retries with configurable backoff
- ✅ Durable state — survives server crashes/restarts
- ✅ Full execution history for debugging
- ✅ Long-running workflows (minutes, hours, or years)
- ✅ Built-in idempotency

---

## 2. Architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│                        CLIENT (curl / Postman)                         │
└───────────────────────────────┬───────────────────────────────────────┘
                                │ HTTP POST /api/payments
                                ▼
┌───────────────────────────────────────────────────────────────────────┐
│                    SPRING BOOT APPLICATION                             │
│                                                                        │
│  ┌─────────────────────┐      ┌──────────────────────────────────┐   │
│  │  PaymentController  │      │        Temporal Worker            │   │
│  │  (REST endpoint)    │      │                                   │   │
│  │                     │      │  ┌────────────────────────────┐  │   │
│  │  workflowClient     │      │  │  PaymentWorkflowImpl       │  │   │
│  │  .newWorkflowStub() │      │  │  (Orchestrator)            │  │   │
│  │                     │      │  │                            │  │   │
│  └──────────┬──────────┘      │  │  calls Activities:        │  │   │
│             │                  │  │  1. InitiationActivity    │  │   │
│             │                  │  │  2. ValidationActivity    │  │   │
│             │                  │  │  3. FraudCheckActivity    │  │   │
│             │                  │  │  4. AccountingActivity    │  │   │
│             │                  │  │  5. CompletionActivity    │  │   │
│             │                  │  └────────────────────────────┘  │   │
│             │                  └──────────────────────────────────┘   │
└─────────────┼──────────────────────────────────────────────────────────┘
              │                          ▲
              │  StartWorkflow           │ Long-poll for tasks
              ▼                          │
┌───────────────────────────────────────────────────────────────────────┐
│                      TEMPORAL SERVER                                   │
│                                                                        │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │  Namespace: default                                              │  │
│  │  Task Queue: payment-task-queue                                  │  │
│  │                                                                   │  │
│  │  Workflow Execution History (durable event log):                 │  │
│  │  [WorkflowStarted] → [ActivityScheduled] → [ActivityCompleted]  │  │
│  │  → [ActivityScheduled] → ... → [WorkflowCompleted]              │  │
│  └─────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────┘
              │
              │ Web UI
              ▼
        http://localhost:8233
```

### Data Flow per Payment

```
PaymentRequest (JSON)
      │
      ▼
[1] PaymentInitiationActivity
      │  → Assigns paymentId, bookingRef, timestamp
      │  → PaymentContext.status = INITIATED
      ▼
[2] PaymentValidationActivity
      │  → Validates amount, currency, accounts, payment type
      │  → Derives: route, FX rate, value date, charge bearer
      │  → PaymentContext.status = VALIDATED
      ▼
[3] FraudCheckActivity
      │  → Checks sanctions lists, velocity, patterns
      │  → Assigns risk score: LOW / MEDIUM / HIGH
      │  → HIGH → PaymentContext.status = FRAUD_BLOCKED (exits)
      │  → MEDIUM/LOW → PaymentContext.status = FRAUD_CHECKED
      ▼
[4] AccountingActivity
      │  → Posts DEBIT entry on source account
      │  → Posts CREDIT entry on destination account
      │  → PaymentContext.status = ACCOUNTING_POSTED
      ▼
[5] PaymentCompletionActivity
      │  → Generates transaction reference (TXN-...)
      │  → Sends sender notification
      │  → Sends receiver notification
      │  → PaymentContext.status = COMPLETED
      ▼
PaymentResult (JSON) — returned to caller
```

---

## 3. Project Structure

```
payment-workflow/
├── pom.xml                                      # Maven dependencies
├── README.md                                    # This file
│
└── src/
    ├── main/
    │   ├── java/com/payment/workflow/
    │   │   │
    │   │   ├── PaymentWorkflowApplication.java  # Spring Boot entry point
    │   │   │
    │   │   ├── model/                           # Domain objects (POJOs)
    │   │   │   ├── PaymentRequest.java          # Input: what the client sends
    │   │   │   ├── PaymentResult.java           # Output: final response
    │   │   │   ├── PaymentContext.java          # State bag flowing through workflow
    │   │   │   ├── PaymentStatus.java           # Status enum (INITIATED → COMPLETED)
    │   │   │   └── WorkflowStep.java            # Single audit trail entry
    │   │   │
    │   │   ├── workflow/                        # Temporal workflow definitions
    │   │   │   ├── PaymentWorkflow.java         # @WorkflowInterface (contract)
    │   │   │   └── PaymentWorkflowImpl.java     # Orchestration logic
    │   │   │
    │   │   ├── activities/                      # Temporal activity definitions
    │   │   │   ├── PaymentInitiationActivity.java      # Interface (Exit Point 1)
    │   │   │   ├── PaymentInitiationActivityImpl.java  # Implementation
    │   │   │   ├── PaymentValidationActivity.java      # Interface (Exit Point 2)
    │   │   │   ├── PaymentValidationActivityImpl.java  # Implementation
    │   │   │   ├── FraudCheckActivity.java             # Interface (Exit Point 3)
    │   │   │   ├── FraudCheckActivityImpl.java         # Implementation
    │   │   │   ├── AccountingActivity.java             # Interface (Exit Point 4)
    │   │   │   ├── AccountingActivityImpl.java         # Implementation
    │   │   │   ├── PaymentCompletionActivity.java      # Interface (Exit Point 5)
    │   │   │   └── PaymentCompletionActivityImpl.java  # Implementation
    │   │   │
    │   │   ├── controller/
    │   │   │   └── PaymentController.java       # REST endpoints
    │   │   │
    │   │   └── config/
    │   │       ├── TemporalConfig.java          # Temporal beans (client, stubs)
    │   │       └── OpenApiConfig.java           # Swagger/OpenAPI setup
    │   │
    │   └── resources/
    │       └── application.yml                  # App + Temporal configuration
    │
    └── test/
        └── java/com/payment/workflow/
            └── PaymentWorkflowTest.java         # Unit + integration tests
```

---

## 4. Temporal Concepts Explained

### 4.1 Workflow

A **Workflow** is a durable function. Temporal persists every step as an event in an append-only history log.

```java
@WorkflowInterface
public interface PaymentWorkflow {
    @WorkflowMethod
    PaymentResult processPayment(PaymentRequest request);

    @QueryMethod
    PaymentContext getCurrentStatus();
}
```

**Rules for Workflow code:**

| ❌ DO NOT use | ✅ USE INSTEAD |
|---|---|
| `System.currentTimeMillis()` | `Workflow.currentTimeMillis()` |
| `UUID.randomUUID()` | `Workflow.randomUUID()` |
| `Thread.sleep(5000)` | `Workflow.sleep(Duration.ofSeconds(5))` |
| `new Random()` | `Workflow.newRandom()` |
| Direct HTTP/DB calls | Activity methods |

Why? Temporal **replays** the workflow from history after a crash. If your code is non-deterministic, the replay produces different results — causing errors.

### 4.2 Activity

An **Activity** is a single step that CAN do I/O. Activities are the only place to call external services, databases, or APIs.

```java
@ActivityInterface
public interface FraudCheckActivity {
    @ActivityMethod
    PaymentContext performFraudCheck(PaymentContext context);
}
```

Each activity has retry options configured in the workflow:

```java
ActivityOptions options = ActivityOptions.newBuilder()
    .setScheduleToCloseTimeout(Duration.ofSeconds(120))
    .setRetryOptions(RetryOptions.newBuilder()
        .setMaximumAttempts(3)
        .setInitialInterval(Duration.ofSeconds(2))
        .setBackoffCoefficient(2.0)   // Waits 2s, 4s, 8s between retries
        .build())
    .build();
```

### 4.3 Worker

A **Worker** is a process that:
1. Polls Temporal Server's task queue for work
2. Executes workflows and activities when tasks arrive
3. Reports results back to Temporal Server

In this app, the Worker is started automatically by `temporal-spring-boot-starter-alpha` based on `application.yml`.

### 4.4 Task Queue

A **Task Queue** is a named buffer in Temporal Server. Workers long-poll their assigned task queue. Multiple workers can poll the same queue for horizontal scaling.

This app uses: `payment-task-queue`

### 4.5 Workflow ID vs Run ID

| Concept | Description |
|---|---|
| **Workflow ID** | Business-meaningful ID (we use the Payment ID). Ensures idempotency — starting a workflow with an existing ID fails gracefully |
| **Run ID** | Internal UUID for a specific execution attempt |

### 4.6 Query Method

Queries let you inspect live workflow state without modifying it:

```java
// From controller — queries the in-memory state of the running workflow
PaymentWorkflow stub = workflowClient.newWorkflowStub(PaymentWorkflow.class, paymentId);
PaymentContext context = stub.getCurrentStatus();
```

---

## 5. Workflow Exit Points

The workflow has 5 named exit points — each maps to an Activity:

### Exit Point 1 — Payment Initiation (`PaymentInitiationActivity`)

**When called:** First, always  
**Purpose:** Book the payment — assign identifiers and timestamps  
**Outputs written to context:**
- `assignedPaymentId` — unique ID (PAY-XXXX)
- `initiationTime` — exact timestamp
- `initiationReference` — booking reference (BKG-...)
- `currentStatus` → `INITIATED`

**Failure behaviour:** Sets status to `FAILED`, workflow exits immediately

---

### Exit Point 2 — Validation & Derivations (`PaymentValidationActivity`)

**When called:** After successful initiation  
**Purpose:** Validate business rules and enrich the payment with derived data  

**Validation rules:**
- Amount must be between 0.01 and 10,000,000
- Currency must be in: USD, EUR, GBP, SGD, JPY, CHF, AUD, CAD, HKD, CNY
- Source and destination accounts must not be the same
- Payment type must be: WIRE, ACH, SEPA, SWIFT, INTERNAL

**Derivations:**

| Derived Field | Logic |
|---|---|
| `paymentRoute` | INTERNAL / DOMESTIC / REGIONAL / CROSS_BORDER (based on type) |
| `correspondentBank` | Derived from target currency for cross-border |
| `fxRate` | Simulated rate table (SGD base) |
| `valueDate` | T+0 (INTERNAL), T+1 (ACH/SEPA), T+2 (WIRE/SWIFT) |
| `chargeBearer` | OUR (WIRE/INTERNAL), SHA (ACH/SEPA) |

**Failure behaviour:** Sets `validationPassed = false`, workflow exits with `FAILED`

---

### Exit Point 3 — Fraud Check (`FraudCheckActivity`)

**When called:** After successful validation  
**Purpose:** Screen for fraud, AML violations, sanctions  

**Fraud rules (additive scoring):**

| Rule | Score Added |
|---|---|
| Account on blocked/sanctions list | +100 (auto-block) |
| Amount > $50,000 | +40 |
| Amount > $10,000 | +20 |
| Cross-border payment | +10 |
| Amount in structuring range ($9,990–$9,999) | +30 |
| High velocity pattern detected | +35 |

**Risk levels:**

| Score | Level | Outcome |
|---|---|---|
| 0–29 | LOW | Pass, proceed normally |
| 30–69 | MEDIUM | Pass, flag for manual review |
| 70+ | HIGH | **BLOCK** — workflow exits with `FRAUD_BLOCKED` |

**Testing fraud blocking:** Use `sourceAccountId: "ACC-BLOCKED-001"`

---

### Exit Point 4 — Accounting (`AccountingActivity`)

**When called:** After fraud clearance  
**Purpose:** Post double-entry GL entries  

**Entries posted:**
- **DEBIT** source account (reduces sender balance)
- **CREDIT** destination account (increases receiver balance)

Generates an accounting reference: `GL-{timestamp}-{id}`

**Idempotency:** Real implementations should check for existing entries with the same paymentId before posting.

**Failure behaviour:** Sets `accountingPosted = false`, workflow exits with `FAILED`

---

### Exit Point 5 — Completion & Notifications (`PaymentCompletionActivity`)

**When called:** After accounting is posted  
**Purpose:** Finalize and notify  

**Actions:**
1. Generates transaction reference: `TXN-{date}-{paymentId-suffix}`
2. Sends notification to sender (simulated)
3. Sends notification to receiver (simulated)
4. Sets status to `COMPLETED`

**Important:** Notification failures do **NOT** fail the payment. Accounting has already posted — funds have moved. Notification failures are captured in `notificationSentToSender` / `notificationSentToReceiver` flags.

---

## 6. Running Locally on Mac

### Prerequisites

| Tool | Install Command | Version |
|---|---|---|
| Java 17+ | `brew install openjdk@17` | 17+ |
| Maven 3.8+ | `brew install maven` | 3.8+ |
| Temporal CLI | `brew install temporal` | Latest |
| Docker (optional) | [docker.com](https://docker.com) | Any |

#### Verify installs:
```bash
java -version         # Should show 17+
mvn -version          # Should show 3.8+
temporal --version    # Should show temporal version
```

---

### Step 1 — Start Temporal Server (Local Dev Mode)

Temporal CLI includes a lightweight dev server that runs everything in a single process:

```bash
temporal server start-dev
```

This starts:
- Temporal Server on port **7233** (gRPC)
- Temporal Web UI on port **8233** (http://localhost:8233)

> **Keep this terminal open.** The server must be running before starting the Spring Boot app.

---

### Step 2 — Clone and Build the Application

```bash
# Navigate to the project directory
cd payment-workflow

# Build without running tests (faster first build)
mvn clean package -DskipTests

# OR build and run tests (requires Temporal server to not conflict — tests use in-memory Temporal)
mvn clean package
```

---

### Step 3 — Run the Spring Boot Application

```bash
mvn spring-boot:run
```

Expected output:
```
  .   ____          _            __ _ _
 /\\ / ___'_ __ _ _(_)_ __  __ _ \ \ \ \
Started PaymentWorkflowApplication in 3.2 seconds (JVM running for 4.1)
[Temporal] Worker started on task queue: payment-task-queue
```

The app is now running on **http://localhost:8080**

---

### Step 4 — Test the API

#### Option A: Swagger UI (Recommended for exploration)
Open http://localhost:8080/swagger-ui.html in your browser.

#### Option B: curl

**Submit a payment (synchronous — waits for completion):**
```bash
curl -X POST http://localhost:8080/api/payments \
  -H "Content-Type: application/json" \
  -d '{
    "sourceAccountId": "ACC-1001",
    "destinationAccountId": "ACC-2002",
    "amount": 1500.00,
    "currency": "USD",
    "paymentType": "WIRE",
    "reference": "INV-2024-001",
    "description": "Invoice payment"
  }'
```

**Submit a payment (asynchronous — returns immediately):**
```bash
curl -X POST http://localhost:8080/api/payments/async \
  -H "Content-Type: application/json" \
  -d '{
    "sourceAccountId": "ACC-3001",
    "destinationAccountId": "ACC-3002",
    "amount": 500.00,
    "currency": "SGD",
    "paymentType": "INTERNAL"
  }'
```

**Check live status (while workflow is running):**
```bash
curl http://localhost:8080/api/payments/{paymentId}/status
```

**Test fraud blocking:**
```bash
curl -X POST http://localhost:8080/api/payments \
  -H "Content-Type: application/json" \
  -d '{
    "sourceAccountId": "ACC-BLOCKED-001",
    "destinationAccountId": "ACC-2002",
    "amount": 100.00,
    "currency": "USD",
    "paymentType": "WIRE"
  }'
```

**Test validation failure (unsupported currency):**
```bash
curl -X POST http://localhost:8080/api/payments \
  -H "Content-Type: application/json" \
  -d '{
    "sourceAccountId": "ACC-1001",
    "destinationAccountId": "ACC-2002",
    "amount": 100.00,
    "currency": "XYZ",
    "paymentType": "WIRE"
  }'
```

---

### Step 5 — View Workflow Execution in Temporal UI

1. Open http://localhost:8233
2. Click on the **default** namespace
3. You'll see all workflow executions listed
4. Click any execution to see:
   - Full event history (every step recorded)
   - Activity inputs and outputs
   - Timing for each step
   - Retry attempts (if any)

This is extremely useful for debugging — you can see exactly where and why a workflow failed.

---

### All Running Services Summary

| Service | URL | Purpose |
|---|---|---|
| Spring Boot App | http://localhost:8080 | Payment REST API |
| Swagger UI | http://localhost:8080/swagger-ui.html | Interactive API docs |
| API Docs (JSON) | http://localhost:8080/api-docs | OpenAPI spec |
| Temporal Web UI | http://localhost:8233 | Workflow monitoring |
| Temporal gRPC | localhost:7233 | Server (internal) |
| Actuator Health | http://localhost:8080/actuator/health | App health |

---

## 7. API Reference

### POST /api/payments
Submits a payment and waits for completion (synchronous).

**Request Body:**
```json
{
  "paymentId": "PAY-OPTIONAL-ID",        // Optional, auto-generated if omitted
  "sourceAccountId": "ACC-1001",          // Required
  "destinationAccountId": "ACC-2002",     // Required
  "amount": 1500.00,                      // Required, > 0
  "currency": "USD",                      // Required: USD|EUR|GBP|SGD|JPY|CHF|AUD|CAD|HKD|CNY
  "paymentType": "WIRE",                  // Required: WIRE|ACH|SEPA|SWIFT|INTERNAL
  "reference": "INV-2024-001",            // Optional
  "description": "Invoice payment"        // Optional
}
```

**Success Response (200):**
```json
{
  "paymentId": "PAY-ABC123DEF456",
  "status": "COMPLETED",
  "message": "Payment completed successfully. Transaction Ref: TXN-20240315-ABC123...",
  "amount": 1500.00,
  "currency": "USD",
  "sourceAccountId": "ACC-1001",
  "destinationAccountId": "ACC-2002",
  "transactionReference": "TXN-20240315-ABC123DEF456",
  "completedAt": "2024-03-15T10:30:45.123",
  "auditTrail": [
    {
      "stepName": "INITIATION",
      "description": "Payment initiated. ID: PAY-ABC123, Amount: 1500.00 USD...",
      "success": true,
      "executedAt": "2024-03-15T10:30:44.100",
      "outputData": "{\"paymentId\":\"PAY-ABC123\",\"bookingRef\":\"BKG-...\"}"
    },
    { "stepName": "VALIDATION", ... },
    { "stepName": "FRAUD_CHECK", ... },
    { "stepName": "ACCOUNTING", ... },
    { "stepName": "COMPLETION", ... }
  ]
}
```

**Fraud Blocked Response (402):**
```json
{
  "paymentId": "PAY-XYZ",
  "status": "FRAUD_BLOCKED",
  "message": "Payment blocked by fraud screening. Risk Score: 100/100...",
  ...
}
```

---

### POST /api/payments/async
Starts workflow and returns immediately with the payment ID.

**Response (202):**
```json
{
  "paymentId": "PAY-ABC123",
  "status": "PROCESSING",
  "message": "Payment workflow started. Poll /api/payments/PAY-ABC123/status for updates."
}
```

---

### GET /api/payments/{paymentId}/status
Queries the live state of a running workflow.

**Response (200):**
```json
{
  "originalRequest": { ... },
  "currentStatus": "FRAUD_CHECKED",
  "assignedPaymentId": "PAY-ABC123",
  "validationPassed": true,
  "paymentRoute": "CROSS_BORDER",
  "fxRate": 0.74,
  "fraudCheckPassed": true,
  "fraudRiskScore": "LOW",
  "auditTrail": [ ... ]
}
```

---

## 8. Testing

### Run All Tests
```bash
mvn test
```

Tests use Temporal's `TestWorkflowEnvironment` — no real Temporal server needed.

### Test Scenarios Covered

| Test | Description |
|---|---|
| `testSuccessfulPaymentWorkflow` | Full happy path — verifies COMPLETED status and 5 audit steps |
| `testPaymentFailsOnInvalidCurrency` | Verifies validation rejects unsupported currencies |
| `testPaymentBlockedByFraud` | Verifies blocked accounts trigger FRAUD_BLOCKED |
| `testInternalPaymentWorkflow` | Verifies SGD internal transfer completes |
| `testPaymentFailsOnZeroAmount` | Verifies zero amount fails validation |
| `testAuditTrailContainsAllSteps` | Verifies all 5 steps appear in order in audit trail |

### How Temporal Testing Works

```java
@RegisterExtension
public static final TestWorkflowExtension testWorkflow =
    TestWorkflowExtension.newBuilder()
        .setWorkflowTypes(PaymentWorkflowImpl.class)          // Register workflow
        .setActivityImplementations(                           // Register real activities
            new PaymentInitiationActivityImpl(),
            new PaymentValidationActivityImpl(),
            ...
        )
        .build();
```

Temporal's `TestWorkflowEnvironment` spins up an **in-memory Temporal server** for each test. Time-skipping is built in — `Workflow.sleep(Duration.ofDays(1))` completes instantly in tests.

---

## 9. Configuration Reference

`application.yml`:

```yaml
temporal:
  connection:
    target: 127.0.0.1:7233      # Temporal Server address
  namespace: default             # Temporal namespace
  workers:
    - task-queue: payment-task-queue  # Task queue name (must match WorkflowOptions)
      workflow-classes:
        - com.payment.workflow.workflow.PaymentWorkflowImpl
      activity-beans:            # Spring bean names of activity implementations
        - paymentInitiationActivity
        - paymentValidationActivity
        - fraudCheckActivity
        - accountingActivity
        - paymentCompletionActivity
```

---

## 10. Extending the Workflow

### Adding a New Activity (e.g., Compliance Check)

**Step 1:** Create the interface:
```java
@ActivityInterface
public interface ComplianceCheckActivity {
    @ActivityMethod
    PaymentContext performComplianceCheck(PaymentContext context);
}
```

**Step 2:** Implement it:
```java
@Component("complianceCheckActivity")
public class ComplianceCheckActivityImpl implements ComplianceCheckActivity {
    @Override
    public PaymentContext performComplianceCheck(PaymentContext context) {
        // Your logic here
        return context;
    }
}
```

**Step 3:** Register in `application.yml`:
```yaml
activity-beans:
  - complianceCheckActivity   # Add here
```

**Step 4:** Add to the workflow:
```java
// In PaymentWorkflowImpl — add stub
private final ComplianceCheckActivity complianceActivity =
    Workflow.newActivityStub(ComplianceCheckActivity.class, buildActivityOptions(...));

// In processPayment() — call it between fraud check and accounting
context = complianceActivity.performComplianceCheck(context);
```

**Step 5:** Add fields to `PaymentContext.java` for any new data the activity produces.

---

## 11. Troubleshooting

### `Connection refused: localhost:7233`
Temporal server is not running. Start it:
```bash
temporal server start-dev
```

### `Worker is not registered for workflow type`
The workflow class name in `application.yml` doesn't match the actual class. Verify:
```yaml
workflow-classes:
  - com.payment.workflow.workflow.PaymentWorkflowImpl  # Full qualified name
```

### `Non-deterministic workflow error`
You introduced non-deterministic code into the workflow. Common causes:
- Used `System.currentTimeMillis()` (use `Workflow.currentTimeMillis()`)
- Added a new activity call without a workflow version check
- Used a non-deterministic collection (use `ArrayList`, not `HashSet`)

### `Activity task timed out`
Increase `scheduleToCloseTimeout` in the ActivityOptions within `PaymentWorkflowImpl`.

### Workflow not appearing in Temporal UI
Check that the task queue in `WorkflowOptions` matches the worker's task queue:
```java
// Controller:
WorkflowOptions.newBuilder().setTaskQueue("payment-task-queue")

// application.yml:
task-queue: payment-task-queue  # Must match exactly
```

---

## Quick Reference

```bash
# Start Temporal dev server
temporal server start-dev

# Start Spring Boot app
mvn spring-boot:run

# Run tests
mvn test

# Build JAR
mvn clean package -DskipTests

# Run JAR directly
java -jar target/payment-workflow-1.0.0.jar

# View Temporal Web UI
open http://localhost:8233

# View Swagger UI
open http://localhost:8080/swagger-ui.html
```

---

## New in v2 — Database, Kafka & OpenShift

### Database (PostgreSQL + Flyway + JPA)

Three tables are created automatically on startup via Flyway:

| Table | Purpose |
|---|---|
| `payment_records` | One row per payment — updated at each exit point |
| `workflow_event_log` | Append-only audit log — one row per workflow step |
| `fraud_check_requests` | Tracks Kafka fraud request/response round-trips |

**Local dev quickstart:**
```bash
docker-compose up -d postgres
mvn spring-boot:run
```

**Query audit log after a payment:**
```sql
SELECT step_name, event_type, status, description, executed_at
FROM workflow_event_log
WHERE payment_id = 'PAY-ABC123'
ORDER BY executed_at ASC;
```

### Kafka Integration

Three topics:

| Topic | Direction | Purpose |
|---|---|---|
| `payment.fraud.request` | Outbound | Sends payment to external fraud engine |
| `payment.fraud.response` | Inbound | Receives fraud decision from fraud engine |
| `payment.events` | Outbound | Broadcasts lifecycle events to all consumers |

**Async-to-sync bridge pattern** in `FraudCheckActivityImpl`:
The Temporal activity blocks on a `CompletableFuture` until the fraud system responds on Kafka, or times out after 30 seconds and falls back to internal rule-based scoring.

**Local dev quickstart:**
```bash
docker-compose up -d kafka zookeeper
# Topics are auto-created on first use
```

**Test the fraud topic manually:**
```bash
# Publish a test fraud response (simulates external fraud system)
docker exec payment-kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic payment.fraud.response \
  --property "key.serializer=org.apache.kafka.common.serialization.StringSerializer"
```

### OpenShift Deployment

See `OPENSHIFT_DEPLOYMENT.md` for the full step-by-step guide.

**Quick deploy order:**
```bash
oc apply -f openshift/namespace-rbac.yaml
oc apply -f openshift/postgres/postgres.yaml   -n payment-system
oc apply -f openshift/kafka/kafka.yaml         -n payment-system
oc apply -f openshift/temporal/temporal.yaml   -n payment-system
oc apply -f openshift/app/configmap.yaml       -n payment-system
# Create secret (see OPENSHIFT_DEPLOYMENT.md Step 6.1)
oc apply -f openshift/app/deployment.yaml      -n payment-system
oc apply -f openshift/app/service-route.yaml   -n payment-system
oc apply -f openshift/app/hpa.yaml             -n payment-system
```

# payment-workflow-temporal
