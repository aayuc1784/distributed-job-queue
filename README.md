<h3 align="center">Distributed Job Queue</h3>

<p align="center">
  A fault-tolerant and priority-aware distributed job scheduling and execution platform supporting DAG-based dependencies, automatic retries, rate limiting, worker scaling, failure recovery, and durable job processing.
  <br />
</p>

<p align="center">
  <b>Java • Spring Boot • RabbitMQ • PostgreSQL • Redis • NGINX</b>
</p>

<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#architecture">Architecture</a></li>
        <li><a href="#built-with">Built With</a></li>
        <li><a href="#features">Features</a></li>
      </ul>
    </li>
    <li>
      <a href="#job-lifecycle">Job Lifecycle</a>
    </li>
    <li>
      <a href="#priority--scheduling">Priority & Scheduling</a>
    </li>
    <li>
      <a href="#dag-based-dependencies">DAG-based Dependencies</a>
    </li>
    <li>
      <a href="#retry--failure-handling">Retry & Failure Handling</a>
    </li>
    <li>
      <a href="#reliability--recovery">Reliability & Recovery</a>
    </li>
    <li>
      <a href="#api-endpoints">API Endpoints</a>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation--usage">Installation & Usage</a></li>
      </ul>
    </li>
    <li>
      <a href="#testing">Testing</a>
    </li>
    <li>
      <a href="#future-improvements">Future Improvements</a>
    </li>
  </ol>
</details>


<!-- ABOUT THE PROJECT -->

## About The Project

Distributed Job Queue is a priority-aware job scheduling and execution platform designed to process asynchronous jobs reliably across multiple workers.

The system accepts jobs from multiple producers, distributes incoming traffic through NGINX, validates and rate-limits requests through an API Gateway, and persists job metadata in PostgreSQL before scheduling jobs for execution through RabbitMQ.

RabbitMQ acts as the execution transport and manages separate queues for HIGH, MEDIUM, and LOW priority jobs. Jobs can also depend on other jobs through a DAG-based dependency model, allowing complex workflows to be represented and executed only when all required dependencies are completed.

The system focuses on common distributed systems challenges including:

- Priority-aware scheduling
- Job dependencies
- At-least-once delivery
- Retry handling
- Idempotency
- Worker failures
- RabbitMQ failures
- Database and message broker consistency
- Rate limiting
- Worker scaling
- Graceful shutdown
- Dead job recovery

PostgreSQL acts as the source of truth for job metadata and lifecycle state, while RabbitMQ is responsible for transporting executable jobs to workers.

A Transactional Outbox pattern is used to prevent inconsistencies between the database and RabbitMQ.

![Architecture](https://github.com/aayuc1784/distributed-job-queue/blob/main/images/architecture.png)


<!-- ARCHITECTURE -->

## Architecture

The high-level request flow is:

```text
Multiple Producers
        │
        ▼
      NGINX
        │
        ▼
   API Gateway
        │
        ▼
   Job Service
        │
        ├──────────────► PostgreSQL
        │                 Jobs
        │                 Dependencies
        │                 Job Events
        │                 Outbox Events
        │
        ▼
   Outbox Publisher
        │
        ▼
      RabbitMQ
        │
        ├── HIGH_QUEUE
        ├── MEDIUM_QUEUE
        ├── LOW_QUEUE
        │
        ├── RETRY QUEUES
        │
        └── DEAD LETTER QUEUE
                │
                ▼
             Workers
                │
                ▼
           Job Execution
                │
                ▼
           PostgreSQL
```

### Architecture Components

#### Producers

Multiple services can submit jobs to the queue.

Example producers include:

- Email Service
- Payment Service
- Authentication Service
- Database Service
- Other internal services
- External clients

All producers communicate with the system through NGINX.

---

#### NGINX

NGINX acts as the entry point for multiple producers.

Responsibilities:

- Load balancing incoming requests
- Health checks
- Routing traffic to available API instances
- Supporting multiple Job Service instances
- Basic request handling

This allows the Job API to scale horizontally.

```text
Producer 1 ──┐
Producer 2 ──┤
Producer 3 ──┼──► NGINX ──► Job Service Instances
Producer N ──┘
```

---

#### API Gateway

The API Gateway handles request-level concerns before jobs reach the Job Service.

Responsibilities include:

- Rate limiting
- Request validation
- Client/API key identification
- Authentication integration
- Request routing
- Backpressure handling

Rate limiting can be configured per:

- Client
- API key
- Job type
- Request rate

Redis is used for distributed rate limiting.

The initial implementation uses a Token Bucket based rate limiting strategy.

---

#### Job Service

The Job Service is the core orchestration component.

Responsibilities include:

- Job creation
- Job validation
- Priority validation
- Dependency validation
- Cycle detection
- Job lifecycle transitions
- Cancellation handling
- Dead job revival
- Persisting jobs
- Creating transactional outbox events

The Job Service does not directly depend on RabbitMQ for job persistence.

Instead, job metadata is first stored transactionally in PostgreSQL.

---

#### PostgreSQL

PostgreSQL acts as the source of truth.

It stores:

```text
Jobs
Dependencies
Job Events
Outbox Events
Worker Metadata
```

Example tables:

```text
jobs

job_dependencies

job_events

outbox_events

workers
```

RabbitMQ is responsible for transporting executable jobs, while PostgreSQL stores the authoritative state of each job.

---

#### Transactional Outbox

The Transactional Outbox pattern ensures consistency between PostgreSQL and RabbitMQ.

When a job is created:

```text
Database Transaction

├── Save Job
│
├── Save Dependencies
│
└── Create Outbox Event
```

After the transaction completes:

```text
Outbox Publisher
        │
        ▼
   Reads Outbox Event
        │
        ▼
   Publishes to RabbitMQ
        │
        ▼
 Marks Event Processed
```

This prevents scenarios such as:

```text
Job saved in database
        │
        ▼
RabbitMQ publish fails
        │
        ▼
Job never executes
```

If RabbitMQ is temporarily unavailable, the Outbox Publisher retries publishing the event.

---

#### RabbitMQ

RabbitMQ acts as the execution transport.

Separate queues are used for different priority levels.

```text
HIGH_QUEUE

MEDIUM_QUEUE

LOW_QUEUE
```

Additional queues handle retries and failures.

```text
DELAY_QUEUE

RETRY_QUEUE

DEAD_LETTER_QUEUE
```

RabbitMQ is responsible for:

- Job transport
- Worker dispatch
- Message acknowledgement
- Redelivery
- Retry routing
- Dead letter handling

Kafka is intentionally not used because this project focuses on job execution rather than event streaming.

Jobs are published directly to RabbitMQ.

---

#### Workers

Multiple workers consume jobs from RabbitMQ.

Workers use the competing consumer model.

```text
RabbitMQ
    │
    ├──── Worker 1
    │
    ├──── Worker 2
    │
    ├──── Worker 3
    │
    └──── Worker N
```

Workers support:

- Concurrent processing
- Configurable concurrency
- Job execution
- Job acknowledgement
- Retry handling
- Worker heartbeats
- Graceful shutdown
- Crash recovery

Worker scaling can be triggered based on:

- Queue depth
- Worker utilization
- Processing latency
- Oldest message age

---

#### Redis

Redis is used for lightweight distributed operations.

Responsibilities include:

- Rate limiting
- Distributed locks
- Worker heartbeats
- Optional caching

Redis is not the source of truth for jobs.

Job state remains persisted in PostgreSQL.

---

#### Monitoring & Observability

The system exposes metrics and job events for monitoring.

Metrics include:

- Queue depth
- Jobs created
- Jobs completed
- Jobs failed
- Jobs retried
- Dead jobs
- Worker utilization
- Processing latency
- Queue wait time

The observability stack includes:

```text
Spring Boot Actuator

Micrometer

Prometheus

Grafana

Structured JSON Logs
```


<!-- FEATURES -->

## Features

### Job Management

- Create jobs
- Get job by ID
- List jobs
- Filter jobs
- Pagination support
- Job type validation
- Job payload validation
- Job cancellation
- Dead job revival
- Bulk dead job revival

---

### Priority-based Processing

Jobs support three priority levels.

```text
HIGH

MEDIUM

LOW
```

Each priority has its own queue.

```text
HIGH_QUEUE

MEDIUM_QUEUE

LOW_QUEUE
```

The scheduler supports priority-aware processing.

Scheduling strategies can include:

```text
STRICT_PRIORITY

WEIGHTED_ROUND_ROBIN

AGING_BASED_PRIORITY
```

---

### Priority Starvation Prevention

Strict priority scheduling can cause starvation.

Example:

```text
HIGH HIGH HIGH HIGH HIGH

MEDIUM ❌

LOW ❌
```

To prevent lower-priority jobs from waiting indefinitely, the system supports fair scheduling strategies.

Example:

```text
5 HIGH jobs

3 MEDIUM jobs

1 LOW job
```

Priority aging can also increase the effective priority of jobs that have waited beyond a configured threshold.

---

### DAG-based Dependencies

Jobs can depend on multiple other jobs.

Example:

```text
        Job A
       /     \
      Job B   Job C
       \     /
        Job D
```

Job D executes only after both Job B and Job C complete successfully.

The system supports arbitrary DAG-based dependencies with no application-level dependency limit.

Features include:

- Multiple dependencies per job
- Cycle detection
- Dependency validation
- Dependency completion tracking
- Dependency failure propagation

---

### Cycle Detection

Circular dependencies are rejected.

Invalid example:

```text
A → B

B → C

C → A
```

The dependency graph is validated before the job is accepted.

---

### Priority Hierarchy Enforcement

Higher priority jobs cannot depend on lower priority jobs.

Invalid:

```text
HIGH JOB
    │
    ▼
LOW JOB
```

This prevents HIGH priority jobs from being blocked by lower priority work.

Priority rules:

```text
HIGH
 └── Can depend on HIGH

MEDIUM
 └── Can depend on HIGH or MEDIUM

LOW
 └── Can depend on HIGH, MEDIUM, or LOW
```

Conceptually:

```text
priority(dependency) >= priority(job)
```

---

<!-- JOB LIFECYCLE -->

## Job Lifecycle

Jobs move through the following lifecycle.

```text
PENDING
   │
   │ Dependencies Completed
   ▼
READY
   │
   │ Worker Receives Job
   ▼
PROCESSING
   │
   ├──────────────► COMPLETED
   │
   ▼
FAILED
   │
   ├── Retry Available ──► RETRYING ──► READY
   │
   └── Retry Limit Reached ──► DEAD
```

Additional states include:

```text
BLOCKED

CANCELLED
```

### Job States

| Status | Description |
|---|---|
| `PENDING` | Waiting for dependencies |
| `READY` | Ready to be published or processed |
| `PROCESSING` | Currently being executed |
| `COMPLETED` | Successfully completed |
| `FAILED` | Execution failed |
| `RETRYING` | Waiting for retry |
| `DEAD` | Retry limit reached |
| `BLOCKED` | Dependency failed or was cancelled |
| `CANCELLED` | Explicitly cancelled |

---

### Job Events

Job transitions are stored as events.

Example:

```text
JOB_CREATED

JOB_READY

JOB_PROCESSING

JOB_FAILED

JOB_RETRYING

JOB_COMPLETED
```

This provides:

- Audit history
- Debugging
- Failure analysis
- Latency tracking
- Job lifecycle inspection


<!-- PRIORITY -->

## Priority & Scheduling

### Priority Queues

```text
RabbitMQ

├── HIGH_QUEUE
│
├── MEDIUM_QUEUE
│
└── LOW_QUEUE
```

Each priority queue can use different durability guarantees.

Example:

```text
HIGH
Durable / Quorum Queue

MEDIUM
Durable Queue

LOW
Best Effort
```

Durability behavior is explicitly configured rather than silently dropping jobs.

---

### Scheduling Strategies

The project is designed to support multiple scheduling strategies.

#### Strict Priority

```text
HIGH
 ↓
MEDIUM
 ↓
LOW
```

Higher priority jobs are always preferred.

---

#### Weighted Round Robin

Example:

```text
HIGH   → 5 jobs

MEDIUM → 3 jobs

LOW    → 1 job
```

This prevents starvation while still prioritizing important jobs.

---

#### Priority Aging

Jobs waiting for a configured duration can receive a priority boost.

Example:

```text
LOW

Waits too long

↓

MEDIUM
```

This prevents jobs from remaining indefinitely in lower priority queues.


<!-- DAG -->

## DAG-based Dependencies

Dependencies are stored separately from jobs.

Example:

```text
jobs

job_dependencies
```

Conceptually:

```text
job_dependencies

job_id

depends_on_job_id
```

A job becomes `READY` only when all dependencies are successfully completed.

Example:

```text
Job A ──┐
        │
Job B ──┼──► Job D
        │
Job C ──┘
```

Job D waits for:

```text
A

B

C
```

---

### Dependency Failure

If a dependency permanently fails:

```text
Dependency

↓

DEAD
```

Dependent jobs can be handled using a configurable policy.

```text
BLOCK_DEPENDENT

CANCEL_DEPENDENT
```

This prevents jobs from remaining indefinitely in `PENDING` state.


<!-- RETRY -->

## Retry & Failure Handling

Jobs automatically retry when execution fails.

Default configuration:

```text
maxAttemptCount = 5
```

Each job can override the retry limit.

Example:

```json
{
  "maxAttemptCount": 10
}
```

The application can also configure a system-level safety limit.

---

### Retry Flow

```text
Worker
  │
  ▼
Job Execution
  │
  ▼
Failure
  │
  ▼
DELAY_QUEUE
  │
  │ TTL + Jitter
  ▼
RETRY_QUEUE
  │
  ▼
Original Priority Queue
```

---

### Exponential Backoff

Retry delays increase with each failed attempt.

Example:

```text
Attempt 1 → 5 seconds

Attempt 2 → 30 seconds

Attempt 3 → 1 minute

Attempt 4 → 5 minutes
```

Jitter is applied to reduce synchronized retries.

Conceptually:

```text
delay = exponential_backoff + random_jitter
```

---

### Dead Jobs

A job becomes `DEAD` when:

```text
attemptCount >= maxAttemptCount
```

Dead jobs are sent to:

```text
DEAD_LETTER_QUEUE
```

and persisted in PostgreSQL.

---

### Dead Job Revival

Dead jobs can be revived.

Supported operations:

```text
Single Job Revival

Bulk Dead Job Revival
```

On revival:

```text
DEAD

↓

READY
```

Attempt counts can optionally be reset.


<!-- CANCELLATION -->

## Job Cancellation

Jobs can be cancelled before execution.

```text
PENDING

READY

↓

CANCELLED
```

Cancellation is dependency-aware.

Example:

```text
A → B → C
```

Cancelling `B` can result in:

```text
B → CANCELLED

C → BLOCKED
```

The project supports configurable cancellation behavior.

```text
BLOCK_DEPENDENTS

CASCADE_CANCEL
```

---

### Processing Job Cancellation

For long-running jobs, cancellation can use a cancellation token.

```text
Worker
   │
   ▼
Job Processing
   │
   ▼
Check Cancellation Signal
```

This allows jobs already in `PROCESSING` to stop safely when supported by the job handler.


<!-- RELIABILITY -->

## Reliability & Recovery

### PostgreSQL as Source of Truth

Job state is always persisted.

```text
PostgreSQL

Jobs

Dependencies

Events

Outbox Events
```

RabbitMQ is used for execution transport.

---

### Transactional Outbox

Jobs and outbox events are written in the same database transaction.

```text
BEGIN TRANSACTION

Save Job

Save Dependencies

Save Outbox Event

COMMIT
```

The Outbox Publisher asynchronously publishes pending events to RabbitMQ.

This prevents database and message broker consistency issues.

---

### Publisher Confirms

RabbitMQ publisher confirms are used to verify message delivery.

```text
Outbox Publisher
       │
       ▼
   RabbitMQ
       │
       ▼
Publisher Confirm
       │
       ▼
Mark Outbox Event Processed
```

---

### At-least-once Delivery

The system provides at-least-once delivery.

This means a job may occasionally be delivered more than once during failures.

Example:

```text
Worker executes job

↓

Worker crashes before ACK

↓

RabbitMQ redelivers job
```

To safely handle this, jobs support idempotency.

---

### Idempotency

Jobs can include an idempotency key.

Example:

```json
{
  "type": "EMAIL",
  "idempotencyKey": "email-user-123-2026"
}
```

The system prevents duplicate execution when possible by tracking completed idempotency keys.

This allows safe retries and message redelivery.

---

### Worker Heartbeats

Workers periodically report their health.

Example:

```text
Worker

↓

Heartbeat

↓

Redis / PostgreSQL
```

Worker metadata includes:

```text
workerId

supportedJobTypes

currentLoad

maxConcurrency

lastHeartbeat
```

---

### Job Lease

Processing jobs have a lease.

```text
PROCESSING

workerId = worker-1

leaseExpiresAt = timestamp
```

If the worker crashes and the lease expires:

```text
PROCESSING

↓

Recovery

↓

READY

↓

Re-publish
```

This prevents jobs from remaining indefinitely in `PROCESSING`.

---

### Graceful Shutdown

Workers support graceful shutdown.

```text
Worker Shutdown Requested

↓

Stop Receiving New Jobs

↓

Finish Active Jobs

↓

ACK Messages

↓

Shutdown
```

If a job cannot complete before the configured timeout:

```text
NACK

↓

Requeue
```

This allows workers to safely scale down.


<!-- RATE LIMITING -->

## Rate Limiting

Rate limiting is handled at the API Gateway layer.

Redis provides distributed rate limit state.

Supported limits can include:

```text
Per Client

Per API Key

Per Job Type
```

The default algorithm is Token Bucket.

Example:

```text
Client A

100 jobs / minute
```

If the limit is exceeded:

```text
HTTP 429

Too Many Requests
```

---

### Backpressure

The system can protect workers during heavy traffic.

Example:

```text
Queue Depth > Threshold
```

Possible behavior:

```text
HIGH
Accept

MEDIUM
Throttle

LOW
Reject / Best Effort
```

This prevents uncontrolled queue growth.


<!-- WORKERS -->

## Workers & Scaling

Workers use RabbitMQ's competing consumer model.

```text
RabbitMQ
    │
    ├── Worker 1
    │
    ├── Worker 2
    │
    └── Worker N
```

Workers support configurable:

```text
Concurrency

Prefetch Count

Supported Job Types
```

---

### Worker Scaling

Scaling decisions can use:

```text
Queue Depth

Worker Utilization

Oldest Message Age

Processing Latency
```

Example:

```text
Queue Depth > 1000

↓

Scale Up Workers
```

Scale-down operations use a cooldown period to prevent rapid scaling changes.

```text
Queue Depth Low

↓

Graceful Drain

↓

Scale Down
```

The initial implementation is focused on local execution and simulation, while the architecture is designed to support external autoscaling systems later.


<!-- RABBITMQ FAILURE -->

## RabbitMQ Failure Recovery

RabbitMQ should not be treated as the only source of job state.

If RabbitMQ becomes unavailable:

```text
RabbitMQ Down
     │
     ▼
Jobs remain in PostgreSQL

Outbox Events remain pending
     │
     ▼
RabbitMQ Recovers
     │
     ▼
Outbox Publisher Republishes Jobs
```

This ensures durable jobs can recover after message broker failures.

High priority jobs can use stronger durability guarantees such as RabbitMQ quorum queues.


<!-- OBSERVABILITY -->

## Monitoring & Observability

The system exposes application and queue metrics.

### Job Metrics

```text
jobs_created_total

jobs_completed_total

jobs_failed_total

jobs_retried_total

jobs_dead_total
```

### Latency Metrics

```text
queue_wait_time

execution_time

end_to_end_latency
```

### Worker Metrics

```text
active_workers

worker_utilization

worker_failures

worker_heartbeats
```

### RabbitMQ Metrics

```text
queue_depth

messages_ready

messages_unacked

publish_rate

consume_rate
```

---

### Observability Stack

```text
Spring Boot Actuator
        │
        ▼
    Micrometer
        │
        ▼
   Prometheus
        │
        ▼
     Grafana
```

Structured JSON logs are also generated for job and worker events.


<!-- BUILT WITH -->

## Built With

- Java
- Spring Boot
- Spring AMQP
- RabbitMQ
- PostgreSQL
- Redis
- NGINX
- Docker
- Docker Compose
- Python
- Prometheus
- Grafana


<!-- API -->

## API Endpoints

### Jobs

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/jobs` | Create a new job |
| `GET` | `/api/v1/jobs` | List jobs with pagination |
| `GET` | `/api/v1/jobs/{id}` | Get job by ID |
| `GET` | `/api/v1/jobs/filter` | Filter jobs by status and/or type |
| `POST` | `/api/v1/jobs/{id}/cancel` | Cancel a job |
| `POST` | `/api/v1/jobs/{id}/revive` | Revive a dead job |
| `POST` | `/api/v1/jobs/revive` | Revive all dead jobs |

### Dependencies

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/jobs/{id}/dependencies` | Add dependencies |
| `GET` | `/api/v1/jobs/{id}/dependencies` | Get job dependencies |
| `DELETE` | `/api/v1/jobs/{id}/dependencies/{dependencyId}` | Remove dependency |

### Workers

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/v1/workers` | List active workers |
| `GET` | `/api/v1/workers/{id}` | Get worker details |
| `POST` | `/api/v1/workers/{id}/drain` | Gracefully drain worker |

### Dashboard

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/v1/dashboard/jobs/summary` | Job counts grouped by status |
| `GET` | `/api/v1/dashboard/queue-metrics` | RabbitMQ queue metrics |
| `GET` | `/api/v1/dashboard/workers` | Worker metrics |
| `GET` | `/api/v1/dashboard/jobs/latency` | Job latency metrics |
| `GET` | `/api/v1/dashboard/jobs/failures` | Job failure metrics |


<!-- JOB REQUEST -->

## Sample Job Request

```json
{
  "type": "EMAIL",
  "priority": "HIGH",
  "payload": {
    "recipient": "user@example.com",
    "subject": "Welcome",
    "body": "Welcome to the platform"
  },
  "maxAttemptCount": 5,
  "idempotencyKey": "email-123"
}
```

### Job With Dependencies

```json
{
  "type": "GENERATE_REPORT",
  "priority": "MEDIUM",
  "dependencies": [
    "job-id-1",
    "job-id-2"
  ],
  "maxAttemptCount": 5
}
```


<!-- PROJECT STRUCTURE -->

## Project Structure

```text
distributed-job-queue/

├── api-gateway/
│
├── job-service/
│   ├── controller/
│   ├── service/
│   ├── repository/
│   ├── model/
│   ├── dependency/
│   ├── scheduler/
│   └── config/
│
├── worker/
│   ├── consumer/
│   ├── executor/
│   ├── handler/
│   └── heartbeat/
│
├── outbox-publisher/
│
├── common/
│
├── infrastructure/
│   ├── nginx/
│   ├── rabbitmq/
│   ├── postgres/
│   └── redis/
│
├── monitoring/
│   ├── prometheus/
│   └── grafana/
│
├── scripts/
│   ├── create_jobs.py
│   ├── priority_test.py
│   ├── dependency_test.py
│   ├── retry_test.py
│   ├── cancellation_test.py
│   ├── dead_job_test.py
│   ├── rate_limit_test.py
│   ├── worker_failure_test.py
│   ├── rabbitmq_failure_test.py
│   └── load_test.py
│
├── docs/
│   └── images/
│       └── architecture.png
│
├── docker-compose.yml
│
└── README.md
```


<!-- GETTING STARTED -->

## Getting Started

### Prerequisites

Install the following:

- Java 21+
- Maven or Gradle
- Python 3.10+
- Docker
- Docker Compose

The infrastructure services are started through Docker Compose.

These include:

```text
RabbitMQ

PostgreSQL

Redis

NGINX

Prometheus

Grafana
```


### Installation & Usage

#### 1. Clone the repository

```sh
git clone <repository-url>

cd distributed-job-queue
```

---

#### 2. Start infrastructure services

```sh
docker compose up -d
```

Check running containers:

```sh
docker ps
```

---

#### 3. Start Job Service

```sh
cd job-service
```

Build the application:

```sh
./mvnw clean install
```

Run the service:

```sh
./mvnw spring-boot:run
```

---

#### 4. Start API Gateway

```sh
cd api-gateway
```

Run:

```sh
./mvnw spring-boot:run
```

---

#### 5. Start Workers

Workers can be started as multiple application instances.

```sh
cd worker

./mvnw spring-boot:run
```

Run additional workers in separate terminals or through Docker.

---

#### 6. Start Outbox Publisher

```sh
cd outbox-publisher

./mvnw spring-boot:run
```

The Outbox Publisher continuously checks for pending events and publishes them to RabbitMQ.


<!-- TESTING -->

## Testing

Python scripts are provided to test individual components and distributed system failure scenarios.

Install dependencies:

```sh
pip install -r scripts/requirements.txt
```

---

### Create Jobs

```sh
python scripts/create_jobs.py
```

---

### Priority Testing

Creates HIGH, MEDIUM, and LOW priority jobs.

```sh
python scripts/priority_test.py
```

Verifies priority scheduling behavior.

---

### Dependency Testing

Creates DAG-based workflows.

```sh
python scripts/dependency_test.py
```

Tests:

- Multiple dependencies
- Dependency completion
- Cycle detection
- Dependency failure propagation

---

### Retry Testing

```sh
python scripts/retry_test.py
```

Tests:

- Retry attempts
- Backoff
- Jitter
- Retry queue routing
- Dead job handling

---

### Cancellation Testing

```sh
python scripts/cancellation_test.py
```

Tests:

- Pending job cancellation
- Ready job cancellation
- Dependency-aware cancellation
- Cascade cancellation

---

### Dead Job Testing

```sh
python scripts/dead_job_test.py
```

Tests:

- Dead job creation
- Single revival
- Bulk revival

---

### Rate Limit Testing

```sh
python scripts/rate_limit_test.py
```

Tests:

- Per-client limits
- Token Bucket behavior
- HTTP 429 responses

---

### Worker Failure Testing

```sh
python scripts/worker_failure_test.py
```

Tests:

```text
Job Processing

↓

Worker Crash

↓

Lease Expiration

↓

Recovery

↓

Job Requeued
```

---

### RabbitMQ Failure Testing

```sh
python scripts/rabbitmq_failure_test.py
```

Tests:

```text
Create Jobs

↓

Stop RabbitMQ

↓

Restart RabbitMQ

↓

Outbox Recovery

↓

Jobs Published Again
```

---

### Load Testing

```sh
python scripts/load_test.py
```

The load test can simulate:

- Multiple producers
- Concurrent requests
- High queue depth
- Worker scaling
- Priority distribution

Metrics collected include:

```text
Throughput

Queue Wait Time

Execution Time

End-to-End Latency

Failure Rate
```


<!-- DESIGN DECISIONS -->

## Design Decisions & Tradeoffs

### Why RabbitMQ Instead of Kafka?

This project focuses on job execution.

RabbitMQ provides features that align naturally with job queues:

- Message acknowledgement
- Competing consumers
- Routing
- Dead letter queues
- Retry queues
- Queue-based dispatch
- Prefetch configuration

Kafka is primarily designed for durable event streaming and log-based consumption.

Jobs are therefore published directly to RabbitMQ.

---

### Why PostgreSQL?

RabbitMQ should not be the source of truth for job state.

PostgreSQL stores:

```text
Job Metadata

Dependencies

Lifecycle State

Events

Retry Counts

Cancellation State

Outbox Events
```

This also enables recovery when RabbitMQ is unavailable.

---

### Why Transactional Outbox?

Database writes and RabbitMQ publishes cannot safely be treated as one transaction.

The Outbox pattern guarantees:

```text
Job Persisted

+

Publish Event Persisted

↓

Eventually Published to RabbitMQ
```

---

### Why Redis?

Redis handles lightweight distributed concerns.

```text
Rate Limiting

Distributed Locks

Worker Heartbeats

Caching
```

Persistent job state remains in PostgreSQL.

---

### Delivery Guarantee

The system uses:

```text
At-Least-Once Delivery
```

Exactly-once delivery is difficult to guarantee in distributed systems.

Idempotency is therefore used to safely handle duplicate delivery.


<!-- FUTURE -->

## Future Improvements

Potential future enhancements include:

- Kubernetes deployment
- KEDA-based worker autoscaling
- RabbitMQ cluster deployment
- Quorum queues for all durable priorities
- Dynamic worker capability routing
- Distributed tracing
- OpenTelemetry integration
- Advanced scheduling policies
- Scheduled jobs
- Cron-based recurring jobs
- Job execution time limits
- Multi-tenant isolation
- Web dashboard
- Job replay
- Workflow versioning
- External authentication providers


<!-- KEY LEARNINGS -->

## Key Distributed Systems Concepts

This project explores several important distributed systems concepts.

- Message queues
- Asynchronous processing
- Competing consumers
- At-least-once delivery
- Idempotency
- Transactional Outbox
- Distributed rate limiting
- DAG scheduling
- Cycle detection
- Retry strategies
- Exponential backoff
- Jitter
- Dead letter queues
- Worker heartbeats
- Job leases
- Failure recovery
- Backpressure
- Graceful shutdown
- Priority scheduling
- Starvation prevention
- Horizontal scaling
- Fault tolerance
