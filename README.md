# ⚡ Fast Booking API

A high-concurrency booking API built with **Java 21, Spring Boot, MongoDB, Redis, and Apache Kafka**.

This project demonstrates how a seemingly correct booking implementation can fail under concurrent traffic, causing **overselling**, and how atomic database operations, idempotency, failure compensation, and reliable event publishing can improve the design.

The project intentionally includes both a **BEFORE** and an **AFTER** implementation so the concurrency problem and its solution can be demonstrated and measured.

---

## 🎯 Problem

A booking system must guarantee that confirmed bookings never exceed the available capacity.

For example:

```text
Slot Capacity               = 10
Concurrent Booking Requests = 100
```

The fundamental business invariant is:

```text
confirmed bookings <= slot capacity
```

A traditional read-check-write implementation can violate this invariant when multiple requests execute concurrently.

---

# 🏗 Architecture

```text
                           ┌──────────────┐
                           │    Client    │
                           └──────┬───────┘
                                  │
                                  ▼
                       ┌────────────────────┐
                       │ Spring Boot API    │
                       │      Java 21       │
                       └─────────┬──────────┘
                                 │
                  ┌──────────────┼──────────────┐
                  │              │              │
                  ▼              ▼              ▼
            ┌──────────┐   ┌──────────┐   ┌──────────────┐
            │ MongoDB  │   │  Redis   │   │Outbox Events │
            │          │   │          │   │   MongoDB    │
            └──────────┘   └──────────┘   └──────┬───────┘
                  │              │                │
                  │              │                ▼
                  │              │         ┌──────────────┐
                  │              │         │   Outbox     │
                  │              │         │  Publisher   │
                  │              │         └──────┬───────┘
                  │              │                │
                  │              │                ▼
                  │              │         ┌──────────────┐
                  │              │         │ Apache Kafka │
                  │              │         └──────────────┘
                  │              │
             Capacity        Idempotency
             Bookings          Cache
```

---

# ❌ BEFORE — Non-Atomic Booking

The initial implementation follows a traditional read-check-write flow:

```text
Read Slot
    ↓
Check Capacity
    ↓
Increment booked
    ↓
Save Slot
    ↓
Create Booking
```

Example:

```java
var slotOp = slotRepository.findById(request.slotId());

if (slotOp.isEmpty()) {
    throw new IllegalStateException("SLOT_NOT_FOUND");
}

var slot = slotOp.get();

if (slot.getBooked() < slot.getCapacity()) {
    slot.setBooked(slot.getBooked() + 1);
    slotRepository.save(slot);
} else {
    throw new IllegalStateException("SLOT_FULL");
}
```

This implementation contains a race condition.

Multiple requests can read the same `booked` value before another request persists its update.

Example:

```text
Capacity = 10
Current booked = 9

Thread A reads booked = 9
Thread B reads booked = 9
Thread C reads booked = 9

All evaluate:

9 < 10

All proceed with the booking operation.
```

Under concurrent traffic, the application can therefore accept more bookings than the configured capacity.

---

# ✅ AFTER — Atomic Capacity Reservation

The improved implementation performs the capacity check and increment as a **single atomic MongoDB operation**.

```java
Query query = Query.query(
    Criteria.where("_id")
        .is(request.slotId())
        .and("$expr")
        .is(
            new Document(
                "$lt",
                List.of("$booked", "$capacity")
            )
        )
);

Update update = new Update()
    .inc("booked", 1);

Slot updated = mongoTemplate.findAndModify(
    query,
    update,
    FindAndModifyOptions.options().returnNew(true),
    Slot.class
);

if (updated == null) {
    throw new IllegalStateException("SLOT_FULL");
}
```

Conceptually:

```text
booked < capacity
        +
$inc booked by 1
        ↓
Single Atomic Operation
```

MongoDB performs the condition check and increment atomically on the slot document.

Concurrent requests can no longer independently read the same capacity value and then update it later.

This protects the core invariant:

```text
confirmed bookings <= capacity
```

---

# 🔐 Idempotency

Booking requests contain an idempotency key:

```json
{
  "slotId": "slot-load-test",
  "userId": "customer-1",
  "idempotencyKey": "unique-request-id"
}
```

The AFTER implementation uses two levels of idempotency protection.

## Redis Fast Path

Redis stores:

```text
booking:idem:{idempotencyKey}
```

A repeated request can therefore return an existing booking without repeating the booking operation.

```text
Request
   ↓
Redis
   │
   ├── Found ──────► Return existing booking
   │
   └── Not Found
          ↓
       MongoDB
```

## Durable MongoDB Check

If the Redis entry does not exist, MongoDB is checked using the idempotency key.

This prevents correctness from depending entirely on the Redis cache.

Conceptually:

```text
Redis
  ↓
Cache Miss
  ↓
MongoDB Idempotency Check
  ↓
Existing Booking?
  ├── YES → Return existing booking
  └── NO  → Continue booking
```

The idempotency key is also protected by a unique database constraint so concurrent requests using the same key cannot create multiple durable bookings.

---

# 🛡 Failure Compensation

Atomic capacity reservation happens before the booking is persisted.

That creates another failure scenario:

```text
Reserve Capacity     ✅
        ↓
Save Booking         ❌
```

Without compensation, the slot counter would remain incremented even though no booking was created.

Example:

```text
Capacity = 10
booked   = 10

Actual confirmed bookings = 9
```

One unit of capacity would effectively be lost.

The AFTER implementation therefore performs a compensating operation when booking persistence fails:

```text
Reserve Capacity
      ↓
Save Booking
   ↙       ↘
Success   Failure
            ↓
      Release Capacity
            ↓
      booked = booked - 1
```

The release operation performs:

```java
Update update = new Update()
    .inc("booked", -1);
```

with protection against decrementing below zero.

Once the booking has been successfully persisted, the capacity is considered consumed.

Therefore, failures in downstream operations such as Redis or event publishing must **not** release the capacity of an already confirmed booking.

---

# 📨 Outbox-Based Event Publishing

Confirmed bookings generate events that must eventually be published to Kafka.

Publishing directly from the request flow creates a failure scenario:

```text
Booking saved       ✅
        ↓
Kafka unavailable   ❌
        ↓
Event lost
```

To reduce this coupling, the application uses an **Outbox Pattern**.

Instead of publishing directly to Kafka from the booking request, an outbox event is stored in MongoDB.

```text
Booking Request
       ↓
Atomic Capacity Reservation
       ↓
Persist Booking
       ↓
Create PENDING Outbox Event
       ↓
Update Redis
       ↓
Return Response
```

A separate background publisher processes the events:

```text
Outbox Publisher
       ↓
Read PENDING Events
       ↓
Publish to Kafka
       ↓
Kafka Acknowledgement
       ↓
Mark PUBLISHED
```

---

## Outbox Event Lifecycle

A newly created event starts as:

```text
PENDING
```

After Kafka confirms successful publishing:

```text
PENDING
   ↓
PUBLISHED
```

If publishing fails, the event remains available for retry and its retry counter is incremented.

After the configured maximum number of attempts:

```text
PENDING
   ↓
Retry
   ↓
Retry
   ↓
FAILED
```

This allows Kafka failures to be tracked instead of silently losing events.

---

## ⚠️ Transactional Boundary

This portfolio implementation uses a standalone MongoDB instance.

The booking document and outbox event are therefore **not written within the same MongoDB transaction**.

A failure could theoretically occur between:

```text
bookingRepository.save()     ✅

          application crash

outboxEventRepository.save() ❌
```

For that reason, this project should be described as demonstrating an:

```text
Outbox-based reliable event publishing pattern
```

rather than claiming a fully transactional outbox implementation.

A production extension could run MongoDB with transaction support and persist:

```text
Booking
   +
Outbox Event
```

within the same database transaction.

---

# 🔄 Final AFTER Flow

The resulting booking flow is:

```text
                    Booking Request
                           │
                           ▼
                 Redis Idempotency Check
                           │
                     Cache Miss
                           │
                           ▼
                 MongoDB Idempotency Check
                           │
                      Not Found
                           │
                           ▼
                Atomic Capacity Reserve
                           │
                           ▼
                    Save Booking
                      │         │
                 Success      Failure
                      │         │
                      │         ▼
                      │   Release Capacity
                      │
                      ▼
                Save Outbox Event
                      │
                      ▼
                 Update Redis
                      │
                      ▼
                Return Response


                Background Process
                      │
                      ▼
             Read PENDING Events
                      │
                      ▼
                Publish Kafka
                   │       │
               Success   Failure
                   │       │
                   ▼       ▼
              PUBLISHED   Retry
```

---

# 🧪 Load Testing

Load tests were executed using **Apache JMeter 5.6.3**.

Two different types of tests were performed:

1. **Concurrency correctness testing**
2. **Performance/load testing**

These tests intentionally measure different characteristics of the system.

---

# 🚨 Concurrency Correctness Test

The most important test verifies whether the application maintains capacity correctly under concurrent traffic.

Configuration:

```text
Threads:       100
Ramp-up:       1 second
Loop Count:    1
Slot Capacity: 10
```

---

## ❌ BEFORE Result

The non-atomic implementation produced:

```text
Concurrent Requests: 100
Capacity:             10
Confirmed Bookings:   14
Rejected Requests:    86
```

Result:

```text
14 confirmed > 10 capacity
```

**Overselling occurred.**

Four bookings were accepted beyond the configured capacity during this test run.

```text
Capacity
10 ──────────────────────┐
                         │
                         │ +4
                         ▼
Confirmed Bookings = 14
```

---

## ✅ AFTER Result

The atomic implementation produced:

```text
Concurrent Requests: 100
Capacity:             10
Confirmed Bookings:   10
Rejected Requests:    90
Final booked value:   10
```

Result:

```text
confirmed bookings == capacity
```

**No overselling occurred.**

The 90 rejected requests represent expected business rejections because the slot had reached capacity; they are not interpreted as infrastructure failures.

---

# 📊 Performance Benchmark

For performance testing, slot capacity was increased to:

```text
capacity = 1000
```

This prevented normal capacity rejection from distorting the latency and throughput measurements.

The following numbers represent the observed results from the executed JMeter test runs.

---

## 100 Concurrent Users

| Metric | BEFORE | AFTER |
|---|---:|---:|
| Requests | 100 | 100 |
| Average | 15 ms | 15 ms |
| Median | 14 ms | 16 ms |
| P90 | 20 ms | 19 ms |
| P95 | 30 ms | 19 ms |
| P99 | 45 ms | 20 ms |
| Max | 46 ms | 20 ms |
| Throughput | 98.6 req/s | 100.2 req/s |
| Error Rate | 0% | 0% |

At this load level, average latency was equal while the AFTER implementation showed lower tail latency in this particular test run.

---

## 200 Concurrent Users

| Metric | BEFORE | AFTER |
|---|---:|---:|
| Requests | 200 | 200 |
| Average | 24 ms | 30 ms |
| Median | 14 ms | 18 ms |
| P90 | 60 ms | 81 ms |
| P95 | 74 ms | 107 ms |
| P99 | 86 ms | 139 ms |
| Max | 90 ms | 174 ms |
| Throughput | 200.0 req/s | 194.6 req/s |
| Error Rate | 0% | 0% |

At 200 concurrent users, the BEFORE implementation showed lower latency in this particular run.

However, the concurrency correctness test demonstrates that the BEFORE implementation cannot reliably enforce slot capacity.

---

## 500 Concurrent Users

| Metric | BEFORE | AFTER |
|---|---:|---:|
| Requests | 500 | 500 |
| Average | 882 ms | 1080 ms |
| Median | 964 ms | 1179 ms |
| P90 | 1139 ms | 1331 ms |
| P95 | 1164 ms | 1382 ms |
| P99 | 1201 ms | 1446 ms |
| Max | 1252 ms | 1499 ms |
| Throughput | 245.0 req/s | 218.7 req/s |
| Error Rate | 0% | 0% |

A significant latency increase was observed between the 200-user and 500-user tests.

This indicates a saturation or queueing point somewhere in the tested environment.

The JMeter results alone are not sufficient to attribute the bottleneck to MongoDB, Redis, Kafka, the application thread pool, the MongoDB connection pool, JVM resources, or the local load-generation environment.

Additional profiling would be required to identify the bottleneck.

---

# 📈 BEFORE vs AFTER

The primary goal of the AFTER implementation is **correctness under concurrency**, not simply producing a lower average response time.

Observed correctness test:

| | BEFORE | AFTER |
|---|---:|---:|
| Concurrent Requests | 100 | 100 |
| Capacity | 10 | 10 |
| Confirmed Bookings | **14 ❌** | **10 ✅** |
| Rejected Requests | 86 | 90 |
| Overselling | **YES ❌** | **NO ✅** |
| Capacity Enforcement | Read/Check/Write | Atomic |
| Idempotency | Database check | Redis + MongoDB |
| Failure Compensation | No | Yes |
| Event Publishing | Request flow | Outbox-based |

The BEFORE implementation was faster in some benchmark runs, but it violated the fundamental business invariant.

The AFTER implementation prioritizes:

```text
Correctness
    +
Idempotency
    +
Failure Handling
    +
Reliable Event Delivery
```

while exposing the performance cost of contention for further analysis.

---

# 🛠 Technology Stack

- **Java 21**
- **Spring Boot**
- **Spring Data MongoDB**
- **MongoDB 8**
- **Redis 7**
- **Apache Kafka 3.9**
- **Docker / Docker Compose**
- **Apache JMeter 5.6.3**
- **Maven**

---

# 🚀 Running the Infrastructure

Start the infrastructure with:

```bash
docker compose up -d
```

Services:

```text
MongoDB → localhost:27017
Redis   → localhost:6379
Kafka   → localhost:9092
```

Verify:

```bash
docker ps
```

Then start the Spring Boot application.

---

# 📂 Project Structure

```text
fast-booking-api/
│
├── src/
│   ├── main/
│   │   └── java/
│   │       └── com/kareem/booking/
│   │
│   │           ├── controller/
│   │           ├── dto/
│   │           ├── model/
│   │           ├── repository/
│   │           └── service/
│   │
│   └── test/
│
├── load-tests/
│   └── fast-booking-test.jmx
│
├── docker-compose.yml
├── pom.xml
├── README.md
└── .gitignore
```

---

# 📌 Engineering Lessons

This project demonstrates practical backend engineering concepts including:

- Race conditions in read-modify-write workflows
- Concurrency-safe capacity enforcement
- Atomic MongoDB operations
- Idempotent API design
- Redis-backed idempotency fast paths
- Durable duplicate detection
- Compensating actions for partial failures
- Failure boundaries in distributed workflows
- Outbox-based event publishing
- Kafka publishing retries
- Event failure tracking
- Event-driven architecture
- Load testing with JMeter
- Average vs percentile latency
- P95/P99 latency analysis
- Throughput analysis
- Saturation and queueing behavior
- Separating correctness testing from performance testing

---

# 🔭 Potential Production Improvements

The project intentionally stops short of introducing unnecessary production infrastructure.

Potential extensions include:

- Use MongoDB transactions for atomic booking + outbox persistence
- Run MongoDB as a replica set
- Add safe coordination when multiple Outbox Publisher instances are running
- Add dead-letter handling for permanently failed events
- Add exponential backoff for event retries
- Profile the latency increase between 200 and 500 concurrent users
- Analyze MongoDB connection-pool behavior
- Analyze Spring Boot/Tomcat thread-pool saturation
- Monitor JVM CPU, heap and garbage collection
- Add Spring Boot Actuator and Micrometer
- Export metrics to Prometheus
- Build Grafana dashboards
- Run repeatable JMeter tests in non-GUI mode
- Run the load generator separately from the application infrastructure

---

# 💡 Key Takeaway

A booking API is not correct simply because it returns successful responses quickly.

Under concurrency, correctness requires protecting business invariants.

This project demonstrates the evolution from:

```text
Naive Read/Check/Write
          ↓
     Race Condition
          ↓
       Overselling
```

to:

```text
Atomic Capacity Reservation
          +
      Idempotency
          +
 Failure Compensation
          +
  Outbox Publishing
          ↓
Concurrency-Safe Booking Flow
```

The most important measured result was:

```text
100 concurrent requests
10 available slots

BEFORE → 14 confirmed bookings ❌
AFTER  → 10 confirmed bookings ✅
```

The optimization therefore focuses not only on speed, but on building a booking workflow that remains correct when requests arrive concurrently.