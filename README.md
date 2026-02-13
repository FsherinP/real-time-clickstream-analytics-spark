---

# Real-Time Telemetry Analytics Engine

### Apache Spark Structured Streaming + Kafka

## 📌 Project Overview

This project implements a production-grade real-time telemetry analytics pipeline. It consumes nested frontend telemetry events from **Apache Kafka** using **Spark Structured Streaming** to model user behavior and content engagement.

Unlike basic aggregation demos, this engine implements **stateful stream processing** to track user journeys and session lifecycles in real-time.

### Key Learning Objectives

1. **Event-time systems:** Handling late-arriving data using watermarks.
2. **Stateful Processing:** Using `mapGroupsWithState` for complex behavioral funnels.
3. **Schema Enforcement:** Parsing deeply nested JSON in high-throughput streams.
4. **Product Analytics:** Driving DAU and retention metrics from raw event logs.

---

## 🏗 Architecture

1. **Ingestion:** Frontend apps push Sunbird-style telemetry to Kafka.
2. **Processing:** Spark extracts, normalizes, and enriches nested JSON fields.
3. **Analytics:** Parallel streams handle simple aggregations and complex stateful transitions.
4. **Sink:** Results are currently output to Console/Sinks (Extensible to Delta Lake or PowerBI).

---

## 📦 Telemetry Event Structure

The pipeline processes nested JSON events following this schema:

```json
{
  "eid": "IMPRESSION",
  "ets": 1770888766103,
  "ver": "3.0",
  "actor": { "id": "user-id", "type": "User" },
  "context": {
    "pdata": { "id": "prod.portal", "ver": "4.8.14" },
    "did": "device-id"
  },
  "object": { "id": "content_123" },
  "edata": { "pageid": "/home", "duration": 120 }
}

```

**Normalization Mapping:**

* `user_id` ➔ `actor.id`
* `platform` ➔ `context.pdata.id`
* `content_id` ➔ `object.id`
* `event_time` ➔ Derived from `ets` (epoch)

---

## 🚀 Features Implemented

### 1. Daily Active Users (DAU)

Calculates unique users per event date using event-time processing and watermarking to ensure accuracy even with delayed data.

### 2. Platform & Adoption Analytics

Groups active users by platform (Mobile vs. Web) and application version to measure the impact of new releases.

### 3. Real-Time Top Pages

A rolling **30-minute window** analysis to identify high-traffic content and potential UX bottlenecks as they happen.

### 4. Stateful Funnel Tracking (Advanced)

Uses `mapGroupsWithState` to track the `(user_id, content_id)` journey across three states:

* **IMPRESSION** ➔ **START** ➔ **END**

**Logic Details:**

* Maintains state across micro-batches.
* **Inactivity Timeout:** 30-minute window before the state is cleared.
* **Output:** Emits a completed session record once the "END" event is processed or timeout occurs.

---

## 🗂 Project Structure

```text
real-time-telemetry-spark/
├── src/
│   ├── __init__.py
│   ├── schema.py            # Spark StructType definitions
│   ├── streaming_app.py     # Main entry point for aggregations
│   └── stateful_funnel.py   # mapGroupsWithState logic
├── checkpoint/              # Streaming offset storage
├── requirements.txt
└── README.md

```

---

## ⚙️ Technology Stack

* **Engine:** Apache Spark 3.5 (Structured Streaming)
* **Broker:** Apache Kafka
* **Language:** Python (PySpark)
* **Design Patterns:** Event-time semantics, Watermarking, Stateful mapping.

---

## ▶️ Running the Application

### 1. Initialize Kafka

Create the required topic:

```bash
kafka-topics.sh --create --topic telemetry-events --bootstrap-server localhost:9092

```

### 2. Submit the Spark Job

Ensure you include the Kafka SQL connector:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  src/streaming_app.py

```

---

## 🔐 Fault Tolerance

The system utilizes **Checkpointing** and **Write-Ahead Logs (WAL)**. This ensures that if the Spark job fails, it can resume exactly where it left off without losing the internal state of user funnels.

---

## 🎯 Engineering Intent

This project demonstrates a shift from "batch-thinking" to "stream-thinking." By managing state manually in Spark, the engine can detect drop-offs and behavioral patterns that simple SQL-based streaming cannot capture.

---

**Author:** Fathima Sherin

*Focusing on Large-scale Data Engineering and Real-time Systems.*

---
