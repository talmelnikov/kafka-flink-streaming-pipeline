# kafka-flink-streaming-pipeline

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![Apache Flink](https://img.shields.io/badge/Apache%20Flink-E6526F?style=flat&logo=apacheflink&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)

A real-time event streaming pipeline that ingests data from external sources and outputs aggregated results using Apache Kafka and Apache Flink — built to handle high-throughput production workloads.

---

## Overview

This pipeline was designed to process large-scale event streams in real time. It uses Kafka for durable, partitioned message ingestion and Flink for stateful stream processing with custom windowing logic — enabling low-latency computation over continuous data flows.

**Key scale metrics:**
- 180M+ events processed per day
- 18 parallel Kafka partitions
- Event-time processing with watermarking

---

## Architecture

```
External Sources
      │
      ▼
  Kafka Topics (18 partitions)
      │
      ▼
  Flink Job (stateful, event-time windows)
      │
      ▼
  Sink: PostgreSQL / S3 / Downstream consumers
```

---

## Features

- **Kafka producer** — ingests events from external sources into partitioned topics
- **Flink stateful job** — event-time windowing with watermarks for late data handling
- **Custom window logic** — tumbling/sliding windows for aggregation use cases
- **Fault tolerance** — checkpointing enabled for at-least-once processing guarantees
- **Observability** — metrics exposed for Grafana / New Relic integration

---

## Tech Stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka |
| Stream processing | Apache Flink |
| Language | Python / Java |
| Containerization | Docker |
| Monitoring | Grafana, New Relic |

---

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.9+
- Java 11+ (for Flink)

### Run locally

```bash
git clone https://github.com/talmelnikov/kafka-flink-streaming-pipeline
cd kafka-flink-streaming-pipeline

# Start Kafka + Zookeeper
docker-compose up -d

# Install Python dependencies
pip install -r requirements.txt

# Submit Flink job
python producer.py        # Start producing events
python flink_job.py       # Submit the processing job
```

---

## Project Structure

```
kafka-flink-streaming-pipeline/
├── producer/
│   └── producer.py          # Kafka event producer
├── flink_jobs/
│   └── streaming_job.py     # Flink stateful processing job
├── config/
│   └── settings.py          # Kafka + Flink configuration
├── docker-compose.yml        # Local Kafka + Zookeeper setup
├── requirements.txt
└── README.md
```

---

## What I Learned

- Designing partitioning strategies for high-throughput Kafka topics
- Implementing event-time processing with watermarks to handle out-of-order events
- Tuning Flink checkpointing intervals for latency vs. fault-tolerance tradeoffs
- Observability patterns for production streaming systems

---

## Author

**Tal Melnikov** — [LinkedIn](https://www.linkedin.com/in/tal-melnikov/) · [GitHub](https://github.com/talmelnikov)
