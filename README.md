# ids-data-platform

Distributed data platform for the Sentinel IDS/IPS. It moves processed telemetry
from the backend into a streaming pipeline and an S3 data lake for analytics and
the downstream consumers (web dashboard, iOS app, AI summarization, MISP).

```text
Backend REST API ──poll──► [ingestion] ──► Kafka topics ──► [processing / Spark] ──► S3 (Parquet)
```

## Modules

| Module       | Stack                                   | Role |
| ------------ | --------------------------------------- | ---- |
| `ingestion`  | Kotlin + Spring Boot + Spring Kafka     | Polls the backend `/api/alerts` and publishes each event to a Kafka topic (`ids.alerts`). Decoupled from the backend (no backend change, watermark-based incremental fetch). |
| `processing` | Kotlin + Spark Structured Streaming     | Consumes the Kafka topic, parses the JSON, and appends date-partitioned **Parquet** to **S3** (`s3a://…`). |

## Run locally

### 1. Infrastructure (Kafka)
```bash
cd docker && docker compose up -d        # Kafka on :9092, Kafka UI on :8089
```

### 2. Ingestion (backend → Kafka)
Requires the backend reachable (defaults to `http://localhost:8082`).
```bash
mvn -pl ingestion -am spring-boot:run
# or: BACKEND_BASE_URL=https://qa-api.puk3p.online KAFKA_BOOTSTRAP=localhost:9092 \
#     mvn -pl ingestion spring-boot:run
```

### 3. Processing (Kafka → S3 Parquet)
Needs AWS credentials + a bucket. Set env and run the shaded jar:
```bash
mvn -pl processing -am package
AWS_ACCESS_KEY_ID=...  AWS_SECRET_ACCESS_KEY=...  AWS_REGION=eu-central-1 \
KAFKA_BOOTSTRAP=localhost:9092 \
S3_OUTPUT=s3a://sentinel-ids-lake/alerts \
S3_CHECKPOINT=s3a://sentinel-ids-lake/_checkpoints/alerts \
java -jar processing/target/ids-data-platform-processing-0.1.0.jar
```
For a real cluster, submit instead:
`spark-submit --class ro.puk3p.sentinel.dataplatform.processing.AlertsToS3JobKt processing/target/…jar`

## Configuration

Ingestion (`ingestion/src/main/resources/application.yml`, overridable via env):
- `BACKEND_BASE_URL`, `KAFKA_BOOTSTRAP`, `ingestion.poll-interval-ms`, topic names.

Processing (env): `KAFKA_BOOTSTRAP`, `ALERTS_TOPIC`, `S3_OUTPUT`, `S3_CHECKPOINT`,
`SPARK_MASTER`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.

## Roadmap
- Additional topics (traffic, forensics) and Spark aggregations.
- Iceberg tables (ACID, schema evolution) over the raw Parquet.
- Wire S3 credentials through Vault / the infra-devops deploy flow.
