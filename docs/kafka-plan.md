# Kafka Ingest Plan

Alternative implementation of the ingest pipeline using Kafka as the durable
buffer between the gRPC receiver and ClickHouse. Implemented on a separate
branch to showcase the production-scale architecture while keeping the main
branch self-contained (no external dependencies beyond ClickHouse).

**Motivation:** the main branch uses an in-memory batcher (at-most-once,
data loss on hard kill). Kafka gives at-least-once delivery, independent
scaling of ingest and storage, and removes all batching complexity from the
Go service. The trade-off is an extra infrastructure dependency.

See the main branch README for the original design and the decision to keep
it dependency-light for the assignment.

---

## Architecture

```
gRPC Export()
     │
     ▼
  MapRows()          ← unchanged from main branch
     │
     ▼
Kafka Producer       ← replaces Batcher + MetricsStore calls
  (one topic per metric type)
     │
     ▼
ClickHouse Kafka     ← queue tables, one per topic
  Engine tables
     │  (Materialized Views)
     ▼
MergeTree tables     ← existing destination tables, unchanged
```

**What changes vs main branch:**
- `internal/ingest/batcher.go` — deleted
- `internal/ingest/cache.go` — deleted (series dedup handled by ReplacingMergeTree)
- `internal/storage/client.go` Insert* methods — deleted (CH pulls from Kafka)
- `MetricsStore` interface — deleted
- `Export()` — publishes to Kafka topics, returns immediately
- `schema.go` — adds 12 new DDL objects (6 queue tables + 6 MVs)
- `docker-compose.yml` — adds Redpanda (Kafka-compatible, no Zookeeper)

**What stays the same:**
- `MapRows()` and all row structs
- `computeSeriesID()` fingerprinting
- All 6 destination MergeTree tables
- `cmd/main.go` signal handling and health endpoint
- OTel instrumentation (adjusted counters)

---

## Topics

| Topic | Row type |
|---|---|
| `otlp.series` | `SeriesRow` |
| `otlp.gauge` | `GaugeDatapointRow` |
| `otlp.sum` | `SumDatapointRow` |
| `otlp.histogram` | `HistogramDatapointRow` |
| `otlp.exponential_histogram` | `ExponentialHistogramDatapointRow` |
| `otlp.summary` | `SummaryDatapointRow` |

Format: `JSONEachRow`. Map columns (`ResourceAttributes`, `ScopeAttributes`,
`Attributes`) serialize naturally as JSON objects via `json.Marshal` —
ClickHouse Kafka engine handles `Map(String, String)` with JSONEachRow natively.

---

## Phase 1 — Infrastructure: Redpanda + ClickHouse Queue Tables

**Goal:** Redpanda running locally, 6 Kafka queue tables and 6 materialized
views in ClickHouse. No Go changes yet. Verify CH consumes from topics manually.

### Tasks

1. Add Redpanda to `docker-compose.yml`:
   ```yaml
   redpanda:
     image: redpandadata/redpanda:latest
     command: redpanda start --overprovisioned --smp 1 --memory 512M
       --reserve-memory 0M --node-id 0 --check=false
       --kafka-addr PLAINTEXT://0.0.0.0:9092
       --advertise-kafka-addr PLAINTEXT://localhost:9092
     ports:
       - "9092:9092"
   ```
   Update `local-up` / `demo-up` targets — Redpanda must be healthy before
   the service starts.

2. Add `KafkaConfig` to `internal/config/config.go`:
   - `KAFKA_BROKERS` (default `localhost:9092`)
   - `KAFKA_TOPIC_PREFIX` (default `otlp`)

3. Add 6 queue tables + 6 materialized views to `internal/storage/schema.go`.
   Pattern per metric type (example: gauge):

   ```sql
   -- Queue table: Kafka engine reads from topic
   CREATE TABLE IF NOT EXISTS otel_metrics_gauge_queue (
       SeriesID       UInt64,
       StartTimeUnix DateTime64(9),
       TimeUnix       DateTime64(9),
       Flags          UInt32,
       Value          Float64
   ) ENGINE = Kafka(...)
     SETTINGS kafka_topic_list    = 'otlp.gauge',
              kafka_group_name    = 'clickhouse.otlp.gauge',
              kafka_format        = 'JSONEachRow',
              kafka_max_block_size = 65536;

   -- Materialized view: bridges queue → destination
   CREATE MATERIALIZED VIEW IF NOT EXISTS otel_metrics_gauge_mv
       TO otel_metrics_gauge AS
   SELECT * FROM otel_metrics_gauge_queue;
   ```

   Series queue table includes all `SeriesRow` fields including
   `Map(String, String)` columns — JSONEachRow reads them as JSON objects.

4. Update `CreateTables()` to execute queue tables and MVs after the existing
   destination tables.

### ✅ Done when
`make local-up` starts ClickHouse + Redpanda. `make test-integration` passes
(existing tests unaffected — queue tables and MVs are additive). Manually
publish a JSON row to `otlp.gauge` via `rpk topic produce` and verify it
appears in `otel_metrics_gauge`.

---

## Phase 2 — Go Producer

**Goal:** replace `Batcher` + `MetricsStore` calls in `Export()` with a Kafka
producer. Service publishes rows and returns immediately. No integration test
changes yet — end-to-end flow not wired in tests until Phase 3.

### Tasks

1. Add `github.com/segmentio/kafka-go` to `go.mod`:
   ```
   go get github.com/segmentio/kafka-go
   ```

2. Create `internal/ingest/producer.go`:
   ```go
   type Producer struct {
       writers map[string]*kafka.Writer  // topic → writer
   }

   func NewProducer(brokers []string, topicPrefix string) *Producer
   func (p *Producer) Publish(ctx context.Context, topic string, rows any) error
   func (p *Producer) Close() error
   ```
   `Publish` marshals `rows` as `JSONEachRow` (one JSON object per row,
   newline-separated) and sends as a single Kafka message batch.
   Log + return error on failure — caller decides whether to surface to gRPC.

3. Rewrite `internal/ingest/service.go` `Export()`:
   ```go
   func (m *dash0MetricsServiceServer) Export(ctx, request) {
       rows := MapRows(request.GetResourceMetrics())
       m.producer.Publish(ctx, "otlp.series", rows.Series)
       m.producer.Publish(ctx, "otlp.gauge", rows.Gauge)
       m.producer.Publish(ctx, "otlp.sum", rows.Sum)
       m.producer.Publish(ctx, "otlp.histogram", rows.Histogram)
       m.producer.Publish(ctx, "otlp.exponential_histogram", rows.ExponentialHistogram)
       m.producer.Publish(ctx, "otlp.summary", rows.Summary)
       return &colmetricspb.ExportMetricsServiceResponse{}, nil
   }
   ```
   Remove `SeriesCache` field and `filterNewSeries` call — series dedup
   is handled entirely by `ReplacingMergeTree` on the CH side.

4. Delete `internal/ingest/batcher.go` and `internal/ingest/cache.go`.

5. Delete `MetricsStore` interface from `service.go`. Delete all `Insert*`
   methods from `storage/client.go` (keep `CreateTables`, `Close`, `Ping`).

6. Update `cmd/main.go`:
   - Construct `Producer` from `cfg.Kafka`
   - Remove `Batcher`, `SeriesCache` construction
   - `defer producer.Close()` — flushes in-flight messages on shutdown
   - Remove `<-batcher.Done()` wait

7. Add `TestProducer` unit test with a mock Kafka writer:
   - `Publish` with gauge rows → correct topic, correct JSON format
   - `Publish` with empty rows → no message sent
   - `Close` flushes pending messages

### ✅ Done when
`go build ./...` passes. `make test` passes (unit tests only). Service
starts, receives a gRPC Export call, publishes to Kafka, returns 200. No
integration test changes yet.

---

## Phase 3 — Integration Tests

**Goal:** end-to-end tests proving gRPC → Kafka → ClickHouse flow. Adjust
existing tests to use Kafka path; add new tests for delivery guarantees.

### Tasks

1. Add Redpanda testcontainer to `integration_test/setup_test.go` alongside
   the existing ClickHouse container. Start both in `TestMain` via `sync.Once`.
   Pass broker address to both the service and ClickHouse schema.

2. Update `integration_test/helpers_test.go`:
   - `getServer(t)` removes `batcher` return value, returns `(client, closer)`
   - Add `waitForConsumption(t, store, table, expectedRows, timeout)` helper —
     polls CH until the expected row count appears (CH Kafka consumer is async,
     unlike the old `batcher.Flush(ctx)` explicit sync point)

3. Update existing tests:
   - Remove all `batcher.Flush(ctx)` calls — replace with `waitForConsumption`
   - `TestGRPCToClickHouse` — unchanged logic, new sync mechanism
   - `TestInsertGauge` / `TestInsertSum` — same
   - `TestReferentialIntegrity` — same, wait for both series and gauge tables

4. Add new tests:
   - `TestSeriesDedup_KafkaPath` — same series published 200 times →
     `ReplacingMergeTree` collapses to 1 row after `OPTIMIZE FINAL`
   - ~~`TestKafkaRedelivery`~~ — dropped; ClickHouse's Kafka engine consumer
     enters a long reconnection backoff after a broker restart, making
     stop/start tests impractical within reasonable CI timeframes

### ✅ Done when
`make test-integration` passes. All existing test assertions hold. New dedup
test passes.

---

## Phase 4 — README + Branch Cleanup

**Goal:** document the branch, cross-reference from main.

### Tasks

1. Update branch README — add a note at the top:
   ```
   > **Branch:** `feat/kafka-ingest`
   > This branch replaces the in-memory batcher with a Kafka-backed ingest
   > pipeline. See the main branch for the self-contained implementation
   > and the design rationale for choosing it for the assignment.
   ```

2. Update main branch README — add a line under Production next steps:
   ```
   The `feat/kafka-ingest` branch implements this architecture with
   Redpanda + ClickHouse Kafka engine, if you want to see the full
   production path.
   ```

3. Run `go vet ./...`, `make test`, `make test-integration`. Clean.

### ✅ Done when
Branch is pushed. Main branch README links to it. Both branches build and
test green independently.

---

## Decision Log

| Decision | Choice | Rationale |
|---|---|---|
| Kafka client | `segmentio/kafka-go` | Pure Go, no CGO (unlike confluent); good enough for producer-only use case |
| Serialization | `JSONEachRow` | Native CH Kafka engine format; `json.Marshal` of row structs works directly; no schema registry needed |
| Map columns | Keep `Map(String, String)` in queue tables | JSONEachRow handles JSON objects natively; no schema change needed |
| Redpanda over Kafka | Redpanda | No Zookeeper; single binary; Kafka-compatible API; faster startup in CI |
| Series dedup | Remove LRU cache | ReplacingMergeTree is the only dedup layer needed; cache was an optimisation for direct inserts that no longer apply |
| `waitForConsumption` polling | ~100ms interval, 10s timeout | CH Kafka consumer flushes every ~500ms by default; polling is simpler than injecting a sync point into the production path |
| Drop `TestKafkaRedelivery` | Removed | CH Kafka engine consumer enters multi-minute reconnection backoff after broker stop/start; impractical to test within CI timeframes |