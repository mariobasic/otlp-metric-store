# OTLP Metric Store

> **Branch: `feat/kafka-ingest`**
>
> This branch replaces the in-memory batcher with a Kafka-backed ingest pipeline.
> See the [main branch](../../tree/main) for the self-contained implementation
> (no dependencies beyond ClickHouse) and the design rationale for choosing it
> as the assignment submission.

A Go service that receives OTLP metric datapoints over gRPC and stores them in ClickHouse, with a shared metadata catalogue (`otel_metric_series`) referenced by `SeriesID` so the datapoints tables stay narrow and time-range queries never full-scan.

Submitted as the Dash0 take-home assignment.

---

## Quick start

**End-to-end demo** (requires Docker):

```shell
make demo-up       # start Redpanda + ClickHouse (waits until healthy) + service in background
make send-metrics  # send 40 Gauge datapoints via telemetrygen (repeat as needed)
make demo-down     # graceful shutdown — service + containers
```

**Manual workflow** (service in its own terminal):

```shell
make local-up   # start Redpanda + ClickHouse
make run        # start the service  (second terminal)
make send-metrics
make local-down
```

**Tests:**

```shell
make test               # unit tests (~1s, no Docker)
make test-integration   # E2E against real Redpanda + ClickHouse containers (~30s)
```

**Other:**

```shell
make build       # compile binary
make local-logs  # tail container logs
```

Service listens on:
- `localhost:4317` — OTLP/gRPC metrics endpoint
- `localhost:13133` — `GET /health` (200 if ClickHouse is reachable, 503 otherwise)

---

## Architecture

```
                ┌───────────────────────────────────────────────────────────┐
                │                         ingest                           │
gRPC Export ──► │  MapRows  ──►  Kafka Producer                            │
(OTLP)          │   │ valid       │ one message batch per topic            │
                │   ▼ ate         │ (series, gauge, sum, histogram,        │
                │  skipped        │  exponential_histogram, summary)       │
                │  counter        ▼                                        │
                │            return immediately                            │
                └───────────────────────────────────────────────────────────┘
                                  │
                                  ▼  JSONEachRow
                    ┌──────────────────────────┐
                    │      Redpanda (Kafka)     │
                    │  6 topics (otlp.*)        │
                    └──────────┬───────────────┘
                               ▼
                 ┌────────────────────────────────────┐
                 │            ClickHouse               │
                 │                                     │
                 │  Kafka engine queue tables (6)       │
                 │       │ Materialized Views (6)       │
                 │       ▼                              │
                 │  otel_metric_series  (catalogue)     │
                 │  otel_metrics_gauge  (slim)          │
                 │  otel_metrics_sum    (slim)          │
                 │  otel_metrics_histogram              │
                 │  otel_metrics_exponential_histogram   │
                 │  otel_metrics_summary                │
                 └────────────────────────────────────┘
```

**What changed from main branch:**
- `internal/ingest/batcher.go` — deleted (Kafka replaces the in-memory batcher)
- `internal/ingest/cache.go` — deleted (series dedup handled entirely by `ReplacingMergeTree`)
- `internal/storage/client.go` `Insert*` methods — deleted (ClickHouse pulls from Kafka)
- `MetricsStore` interface — deleted
- `Export()` — publishes to Kafka topics, returns immediately
- `internal/storage/schema.go` — adds 12 new DDL objects (6 queue tables + 6 MVs)
- `docker-compose.yml` — adds Redpanda (Kafka-compatible, no Zookeeper)

**What stayed the same:**
- `MapRows()` and all row structs
- `computeSeriesID()` fingerprinting
- All 6 destination MergeTree tables
- `cmd/main.go` signal handling and health endpoint
- Input validation and skip counters

Packages:

| Package | Role |
|---|---|
| `cmd/` | `main` — wiring; signal handling; gRPC + HTTP listeners; graceful shutdown. |
| `internal/config` | Env-backed `Config` struct (no magic literals in callers). |
| `internal/ingest` | OTLP → row mapping, fingerprinting, Kafka `Producer`, gRPC handler, instruments. |
| `internal/storage` | ClickHouse DDL (destination tables + Kafka queue tables + MVs). Pure infrastructure. |
| `integration_test` | testcontainers-driven E2E tests (Redpanda + ClickHouse) behind the `integration` build tag. |

---

## Schema

### `otel_metric_series` — shared metadata catalogue

```sql
CREATE TABLE otel_metric_series (
    SeriesID              UInt64,
    MetricType            LowCardinality(String),  -- Gauge / Sum / Histogram / ...
    ServiceName           LowCardinality(String),
    MetricName            LowCardinality(String),
    MetricDescription     String,
    MetricUnit            String,
    ResourceAttributes    Map(LowCardinality(String), String),
    ResourceSchemaUrl     String,
    ScopeName             LowCardinality(String),
    ScopeVersion          LowCardinality(String),
    ScopeAttributes       Map(LowCardinality(String), String),
    ScopeDroppedAttrCount UInt32,
    ScopeSchemaUrl        String,
    Attributes            Map(LowCardinality(String), String),  -- datapoint attrs
    FirstSeen             DateTime,
    LastSeen              DateTime,

    INDEX idx_service_name      ServiceName                  TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_metric_name       MetricName                   TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_key      mapKeys(ResourceAttributes)  TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value    mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key    mapKeys(ScopeAttributes)     TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value  mapValues(ScopeAttributes)   TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key          mapKeys(Attributes)          TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value        mapValues(Attributes)        TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree(LastSeen)
ORDER BY SeriesID;
```

**Why one shared catalogue, not five per-type catalogues:** the metadata is identical across Gauge / Sum / Histogram / ExponentialHistogram / Summary. The `MetricType` column distinguishes them and is part of the `SeriesID` hash, so a metric named `ops` with type Gauge and another named `ops` with type Sum get distinct rows.

**Why `ReplacingMergeTree(LastSeen)`:** repeat inserts of the same series are idempotent — ClickHouse collapses duplicates on `SeriesID` during background merges, keeping the row with the latest `LastSeen`. No locking on the write path.

**Why the bloom filters live here:** attributes no longer exist on the datapoints tables. Attribute-filter queries hit the catalogue first (`bloom -> SeriesIDs`), then fetch matching datapoints. Smaller payloads to scan, sharper indexes.

### Datapoints tables — slim, one per type

```sql
CREATE TABLE otel_metrics_gauge (
    SeriesID      UInt64,
    StartTimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TimeUnix      DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    Value         Float64,
    Flags         UInt32
) ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (SeriesID, toUnixTimestamp64Nano(TimeUnix));
```

Sum / Histogram / ExponentialHistogram / Summary follow the same shape with type-specific value columns appended.

**Why `ORDER BY (SeriesID, TimeUnix)`:** the natural shape for "give me this series over time" — all points for one series are physically adjacent, then ordered by time within that cluster.

**Why `PARTITION BY toDate(TimeUnix)`:** time-range queries (the assignment's stated mandatory filter) confine reads to a small set of partitions; no full-table scans.

**Why `Delta(8), ZSTD(1)` on timestamps:** monotonically increasing nanosecond timestamps compress phenomenally well under delta encoding plus ZSTD.

### Kafka queue tables + materialized views

Each of the 6 topics has a corresponding Kafka engine queue table and a materialized view that bridges rows into the destination MergeTree table:

```sql
CREATE TABLE otel_metrics_gauge_queue (
    -- same columns as otel_metrics_gauge
) ENGINE = Kafka(...)
  SETTINGS kafka_topic_list    = 'otlp.gauge',
           kafka_group_name    = 'clickhouse.otlp.gauge',
           kafka_format        = 'JSONEachRow',
           kafka_max_block_size = 65536;

CREATE MATERIALIZED VIEW otel_metrics_gauge_mv
    TO otel_metrics_gauge AS
SELECT * FROM otel_metrics_gauge_queue;
```

ClickHouse consumes from each topic as a Kafka consumer group and writes into the destination tables automatically. The Go service has no direct inserts — it publishes to Kafka and returns immediately.

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

Format: `JSONEachRow`. Map columns (`ResourceAttributes`, `ScopeAttributes`, `Attributes`) serialize naturally as JSON objects via `json.Marshal` — ClickHouse Kafka engine handles `Map(String, String)` with JSONEachRow natively.

---

## Fingerprinting

`SeriesID` is a `uint64` xxhash of the identifying fields. Same series always produces the same ID — no central sequence, no UUID generation, no shared state across instances.

```go
computeSeriesID(metricName, metricType,
                resourceAttrs, resourceSchemaURL,
                scopeName, scopeVersion,
                datapointAttrs) uint64
```

Implementation lives in `internal/ingest/fingerprint.go`.

**Why xxhash:** fast (single pass over bytes), good distribution, 64-bit fits `UInt64` natively. No crypto needed — this is identity, not signing.

**Why sorted map keys:** Go map iteration order is non-deterministic. Without sorting, two semantically identical attribute maps would produce different hashes between runs.

**Why null-byte separators between fields and between key/value pairs:** keeps boundaries unambiguous so `"ab"+"c"` doesn't collide with `"a"+"bc"` post-concatenation.

**Why both resource and datapoint attrs are in the hash with a section separator:** an attribute `{k:v}` on the resource is semantically different from `{k:v}` on the datapoint — they describe different things. The mapper proves this with `TestComputeSeriesID/resource_vs_datapoint_attrs_are_not_interchangeable`.

**Collision probability:** ~1 in 2^64 per hash. Same risk profile Prometheus TSDB has with its fingerprints — acceptable for this use case.

---

## Series deduplication

`ReplacingMergeTree(LastSeen)` is the sole dedup mechanism on this branch. Every `Export()` call publishes all series rows to Kafka — including series that have been seen before. ClickHouse deduplicates by `SeriesID` during background merges, keeping the row with the latest `LastSeen`.

The main branch uses an in-process LRU cache as a first layer to reduce duplicate writes. On the Kafka branch that cache is removed: the trade-off is more duplicate rows flowing through Kafka and into the queue table, but ClickHouse handles this natively and the LRU added complexity that doesn't pull its weight when the write path is already asynchronous.

Both paths are exercised in integration tests: `TestSeriesDedup_KafkaPath` (200 sends -> 1 catalogue row after `OPTIMIZE FINAL`).

### Horizontal scaling

With N instances, each publishes its own series rows to the same Kafka topic. `ReplacingMergeTree` collapses duplicates. Bounded, predictable, no shared infrastructure beyond Kafka (which is already required).

---

## Kafka producer

`internal/ingest/Producer` replaces the batcher from the main branch. One `kafka.Writer` per topic, created at startup.

```
Export()  →  MapRows()  →  Publish(topic, rows)  →  return immediately
```

`Publish` marshals each row struct to JSON and sends the batch as Kafka messages. Empty slices are no-ops. `Close()` flushes all in-flight messages — called on `SIGTERM`/`SIGINT` via `defer` in `main`.

**Why this replaces the batcher:** the main branch batcher existed to amortise ClickHouse inserts (size + ticker triggers). With Kafka in the path, ClickHouse pulls from topics at its own pace via the Kafka engine. The Go service's job is just to get rows into Kafka as fast as possible — no buffering, no flush timers, no batch-size tuning.

**Failure policy:** Kafka write errors propagate to the gRPC response (caller sees the failure). This is the correct at-least-once behaviour: the client retries. ClickHouse Kafka consumers handle dedup via `ReplacingMergeTree`.

---

## Observability

### OTel instruments

All declared in `internal/ingest/instruments.go` with the meter scope `dash0.com/otlp-metric-store`.

| Instrument | Type | Labels | Purpose |
|---|---|---|---|
| `com.dash0.homeexercise.metrics.received` | counter | — | ExportMetricsServiceRequests received |
| `com.dash0.homeexercise.datapoints.processed` | counter | `metric_type` | Datapoints accepted by the mapper |
| `com.dash0.homeexercise.datapoints.skipped` | counter | `reason` | Datapoints rejected during validation |

Stdout exporter by default (demonstrates instrumentation); swap to OTLP in production via the standard OTel collector env vars — no code change required.

### Structured logs

| Event | Level | Key fields |
|---|---|---|
| Datapoint skipped | `WARN` | `reason`, `metric_name` |
| Health: CH ping failed | `WARN` | `err` |
| Shutdown signal received | `INFO` | — |

### Health endpoint

`GET http://<HEALTH_LISTEN_ADDR>/health` -> `200 OK` if `ClickHouse.Ping()` succeeds (2s timeout), `503` otherwise. Separate HTTP listener from the gRPC port (OTel collector convention, also keeps liveness probes off the OTLP path).

---

## Input validation

The mapper rejects (counts as `datapoints.skipped{reason=...}` and emits a `WARN` log) on:

| Reason | What it catches |
|---|---|
| `empty_metric_name` | `Metric.Name == ""` — drops the entire metric. |
| `zero_timestamp` | `dp.TimeUnixNano == 0` — drops that datapoint. |
| `invalid_value` | NaN or Inf on `NumberDataPoint.Value` (Gauge / Sum) or `Histogram/Exp/Summary.Sum`. |

Spec-invalid empty attribute keys are silently stripped rather than rejected — recoverable, never crashes the pipeline.

---

## Configuration

All from environment variables; sensible defaults if unset.

| Var | Default | Component |
|---|---|---|
| `CLICKHOUSE_ADDR` | `localhost:9000` | storage |
| `CLICKHOUSE_DATABASE` | `default` | storage |
| `CLICKHOUSE_USERNAME` | `default` | storage |
| `CLICKHOUSE_PASSWORD` | `""` | storage |
| `GRPC_LISTEN_ADDR` | `localhost:4317` | ingest |
| `GRPC_MAX_RECEIVE_BYTES` | `16777216` (16 MiB) | ingest |
| `HEALTH_LISTEN_ADDR` | `localhost:13133` | ops |
| `KAFKA_BROKERS` | `localhost:9092` | kafka (Go producer) |
| `KAFKA_CH_BROKERS` | `redpanda:29092` | kafka (CH engine) |
| `KAFKA_TOPIC_PREFIX` | `otlp` | kafka |

---

## Design considerations

The plan considered alternatives and explicitly rejected them; what's documented here is the *why we chose what we chose*, not pretend uncertainty.

### In-memory batcher vs Kafka

| Option | Why not (chosen / not chosen) |
|---|---|
| **Kafka (chosen — this branch)** | At-least-once delivery, independent scaling of ingest and storage, removes all batching complexity from the Go service. Trade-off: an extra infrastructure dependency (Redpanda). |
| **In-memory batcher (main branch)** | Zero dependencies beyond ClickHouse. At-most-once: data loss on hard kill. Good enough for a self-contained assignment submission; not production-grade. |

### Series deduplication — write path

| Option | Why not (chosen / not chosen) |
|---|---|
| **ReplacingMergeTree only (chosen — this branch)** | Simplest. Correctness is guaranteed by ClickHouse. More duplicate rows flow through Kafka, but the volume is bounded and CH handles it natively. |
| **Local LRU cache + ReplacingMergeTree (main branch)** | Reduces duplicate writes when the service does direct inserts. Unnecessary when Kafka decouples the write path — the async consumer means the cache doesn't reduce any meaningful load. |
| **Redis distributed cache** | Global cross-instance dedup, but adds Redis as a hard dependency. `ReplacingMergeTree` already guarantees correctness — not worth the dependency. |

### Read path enrichment — joining datapoints to metadata

| Option | Why not (chosen / not chosen) |
|---|---|
| **Regular JOIN (chosen)** | Simplest. ClickHouse executes a hash join at query time. No extra DDL, no extra objects to manage. Correct for the assignment scope. |
| **ClickHouse Dictionary** | In-memory, Direct Join, `dictGet`. Best production option. Worth adding at scale. |

---

## Decision log

| Decision | Choice | Rationale |
|---|---|---|
| Kafka client | `segmentio/kafka-go` | Pure Go, no CGO (unlike confluent); good enough for producer-only use case |
| Serialization | `JSONEachRow` | Native CH Kafka engine format; `json.Marshal` of row structs works directly; no schema registry needed |
| Map columns | Keep `Map(String, String)` in queue tables | JSONEachRow handles JSON objects natively; no schema change needed |
| Redpanda over Kafka | Redpanda | No Zookeeper; single binary; Kafka-compatible API; faster startup in CI |
| Series dedup | `ReplacingMergeTree` only | Cache was an optimisation for direct inserts that doesn't apply with Kafka in the path |
| Failure handling | Propagate to gRPC caller | At-least-once: client retries on failure; CH deduplicates |
| OTel exporter | stdout | Demonstrates instrumentation; exporter swap is an ops concern (env var) |
| Mapper API | `MapRows(ctx, rm) MappedRows` | Single OTLP traversal; adding a new metric type = adding a field, no signature change |
| Directory structure | `cmd/` + `internal/{config,storage,ingest}/` + `integration_test/` | Avoids flat-package sprawl; `internal/` enforces Go visibility; integration tests as a peer directory behind a build tag |
| Integration test isolation | Separate directory + `//go:build integration` tag | `go test ./...` skips integration by default; `-tags integration` includes them. Containers shared across the package via `sync.Once`; tests truncate between runs |
| Config | Env-var-backed, no external library | Eliminates magic literals; behaviour configurable without recompile; no extra dep |

---

## Production next steps

Roughly in order of value:

1. **OTLP exporter for self-instrumentation.** Stdout is good for demos; OTLP to a real backend (e.g. Dash0) lets the metrics actually drive alerts.
2. **Read-path Dictionary.** When query latency on the join becomes the bottleneck, replace the regular JOIN with a Dictionary keyed on `SeriesID`, refreshed on a `LIFETIME` schedule.
3. **Kafka producer instrumentation.** Add histograms for publish latency and batch size per topic, plus error counters. The Kafka client exposes stats that can be wired into OTel instruments.
4. **Consumer lag monitoring.** Track ClickHouse's Kafka consumer offset lag. Alert when the gap grows — the data is being ingested but not yet queryable.
5. **Authentication on the gRPC + health endpoints.** Currently `insecure.NewCredentials()` — fine for a sealed network, not fine for anything else.
6. **Dead-letter topic.** Rows that fail ClickHouse insertion (schema mismatch, corrupt JSON) currently cause the Kafka consumer to retry indefinitely. A DLT would isolate poison pills.

---

## Development approach

AI assistance is allowed and expected per the assignment brief. This project was planned and built collaboratively with Claude (Anthropic).

The full execution plan is in [`docs/execution-plan.md`](docs/execution-plan.md). It covers the phased task breakdown, pre-decisions locked in before each phase, execution notes documenting where reality diverged from the plan and why, and the decision log that informed the schema design, batcher architecture, and trade-off choices.

The Kafka ingest plan is in [`docs/kafka-plan.md`](docs/kafka-plan.md).

---

## References

- [OpenTelemetry Metrics](https://opentelemetry.io/docs/concepts/signals/metrics/)
- [OpenTelemetry Protocol (OTLP)](https://github.com/open-telemetry/opentelemetry-proto)
- [ClickHouse `ReplacingMergeTree`](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree)
- [ClickHouse Kafka Engine](https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka)
- [`segmentio/kafka-go`](https://github.com/segmentio/kafka-go) — Kafka producer client
- [`cespare/xxhash`](https://github.com/cespare/xxhash) — fingerprint hash
- [Redpanda](https://redpanda.com/) — Kafka-compatible event streaming