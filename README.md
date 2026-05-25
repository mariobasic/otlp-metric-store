# OTLP Metric Store

A Go service that receives OTLP metric datapoints over gRPC and stores them in ClickHouse, with a shared metadata catalogue (`otel_metric_series`) referenced by `SeriesID` so the datapoints tables stay narrow and time-range queries never full-scan.

Submitted as the Dash0 take-home assignment.

---

## Quick start

**End-to-end demo** (requires Docker):

```shell
make demo-up       # start ClickHouse (waits until healthy) + service in background
make send-metrics  # send 40 Gauge datapoints via telemetrygen (repeat as needed)
make demo-down     # graceful shutdown — service + ClickHouse
```

**Manual workflow** (service in its own terminal):

```shell
make local-up   # start ClickHouse
make run        # start the service  (second terminal)
make send-metrics
make local-down
```

**Tests:**

```shell
make test               # unit tests (~1s, no Docker)
make test-integration   # E2E against a real ClickHouse 26.2 container (~20s)
```

**Other:**

```shell
make build       # compile binary
make local-logs  # tail ClickHouse container logs
```

Service listens on:
- `localhost:4317` — OTLP/gRPC metrics endpoint
- `localhost:13133` — `GET /health` (200 if ClickHouse is reachable, 503 otherwise)

---

## Architecture

```
                ┌────────────────────────────────────────────────────────────┐
                │                          ingest                            │
gRPC Export ──► │  MapRows  ──►  filterNewSeries(cache)  ──►  Batcher.Add*   │
(OTLP)          │   │ valid          │ skip if already                │      │
                │   ▼ ate            │ in LRU cache                   │      │
                │  skipped           │                                ▼      │
                │  counter           ▼                       typed buffers   │
                │                 cache hit/                 (size + ticker  │
                │                 miss counter                triggers)     │
                └────────────────────────────────────────────────────────────┘
                                                                     │
                                                                     ▼  series first
                                                          ┌──────────────────────┐
                                                          │       storage        │
                                                          │  ClickHouseMetrics-  │
                                                          │       Store          │
                                                          └──────────┬───────────┘
                                                                     ▼
                                              ┌────────────────────────────────────┐
                                              │            ClickHouse              │
                                              │                                    │
                                              │   otel_metric_series  (catalogue)  │
                                              │   otel_metrics_gauge  (slim)       │
                                              │   otel_metrics_sum    (slim)       │
                                              │   otel_metrics_histogram           │
                                              │   otel_metrics_exponential_…       │
                                              │   otel_metrics_summary             │
                                              └────────────────────────────────────┘
```

Packages:

| Package | Role |
|---|---|
| `cmd/` | `main` — wiring; signal handling; gRPC + HTTP listeners; graceful shutdown. |
| `internal/config` | Env-backed `Config` struct (no magic literals in callers). |
| `internal/ingest` | OTLP → row mapping, fingerprinting, `SeriesCache`, `Batcher`, gRPC handler, instruments. |
| `internal/storage` | ClickHouse DDL + batch-insert primitives. Pure infrastructure. |
| `integration_test` | testcontainers-driven E2E tests behind the `integration` build tag. |

The `MetricsStore` interface lives in `internal/ingest/service.go` (the consumer), not in `storage/`. `storage.ClickHouseMetricsStore` satisfies it by structural typing — no import cycle, and the boundary belongs to the package that uses it.

---

## Schema

### `otel_metric_series` — shared metadata catalogue

```sql
CREATE TABLE otel_metric_series (
    SeriesID              UInt64,
    MetricType            LowCardinality(String),  -- Gauge / Sum / Histogram / …
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

**Why `ReplacingMergeTree(LastSeen)`:** repeat inserts of the same series are idempotent — ClickHouse collapses duplicates on `SeriesID` during background merges, keeping the row with the latest `LastSeen`. No locking on the write path; the in-process `SeriesCache` is an optimisation, not a correctness requirement.

**Why the bloom filters live here:** attributes no longer exist on the datapoints tables. Attribute-filter queries hit the catalogue first (`bloom → SeriesIDs`), then fetch matching datapoints. Smaller payloads to scan, sharper indexes.

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

Two layers, defence in depth:

1. **In-process `SeriesCache` (LRU, default 100k entries)** — `MarkIfNew` is a single atomic `lru.ContainsOrAdd`. Returns `true` the first time a SeriesID is seen, `false` thereafter. The `filterNewSeries` step in `Export()` drops already-cached series before they reach the batcher. Effect: under steady-state traffic, the lookup table receives ~zero writes.

2. **`ReplacingMergeTree(LastSeen)` in ClickHouse** — catches anything the cache misses (evicted entries, cross-instance duplicates, restarts before warm-up). Background merges collapse duplicates by `SeriesID`, keeping the row with the latest `LastSeen`.

Both paths are exercised in integration tests: `TestSeriesDedup_GRPCPath` (200 sends → 1 catalogue row via the cache) and `TestSeriesDedup_ReplacingMergeTree` (10 direct inserts + `OPTIMIZE FINAL` → 1 row).

### Horizontal scaling

With N instances each holding their own LRU, the worst case is one duplicate catalogue insert per (instance × series) on first encounter. `ReplacingMergeTree` collapses them. Bounded, predictable, no shared infrastructure. See _Design considerations_ below for the Redis / ClickHouse-Dictionary alternatives we considered and didn't take.

---

## Batching

`internal/ingest/Batcher` sits between the gRPC handler and the store. Six typed buffers (one per output table), one mutex, a single ticker goroutine.

```
Add* → append under lock → if any buffer >= maxSize → Flush()
                                                         │
                                                         ▼
                                  snapshot+reset under lock, then
                                  send each table outside the lock
                                  (series → gauge → sum → histogram → exp → summary)
```

Flush triggers:
- **Size:** any buffer reaches `BATCHER_MAX_SIZE` (default 10 000).
- **Ticker:** `BATCHER_FLUSH_EVERY` (default 1s).
- **Shutdown:** `SIGTERM/SIGINT` → ctx cancel → final drain with `context.Background()` (so the final flush isn't itself cancelled).

Series rows always go to CH before their datapoints — preserves the catalogue → datapoint ordering at query time. Datapoints without a series are queryable-but-invisible to enrichment joins; series without datapoints are harmless.

`Flush(ctx)` is exposed publicly so tests and graceful-shutdown paths can drain on demand.

**Failure policy:** log-and-drop on insert failure (increment `chInsertErrors{table=…}`, structured log with row count + duration). A retry queue is the natural next step — documented under _Production next steps_.

---

## Observability

### OTel instruments

All declared in `internal/ingest/instruments.go` with the meter scope `dash0.com/otlp-metric-store`.

| Instrument | Type | Labels | Purpose |
|---|---|---|---|
| `com.dash0.homeexercise.metrics.received` | counter | — | ExportMetricsServiceRequests received |
| `com.dash0.homeexercise.datapoints.processed` | counter | `metric_type` | Datapoints accepted by the mapper |
| `com.dash0.homeexercise.datapoints.skipped` | counter | `reason` | Datapoints rejected during validation |
| `com.dash0.homeexercise.series_cache.hits` | counter | — | SeriesIDs already cached (no catalogue write) |
| `com.dash0.homeexercise.series_cache.misses` | counter | — | SeriesIDs newly enqueued |
| `com.dash0.homeexercise.clickhouse.insert_errors` | counter | `table` | Failed batch inserts |
| `com.dash0.homeexercise.batcher.flush_duration` | histogram (ms) | `table` | Wall-clock duration of each batch insert |
| `com.dash0.homeexercise.batcher.batch_size` | histogram (rows) | `table` | Rows per flushed batch |

Stdout exporter by default (demonstrates instrumentation); swap to OTLP in production via the standard OTel collector env vars — no code change required.

### Structured logs

| Event | Level | Key fields |
|---|---|---|
| Datapoint skipped | `WARN` | `reason`, `metric_name` |
| Batch flushed | `DEBUG` | `table`, `rows`, `duration_ms` |
| Insert failed | `ERROR` | `table`, `rows_dropped`, `duration_ms`, `err` |
| Health: CH ping failed | `WARN` | `err` |
| Shutdown signal received | `INFO` | — |

### Health endpoint

`GET http://<HEALTH_LISTEN_ADDR>/health` → `200 OK` if `ClickHouse.Ping()` succeeds (2s timeout), `503` otherwise. Separate HTTP listener from the gRPC port (OTel collector convention, also keeps liveness probes off the OTLP path).

---

## Input validation

The mapper rejects (counts as `datapoints.skipped{reason=…}` and emits a `WARN` log) on:

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
| `BATCHER_MAX_SIZE` | `10000` | batcher |
| `BATCHER_FLUSH_EVERY` | `1s` | batcher |
| `SERIES_CACHE_SIZE` | `100000` | cache |

---

## Design considerations

The plan considered alternatives and explicitly rejected them; what's documented here is the *why we chose what we chose*, not pretend uncertainty.

### Series deduplication — write path

| Option | Why not (chosen / not chosen) |
|---|---|
| **Local LRU cache (chosen)** | Zero dependencies, nanosecond lookups, no network calls on the hot path. Per-instance only — cross-instance duplicates handled by `ReplacingMergeTree`. Cache is empty on restart; a few duplicate inserts until warm-up, all collapsed by background merge. |
| **Redis distributed cache** | Global cross-instance dedup, but adds Redis as a hard dependency (HA, latency, failure modes). `ReplacingMergeTree` already guarantees correctness — the cache is only an optimisation. Not worth the dependency. |
| **ClickHouse Dictionary** | CH-native, no extra dep. Adds a network round-trip on the hot insert path (~1ms vs nanosecond map lookup). Still only eventually consistent between refreshes — `ReplacingMergeTree` is still the real safety net. Adds complexity without improving correctness. |
| **No cache** | Simplest. Correctness is fine — `ReplacingMergeTree` deduplicates. Would work if series cardinality were so low that duplicate inserts were negligible; LRU is the lightweight upgrade that prevents the lookup table from being hammered. |

### Read path enrichment — joining datapoints to metadata

| Option | Why not (chosen / not chosen) |
|---|---|
| **Regular JOIN (chosen)** | Simplest. ClickHouse executes a hash join at query time. No extra DDL, no extra objects to manage. Correct for the assignment scope. Optimise if query latency becomes a bottleneck at scale. |
| **ClickHouse Dictionary** | In-memory, Direct Join, `dictGet`. Requires periodic full reload — enrichment queries can return stale metadata for up to the refresh interval. Best production option for self-managed deployments. Worth adding at scale. |
| **Join table engine** | Same Direct Join performance, always fresh (UPSERT). On self-managed OSS ClickHouse: not distributed (each node maintains its own copy), and no background compaction (each INSERT adds a `.bin` file that never merges). Cloud has it solved; OSS doesn't. Dictionary wins on self-managed CH. |

---

## Decision log

| Decision | Choice | Rationale |
|---|---|---|
| Flush failure handling | Log and drop | Simple; error counter pages on-call; retry documented as next step |
| OTel exporter | stdout | Demonstrates instrumentation; exporter swap is an ops concern (env var) |
| `SeriesCache` impl | hashicorp/golang-lru v2, atomic `ContainsOrAdd` | Bounded, thread-safe, no reinvention; evicted entries handled by `ReplacingMergeTree` |
| `MetricType` in `SeriesRow` | Keep | Required for correct `SeriesID` hash; makes the catalogue a complete metric registry |
| Batcher design | Typed buffers, single ticker, snapshot+reset under lock then send unlocked | Reads/Adds keep buffering during the in-flight insert; series-first ordering preserved |
| Mapper API | `MapRows(ctx, rm) MappedRows` | Single OTLP traversal; adding a new metric type = adding a field, no signature change |
| Directory structure | `cmd/` + `internal/{config,storage,ingest}/` + `integration_test/` | Avoids flat-package sprawl; `internal/` enforces Go visibility; integration tests as a peer directory behind a build tag |
| `MetricsStore` interface location | `internal/ingest/service.go` | Go idiom: interface belongs to the package that uses it. No `ingest → storage` coupling for the interface, only for the row types. |
| Integration test isolation | Separate directory + `//go:build integration` tag | `go test ./...` skips integration by default; `-tags integration` includes them. Container shared across the package via `sync.Once`; tests truncate between runs. |
| Config | Env-var-backed, no external library | Eliminates magic literals; behaviour configurable without recompile; no extra dep |

---

## Production next steps

Roughly in order of value:

1. **Retry queue on flush failure.** Today flush errors log + increment `chInsertErrors` + drop. A bounded retry queue with exponential backoff would survive transient CH unavailability without losing data.
2. **OTLP exporter for self-instrumentation.** Stdout is good for demos; OTLP to a real backend (e.g. Dash0) lets the metrics in the _Observability_ table actually drive alerts.
3. **Read-path Dictionary.** When query latency on the join becomes the bottleneck, replace the regular JOIN with a Dictionary keyed on `SeriesID`, refreshed on a `LIFETIME` schedule.
4. **Per-metric-type batcher goroutines (Phase 2 of the batcher design).** Today's batcher uses one mutex over six buffers; a high-throughput type can briefly hold up another. Splitting into one goroutine per type with a channel removes the contention.
5. **Authentication on the gRPC + health endpoints.** Currently `insecure.NewCredentials()` — fine for a sealed network, not fine for anything else.
6. **Tracing through the batcher.** Currently `Export()` accepts a ctx and uses it for the in-request validation log lines and cache counters, but the batcher's flush uses a long-lived background ctx so traces from request-time don't propagate to the actual CH insert. A span link from the request span to the batch flush span would close that gap.

---

## Development approach

AI assistance is allowed and expected per the assignment brief. This project was planned and built collaboratively with Claude (Anthropic).

The full execution plan is in [`docs/execution-plan.md`](docs/execution-plan.md). It covers the phased task breakdown, pre-decisions locked in before each phase, execution notes documenting where reality diverged from the plan and why, and the decision log that informed the schema design, batcher architecture, and trade-off choices.

---

## References

- [OpenTelemetry Metrics](https://opentelemetry.io/docs/concepts/signals/metrics/)
- [OpenTelemetry Protocol (OTLP)](https://github.com/open-telemetry/opentelemetry-proto)
- [ClickHouse `ReplacingMergeTree`](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree)
- [`hashicorp/golang-lru`](https://github.com/hashicorp/golang-lru) — the LRU backing `SeriesCache`
- [`cespare/xxhash`](https://github.com/cespare/xxhash) — fingerprint hash