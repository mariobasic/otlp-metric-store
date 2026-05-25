# Dash0 Execution Plan

Phased implementation of the OTLP metric store assignment. Each phase ends with a passing test suite and a working system. Complexity increases only after the previous phase is verified.

The companion design document (not committed) covers the original schema design, fingerprinting rationale, batcher internals, and design consideration tables in full. The README summarises the key decisions.

---

## Phase 1 — Restructure + Config + SeriesID + Schema

**Goal:** establish directory layout, eliminate magic literals, correct fingerprinting, updated DDL. No Go insert logic yet.

### Tasks

1. **Restructure directories** — create `cmd/`, `internal/config/`, `internal/storage/`, `internal/ingest/`, `integration_test/`; move existing files:
   - `server.go` → `cmd/main.go` (thin main only)
   - `otel.go` → `cmd/otel.go`
   - `clickhouse_schema.go` → `internal/storage/schema.go`
   - `clickhouse_client.go` → `internal/storage/client.go`
   - `metrics_mapper.go` → `internal/ingest/mapper.go`
   - `metrics_service.go` → `internal/ingest/service.go`
   - `server_test.go` → `internal/ingest/service_test.go`
   - `integration_test.go` → `integration_test/metrics_test.go`
   - Verify `go build ./...` passes before touching any logic.

2. **`internal/config/config.go`** — `Config` struct with `ClickHouseConfig`, `GRPCConfig`, `BatcherConfig`, `CacheConfig`; all values from env with defaults:
   - `CLICKHOUSE_ADDR` (default `localhost:9000`)
   - `GRPC_LISTEN_ADDR` (default `localhost:4317`)
   - `GRPC_MAX_RECEIVE_BYTES` (default `16_777_216`)
   - `BATCHER_MAX_SIZE` (default `10_000`)
   - `BATCHER_FLUSH_EVERY` (default `1s`)
   - `SERIES_CACHE_SIZE` (default `100_000`)
   - Thread `cfg := config.Load()` through `cmd/main.go`; remove all remaining magic literals.

3. Add `github.com/hashicorp/golang-lru/v2` to `go.mod` (`xxhash/v2` is already an indirect dep)

4. Create `internal/ingest/fingerprint.go`:
   - `computeSeriesID(metricName, metricType, resourceAttrs, resourceSchemaURL, scopeName, scopeVersion, dpAttrs) uint64`
   - `sortedKeys(m map[string]string) []string`

5. Add `TestComputeSeriesID` to `internal/ingest/service_test.go`:
   - Same inputs → same ID (determinism)
   - Different MetricName → different ID
   - Different MetricType → different ID (Gauge vs Sum with same name must differ)
   - Different ResourceAttributes → different ID
   - Attribute map order doesn't matter (`{a:1,b:2}` == `{b:2,a:1}`)
   - Empty attributes → valid non-zero ID

6. Update `internal/storage/schema.go`:
   - Add `createSeriesTableSQL` — `otel_metric_series` with `ReplacingMergeTree(LastSeen)`, bloom filters on attribute maps, `ORDER BY SeriesID`
   - Modify all 5 datapoints tables — drop all metadata columns, add `SeriesID UInt64`, change `ORDER BY` to `(SeriesID, toUnixTimestamp64Nano(TimeUnix))`
   - Update `CreateTables()` to execute series DDL first

### ✅ Done when
`go build ./...` + `go test ./...` passes — `TestComputeSeriesID` + `TestCreateTables` (updated for new schema). No magic literals in non-config files.

> **Phase 1 execution notes (delta from plan):**
> - **Build tag retained.** Kept `//go:build integration` on `integration_test/metrics_test.go`. The plan's "directory is the isolation" claim is wrong — `go test ./...` walks every package regardless of directory. The build tag is the only way `make test` skips them while `make test-integration` runs them. Decision Log entry below is corrected.
> - **`SeriesCacheConfig` (not `CacheConfig`).** Renamed for clarity; downstream phases should reference `cfg.SeriesCache.Size`.
> - **Extra ClickHouse env vars.** `CLICKHOUSE_DATABASE`, `CLICKHOUSE_USERNAME`, `CLICKHOUSE_PASSWORD` added because `storage.NewClickHouseMetricsStore` takes all four.
> - **Scope-attr indexes on series table.** `idx_scope_attr_key/value` added (plan only listed resource + attr indexes). Necessary since scope attrs now live only on series table.
> - **`golang-lru/v2` tidied out.** Added then removed by `go mod tidy` (no importer yet). Phase 3 must `go get` it again.
> - **`metricsReceivedCounter` + `meter` moved into `internal/ingest/service.go`.** Plan called for this in Phase 5; happened in Phase 1 as a side effect of the package split. Phase 5 just adds more instruments alongside.
> - **`MetricsStore` interface already declared** in `internal/ingest/service.go` with the OLD shape (`InsertGauge(...storage.GaugeRow)`). Phase 2 must REPLACE it, not "define" it.
> - **`TestCreateTables` not yet updated.** Lives in `integration_test/` and was deferred per "unit tests only in Phase 1" policy. Phase 3 step 5 will update assertions (add `otel_metric_series` to expected tables, drop the metadata-column checks).

---

## Phase 2 — Data Model: Row Structs + Mapper

**Goal:** typed row structs for all 5 metric types and a single-pass OTLP mapper. Pure Go — no ClickHouse calls.

> **Coordination note:** Steps 1 + 2 must land together. Step 1 deletes `storage.GaugeRow`/`SumRow` (currently in `client.go`) to make room for new typed rows in `rows.go`. Step 2 rewrites `mapper.go` to produce them. Doing 1 alone breaks the build (mapper still references old structs).

### Tasks

1. Create `internal/storage/rows.go` — all row structs (also: delete the old wide `GaugeRow`/`SumRow` from `client.go`, and remove the now-dead field references from `InsertGauge`/`InsertSum` — full Insert rewrite happens in Phase 3, but at least make them compile against the new types):

```go
type SeriesRow struct {
    SeriesID              uint64
    MetricType            string
    ServiceName           string
    MetricName            string
    MetricDescription     string
    MetricUnit            string
    ResourceAttributes    map[string]string
    ResourceSchemaUrl     string
    ScopeName             string
    ScopeVersion          string
    ScopeAttributes       map[string]string
    ScopeDroppedAttrCount uint32
    ScopeSchemaUrl        string
    Attributes            map[string]string
    FirstSeen             time.Time
    LastSeen              time.Time
}

type BaseDatapointRow struct {
    SeriesID      uint64
    StartTimeUnix time.Time
    TimeUnix      time.Time
    Flags         uint32
}

type GaugeDatapointRow struct {
    BaseDatapointRow
    Value float64
}

type SumDatapointRow struct {
    GaugeDatapointRow               // embeds BaseDatapointRow + Value
    AggregationTemporality int32
    IsMonotonic            bool
}

type HistogramDatapointRow struct {
    BaseDatapointRow
    Count                  uint64
    Sum                    float64
    Min                    float64
    Max                    float64
    BucketCounts           []uint64
    ExplicitBounds         []float64
    AggregationTemporality int32
}

type ExponentialHistogramDatapointRow struct {
    BaseDatapointRow
    Count                  uint64
    Sum                    float64
    Scale                  int32
    ZeroCount              uint64
    PositiveOffset         int32
    PositiveBucketCounts   []uint64
    NegativeOffset         int32
    NegativeBucketCounts   []uint64
    Min                    float64
    Max                    float64
    AggregationTemporality int32
}

type SummaryDatapointRow struct {
    BaseDatapointRow
    Count             uint64
    Sum               float64
    ValueAtQuantiles  []SummaryQuantile
}

type SummaryQuantile struct {
    Quantile float64
    Value    float64
}
```

2. Rewrite `internal/ingest/mapper.go` — replace `MapGaugeRows`/`MapSumRows` with:

```go
type MappedRows struct {
    Series               []SeriesRow
    Gauge                []GaugeDatapointRow
    Sum                  []SumDatapointRow
    Histogram            []HistogramDatapointRow
    ExponentialHistogram []ExponentialHistogramDatapointRow
    Summary              []SummaryDatapointRow
}

func MapRows(rm []*metricspb.ResourceMetrics) MappedRows
```

Single traversal of the OTLP tree. SeriesID computed inline. All 5 metric types handled.

3. **Replace** the existing `MetricsStore` interface in `internal/ingest/service.go` (added in Phase 1 with the old shape). New shape extends to all 5 datapoint types + series:
```go
type MetricsStore interface {
    InsertSeries(ctx context.Context, rows []storage.SeriesRow) error
    InsertGauge(ctx context.Context, rows []storage.GaugeDatapointRow) error
    InsertSum(ctx context.Context, rows []storage.SumDatapointRow) error
    InsertHistogram(ctx context.Context, rows []storage.HistogramDatapointRow) error
    InsertExponentialHistogram(ctx context.Context, rows []storage.ExponentialHistogramDatapointRow) error
    InsertSummary(ctx context.Context, rows []storage.SummaryDatapointRow) error
}
```
`storage.ClickHouseMetricsStore` satisfies this via Go structural typing — no explicit declaration needed. Note `Export()` in `service.go` currently calls `InsertGauge`/`InsertSum` directly with the old row types — that call site is rewritten in Phase 3 step 3.

### ✅ Done when
Unit tests on `MapRows` pass — verify correct SeriesID computation, correct struct population. No CH dependency needed.

> **Phase 2 execution notes (delta from plan):**
> - **Did more than planned: slim `InsertGauge`/`InsertSum` already rewritten** in `internal/storage/client.go` to match the new DDL column order. Plan had this under Phase 3 step 2.
> - **Did more than planned: `Export()` already rewritten** in `internal/ingest/service.go` to call `MapRows` and the full 6-method `MetricsStore`. Series-first ordering in place. Plan had this under Phase 3 step 3.
> - **Stubs for Series/Histogram/ExponentialHistogram/Summary `Insert*`** added to `storage/client.go`. They return `errNotImplemented` on non-empty input (no-op on empty slice). Phase 3 replaces them with real batch-inserts.
> - **Net effect on Phase 3 scope:** SeriesCache + cache filter in Export + real Insert impls + cmd/main.go CH wiring + integration test refactor. The interface and dispatch shape are stable.

---

## Phase 3 — Direct Inserts: E2E Data Flow

**Goal:** full data flow from gRPC → ClickHouse with series deduplication. No batcher yet — synchronous insert per Export call.

> **Pre-step:** `go get github.com/hashicorp/golang-lru/v2` — was tidied out at end of Phase 1 since nothing imported it. The `cache.go` step below brings it back as a direct require.
> **Config reference:** use `cfg.SeriesCache.Size`, not `cfg.Cache.Size` (renamed in Phase 1).
> **Wire CH connection in `cmd/main.go`:** Phase 1 left `ingest.NewServer(addr, nil)`. Phase 3 must construct `storage.NewClickHouseMetricsStore(ctx, cfg.ClickHouse.Addr, cfg.ClickHouse.Database, cfg.ClickHouse.Username, cfg.ClickHouse.Password)`, defer `Close`, call `CreateTables`, then pass into `NewServer`.

### Tasks

1. Create `internal/ingest/cache.go`:

```go
import lru "github.com/hashicorp/golang-lru/v2"

// SeriesCache deduplicates series inserts within a single instance.
// Bounded by size (default 100_000) — evicted entries trigger a harmless
// duplicate insert handled by ReplacingMergeTree.
// For cross-instance deduplication, see design considerations in README.
type SeriesCache struct {
    cache *lru.Cache[uint64, struct{}]
}

func NewSeriesCache(size int) (*SeriesCache, error)
func (c *SeriesCache) IsNew(id uint64) bool    // uses Contains — does not update recency
func (c *SeriesCache) MarkSeen(id uint64)
```

2. Replace the `errNotImplemented` stubs in `internal/storage/client.go` with real batch-insert implementations for `InsertSeries`, `InsertHistogram`, `InsertExponentialHistogram`, `InsertSummary`. Column order must match the DDL in `schema.go`. (Slim `InsertGauge`/`InsertSum` impls already done in P2.)

3. Add `SeriesCache` to `dash0MetricsServiceServer` (struct field + constructor parameter) and wire the cache filter into `Export()` before the existing `InsertSeries` call:

```go
// In service.go — Export already calls MapRows + 6-method dispatch (done in P2).
// Phase 3 inserts the cache filter between MapRows and InsertSeries:
rows := MapRows(request.GetResourceMetrics())

newSeries := filterNewSeries(rows.Series, m.cache)  // NEW in P3
if len(newSeries) > 0 {
    m.store.InsertSeries(ctx, newSeries)            // Was: InsertSeries(ctx, rows.Series)
}
// Gauge/Sum/Histogram/ExpHist/Summary dispatch already wired in P2.
```

`NewServer` signature gains a `*SeriesCache` parameter. `cmd/main.go` constructs it via `ingest.NewSeriesCache(cfg.SeriesCache.Size)` and threads it in.

4. **Refactor** the existing `integration_test/metrics_test.go` (Phase 1 migrated it as one file) into three:
   - `setup_test.go` — `TestMain` + ClickHouse container via testcontainers-go; single container shared across all tests via `sync.Once`
   - `helpers_test.go` — `getStore(t)` and `getServer(t)` lazy-init accessors
   - `metrics_test.go` — just the tests; setup/helpers extracted

5. Update existing tests + add new ones:
   - `TestCreateTables` — add `otel_metric_series` to the expected-tables list
   - `TestInsertGauge` — update assertions: SeriesID present, metadata columns gone (query series table to fetch metadata)
   - `TestInsertSum` — same
   - `TestGRPCToClickHouse` — update for new schema
   - `TestInsertSeries` — insert series row, query back, verify all fields
   - `TestReferentialIntegrity` — 10 datapoints for 2 series → lookup table 2 rows, datapoints table 10 rows, all SeriesIDs match
   - `TestSeriesDedup` — same series sent 1000 times → lookup table has 1 row (after `OPTIMIZE TABLE ... FINAL` or by waiting for merge)

Run with `make test-integration` (preserves Phase 1 decision to keep `//go:build integration` tag — see Decision Log correction).

### ✅ Done when
All integration tests pass — full E2E data flow verified, series deduplication correct.

> **Phase 3 execution notes (delta from plan):**
> - **SeriesCache API trimmed.** Single atomic `MarkIfNew(id) bool` via `lru.ContainsOrAdd`, instead of plan's `IsNew + MarkSeen` (two locks). One method, one lock per call.
> - **`filterNewSeries` is nil-safe.** A nil cache returns input unchanged — lets the no-store test path and the eventual P4 batcher skip cache wiring without conditionals.
> - **Two dedup tests, not one.** `TestSeriesDedup_GRPCPath` exercises the cache-before-insert path (200 sends → 1 catalogue row). `TestSeriesDedup_ReplacingMergeTree` exercises the merge-engine path (10 direct inserts + `OPTIMIZE FINAL` → 1 row). Together they prove dedup at both layers.
> - **Orphan check in `TestReferentialIntegrity`.** Beyond row counts, a `LEFT JOIN ... WHERE s.SeriesID = 0` assertion confirms no datapoint references a missing series.
> - **All 11 integration tests pass against `clickhouse-server:26.2` testcontainer** in ~17s wall clock (container pull dominates).

---

## Phase 4 — Batcher: Production Throughput

**Goal:** replace synchronous per-Export inserts with a buffered batcher. System behaviour unchanged from CH's perspective — just batched.

> **Pre-decisions (locked in before implementation):**
> - **Cache stays in service.go.** `filterNewSeries` is invoked in `Export()` *before* `batcher.AddSeries(...)`. Batcher buffers; it doesn't know about the cache. Keeps batcher responsibility narrow (buffer + flush).
> - **Batcher uses `context.Background()` for internal flushes.** Once a buffer is snapshot-and-reset, the insert must complete (or fail loudly) — it must not be cancelled by the request that triggered it or by the shutdown signal.
> - **`NewServer(addr, batcher, cache)`.** Store is hidden inside the batcher. `MetricsStore` interface stays where it is — batcher consumes it.
> - **Signal handling in cmd/main.go.** `signal.NotifyContext(...os.Interrupt, syscall.SIGTERM)` cancels a parent ctx; `grpcServer.GracefulStop()` runs in a goroutine watching that ctx; batcher's run-loop drains on ctx.Done() (with `context.Background()` for the drain flush so it doesn't observe its own cancellation).
> - **Integration tests get `batcher` from `getServer`.** Tests call `batcher.Flush(ctx)` after `Export()` before querying CH. Avoids racy sleep-based synchronisation.

### Tasks

1. Create `internal/ingest/batcher.go` — typed batcher, Phase 1 design:
   - One buffer per type (`[]SeriesRow`, `[]GaugeDatapointRow`, etc.)
   - `Add*` methods per type (no ctx param — batcher uses `context.Background()` for size-triggered flushes)
   - Single background ticker goroutine
   - Flush triggered by: any buffer hits `maxSize` OR ticker fires
   - Flush snapshots-and-resets under mutex, then sends *outside* the lock (Adds keep buffering during the in-flight insert)
   - Flush all buffers together — series first, then datapoints
   - Log on insert failure with `slog.Error` (`chInsertErrors` counter wires in Phase 5)
   - Drain all buffers on context cancel (SIGTERM)
   - Public `Flush(ctx)` method for explicit drain (used by tests + shutdown)

2. Update `internal/ingest/service.go` — replace direct inserts with batcher:

```go
func (m *dash0MetricsServiceServer) Export(ctx, request) {
    rows := MapRows(request.GetResourceMetrics())
    m.batcher.AddSeries(rows.Series)   // cache check inside
    m.batcher.AddGauge(rows.Gauge)
    m.batcher.AddSum(rows.Sum)
    // ...
    return &colmetricspb.ExportMetricsServiceResponse{}, nil
}
```

3. Add `TestBatchFlush`:
   - Flush triggers on `maxSize`
   - Flush triggers on ticker
   - Flush drains on shutdown (cancel context)
   - Series rows flushed before datapoints

### ✅ Done when
All previous integration tests still pass with batcher in the middle. `TestBatchFlush` passes.

> **Phase 4 execution notes (delta from plan):**
> - **5 batcher tests, not 1.** Split into `Size`, `Ticker`, `ShutdownDrains`, `SeriesBeforeDatapoints`, `EmptyAdd` — each isolates a single trigger so a failure points at the exact behaviour. Uses an in-memory `fakeStore` implementing `MetricsStore`.
> - **`anyBufferFullLocked()` helper.** Add* methods all share the same "any buffer crossed maxSize?" check. Inlining in each Add was noisier. (Refactored to `addTo[T]` generic in the final polish pass.)
> - **`Done() <-chan struct{}` on Batcher.** Plan called for "drain on shutdown" but no explicit sync primitive — `Done()` lets `cmd/main.go` wait for the run-loop's final drain to complete before exit.
> - **Integration helper change rippled to 3 tests.** `getServer(t)` now returns `(client, *Batcher, closer)`. Each gRPC-path test calls `batcher.Flush(ctx)` after the Export loop, before querying CH. Avoids racy sleep-based sync.
> - **Phase 5 considerations:** instruments will need to land in their own file (instruments.go) since multiple files now use the meter (service.go + batcher.go + cache.go). meter init moves out of service.go's init().

---

## Phase 5 — Operability: Instrumentation + Validation + README

**Goal:** production-grade observability of the service itself, input safety, and documented design decisions.

### Tasks

1. Extract instruments into `internal/ingest/instruments.go` (shared by service, batcher, cache). Instruments:

```go
// Counters
datapointsProcessed     // per metric type label
seriesCacheHits
seriesCacheMisses
chInsertErrors          // per table label

// Histograms
batchFlushDurationMs    // per table label
batchSize               // rows per flush, per table
```

2. Add structured log events at key moments:
   - Batch flushed (rows, series_new, duration_ms, table)
   - Insert failed (table, rows_dropped, error)
   - Datapoint skipped validation (reason, metric_name, series_id)

3. Add input validation in `internal/ingest/service.go` — reject and log (don't crash):
   - TimeUnix zero or negative
   - Value NaN or Inf
   - MetricName empty
   - ResourceAttributes with empty key

4. Add health endpoint — `GET /health` → ping ClickHouse → 200/503. Needs a separate HTTP listener since gRPC owns `4317`. Add `HealthConfig{ListenAddr string}` to `internal/config/config.go` (default `localhost:13133` — OTel collector convention), spin up `http.Server` in `cmd/main.go` alongside the gRPC server, drain on shutdown.

5. Write README with:
   - Schema rationale
   - Fingerprinting
   - Batching strategy
   - Design considerations tables (series dedup options, read path enrichment options)
   - Documented decisions: log-and-drop on flush failure, stdout exporters, LRU cache size, MetricType in SeriesRow
   - Production next steps: retry on flush failure, OTLP exporter, Phase 2 batcher

### ✅ Done when
Full test suite passes. README complete. `go vet ./...` clean.

---

## Phase 6 — Phase 2 Batcher (if time allows)

**Goal:** non-blocking Export handler, each metric type flushes independently.

Replace typed buffers + shared mutex with one goroutine per type, each with its own channel and ticker. Series goroutine flushes on a shorter interval (100ms) to stay ahead of datapoint goroutines.

### ✅ Done when
All tests still pass. Export handler returns without blocking on CH inserts.

---

## Decision Log

| Decision | Choice | Rationale |
|---|---|---|
| Flush failure | Log and drop (Option A) | Simple; error counter pages on-call; retry documented as next step |
| OTel exporter | Keep stdout | Demonstrates instrumentation; exporter swap is ops concern; documented in README |
| SeriesCache | hashicorp/golang-lru v2, size 100k | Bounded, thread-safe, no reinvention; evicted entries handled by ReplacingMergeTree |
| MetricType in SeriesRow | Keep | Required for correct SeriesID hash; makes series table a complete catalogue |
| Batcher design | Typed buffers, Phase 1 first | Each phase independently testable; Phase 2 is natural evolution |
| Mapper API | `MapRows() MappedRows` struct | Single OTLP tree traversal; adding new type = new field, no signature change |
| Directory structure | `cmd/` + `internal/storage/` + `internal/ingest/` + `integration_test/` | Avoids flat package; `internal/` enforces Go visibility |
| `MetricsStore` interface location | `internal/ingest/service.go` (consumer) | Go idiom: interface belongs to the package that uses it, not the one that implements it; avoids `ingest→storage` import coupling |
| Integration test isolation | Separate `integration_test/` directory + `//go:build integration` build tag | **Corrected during Phase 1.** Original plan claimed directory was sufficient, but `go test ./...` walks every package regardless. Build tag is the only way `make test` skips integration tests while `make test-integration` includes them via `-tags integration`. |
| Config | `internal/config/config.go`, env-var-backed + defaults | Eliminates magic literals; makes behaviour configurable without recompile; no external config library needed at this scale |