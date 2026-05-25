package ingest

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"dash0.com/otlp-metric-store/internal/config"
	"dash0.com/otlp-metric-store/internal/storage"
)

// Batcher buffers typed rows and writes them to the store in batches.
// Two flush triggers: a buffer reaches `maxSize` (size-triggered) or the
// ticker fires (time-triggered). Both call the same Flush path.
//
// All buffers flush together on every trigger — keeps series writes ahead of
// their datapoints with no extra coordination. Failed inserts are logged and
// dropped (Option A from the design considerations); a retry queue is the
// next production step.
//
// Add* methods do not take a ctx and internal flushes use context.Background():
// once Flush has snapshot-and-reset a buffer, the insert must complete (or
// fail loudly), not be cancelled by the request that triggered it or by the
// shutdown signal. Public Flush(ctx) is for callers that want to drive a
// drain (tests, graceful shutdown).
type Batcher struct {
	store      MetricsStore
	maxSize    int
	flushEvery time.Duration

	mu                   sync.Mutex
	series               []storage.SeriesRow
	gauge                []storage.GaugeDatapointRow
	sum                  []storage.SumDatapointRow
	histogram            []storage.HistogramDatapointRow
	exponentialHistogram []storage.ExponentialHistogramDatapointRow
	summary              []storage.SummaryDatapointRow

	// done closes when the run loop exits, so the shutdown path can wait
	// for the final drain to complete.
	done chan struct{}
}

// NewBatcher starts a background flush loop bound to ctx. When ctx is
// cancelled the loop drains all buffers and exits.
func NewBatcher(ctx context.Context, store MetricsStore, cfg config.BatcherConfig) *Batcher {
	b := &Batcher{
		store:      store,
		maxSize:    cfg.MaxSize,
		flushEvery: cfg.FlushEvery,
		done:       make(chan struct{}),
	}
	go b.run(ctx)
	return b
}

// Done returns a channel closed when the run loop has finished its shutdown
// drain.
func (b *Batcher) Done() <-chan struct{} { return b.done }

func (b *Batcher) run(ctx context.Context) {
	defer close(b.done)
	ticker := time.NewTicker(b.flushEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			b.Flush(context.Background())
		case <-ctx.Done():
			b.Flush(context.Background())
			return
		}
	}
}

// Flush snapshots every buffer under the mutex then sends each outside the
// lock so concurrent Add* calls keep buffering. Series goes first so its
// catalogue rows land before any datapoint that references them.
func (b *Batcher) Flush(ctx context.Context) {
	b.mu.Lock()
	series := snapshot(&b.series)
	gauge := snapshot(&b.gauge)
	sum := snapshot(&b.sum)
	histogram := snapshot(&b.histogram)
	expHist := snapshot(&b.exponentialHistogram)
	summary := snapshot(&b.summary)
	b.mu.Unlock()

	b.flushOne(ctx, "otel_metric_series", len(series), func() error { return b.store.InsertSeries(ctx, series) })
	b.flushOne(ctx, "otel_metrics_gauge", len(gauge), func() error { return b.store.InsertGauge(ctx, gauge) })
	b.flushOne(ctx, "otel_metrics_sum", len(sum), func() error { return b.store.InsertSum(ctx, sum) })
	b.flushOne(ctx, "otel_metrics_histogram", len(histogram), func() error { return b.store.InsertHistogram(ctx, histogram) })
	b.flushOne(ctx, "otel_metrics_exponential_histogram", len(expHist), func() error { return b.store.InsertExponentialHistogram(ctx, expHist) })
	b.flushOne(ctx, "otel_metrics_summary", len(summary), func() error { return b.store.InsertSummary(ctx, summary) })
}

// flushOne wraps a single Insert* call with timing, batch-size, and
// error-counter instrumentation. Zero-row batches short-circuit.
//
// Log-and-drop on failure (Option A in design considerations): increment the
// error counter and emit a structured log; rows are NOT retried. A retry
// queue is the production next-step in the README.
func (b *Batcher) flushOne(ctx context.Context, table string, rows int, insert func() error) {
	if rows == 0 {
		return
	}
	attrs := metric.WithAttributes(attribute.String("table", table))
	batchSizeHist.Record(ctx, int64(rows), attrs)

	start := time.Now()
	err := insert()
	elapsed := time.Since(start)
	batchFlushDurationHist.Record(ctx, float64(elapsed.Milliseconds()), attrs)

	if err != nil {
		chInsertErrorsCounter.Add(ctx, 1, attrs)
		slog.ErrorContext(ctx, "batcher: insert failed",
			"table", table, "rows_dropped", rows,
			"duration_ms", elapsed.Milliseconds(), "err", err)
		return
	}
	slog.DebugContext(ctx, "batcher: flushed",
		"table", table, "rows", rows, "duration_ms", elapsed.Milliseconds())
}

func (b *Batcher) AddSeries(rows []storage.SeriesRow) {
	addTo(b, &b.series, rows)
}
func (b *Batcher) AddGauge(rows []storage.GaugeDatapointRow) {
	addTo(b, &b.gauge, rows)
}
func (b *Batcher) AddSum(rows []storage.SumDatapointRow) {
	addTo(b, &b.sum, rows)
}
func (b *Batcher) AddHistogram(rows []storage.HistogramDatapointRow) {
	addTo(b, &b.histogram, rows)
}
func (b *Batcher) AddExponentialHistogram(rows []storage.ExponentialHistogramDatapointRow) {
	addTo(b, &b.exponentialHistogram, rows)
}
func (b *Batcher) AddSummary(rows []storage.SummaryDatapointRow) {
	addTo(b, &b.summary, rows)
}

// addTo appends rows to a typed buffer pointer and triggers a Flush if that
// buffer crossed maxSize. Only the buffer just touched can have crossed —
// the other five would have triggered their own Flush on their last Add. The
// generic helper means each Add* is a one-liner; Go monomorphises per T at
// compile time so there is no runtime dispatch cost.
func addTo[T any](b *Batcher, buf *[]T, rows []T) {
	if len(rows) == 0 {
		return
	}
	b.mu.Lock()
	*buf = append(*buf, rows...)
	full := len(*buf) >= b.maxSize
	b.mu.Unlock()
	if full {
		b.Flush(context.Background())
	}
}

// snapshot atomically reads and clears a buffer. Caller must hold b.mu.
func snapshot[T any](buf *[]T) []T {
	out := *buf
	*buf = nil
	return out
}
