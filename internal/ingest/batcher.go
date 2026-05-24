package ingest

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"dash0.com/otlp-log-processor-backend/internal/config"
	"dash0.com/otlp-log-processor-backend/internal/storage"
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
// Adds do not take a ctx: a request context can be cancelled before its rows
// reach CH (the batcher may carry rows past the request lifetime). The
// batcher uses a long-lived ctx captured at construction. Public `Flush(ctx)`
// is for callers that want to wait for a drain (tests, graceful shutdown).
type Batcher struct {
	store      MetricsStore
	maxSize    int
	flushEvery time.Duration

	// internalCtx is used by the ticker loop and by size-triggered flushes.
	// Survives the request that triggered the flush; only cancelled when the
	// parent ctx fires during shutdown.
	internalCtx context.Context

	mu                   sync.Mutex
	series               []storage.SeriesRow
	gauge                []storage.GaugeDatapointRow
	sum                  []storage.SumDatapointRow
	histogram            []storage.HistogramDatapointRow
	exponentialHistogram []storage.ExponentialHistogramDatapointRow
	summary              []storage.SummaryDatapointRow

	// done closes when the background run loop exits, so the shutdown path
	// can wait for the drain to complete before returning.
	done chan struct{}
}

// NewBatcher starts a background flush loop bound to ctx. When ctx is
// cancelled the loop drains all buffers with a fresh background ctx (so the
// final flush isn't itself cancelled) and exits.
func NewBatcher(ctx context.Context, store MetricsStore, cfg config.BatcherConfig) *Batcher {
	b := &Batcher{
		store:       store,
		maxSize:     cfg.MaxSize,
		flushEvery:  cfg.FlushEvery,
		internalCtx: ctx,
		done:        make(chan struct{}),
	}
	go b.run(ctx)
	return b
}

// Done returns a channel closed when the background loop has finished its
// shutdown drain. Callers in graceful-shutdown paths should wait on it before
// returning.
func (b *Batcher) Done() <-chan struct{} { return b.done }

func (b *Batcher) run(ctx context.Context) {
	defer close(b.done)
	ticker := time.NewTicker(b.flushEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			b.Flush(b.internalCtx)
		case <-ctx.Done():
			// Drain with a fresh ctx so the final flush isn't cancelled along
			// with everything else.
			b.Flush(context.Background())
			return
		}
	}
}

// Flush sends every buffered row to the store. Snapshots all buffers under
// the mutex, then sends outside the lock so concurrent Add* calls keep
// buffering. Always sends series first.
func (b *Batcher) Flush(ctx context.Context) {
	b.mu.Lock()
	series := b.series
	gauge := b.gauge
	sum := b.sum
	histogram := b.histogram
	expHist := b.exponentialHistogram
	summary := b.summary
	b.series = nil
	b.gauge = nil
	b.sum = nil
	b.histogram = nil
	b.exponentialHistogram = nil
	b.summary = nil
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
// Log-and-drop on failure (Option A in design considerations): we increment
// the error counter and emit a structured log; rows are NOT retried in
// Phase 1's batcher. Documented as a production next-step (retry queue) in
// the README.
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

// AddSeries appends series rows to the buffer. Triggers a flush if any buffer
// is at or above maxSize after the append.
func (b *Batcher) AddSeries(rows []storage.SeriesRow) {
	if len(rows) == 0 {
		return
	}
	b.mu.Lock()
	b.series = append(b.series, rows...)
	full := b.anyBufferFullLocked()
	b.mu.Unlock()
	if full {
		b.Flush(b.internalCtx)
	}
}

func (b *Batcher) AddGauge(rows []storage.GaugeDatapointRow) {
	if len(rows) == 0 {
		return
	}
	b.mu.Lock()
	b.gauge = append(b.gauge, rows...)
	full := b.anyBufferFullLocked()
	b.mu.Unlock()
	if full {
		b.Flush(b.internalCtx)
	}
}

func (b *Batcher) AddSum(rows []storage.SumDatapointRow) {
	if len(rows) == 0 {
		return
	}
	b.mu.Lock()
	b.sum = append(b.sum, rows...)
	full := b.anyBufferFullLocked()
	b.mu.Unlock()
	if full {
		b.Flush(b.internalCtx)
	}
}

func (b *Batcher) AddHistogram(rows []storage.HistogramDatapointRow) {
	if len(rows) == 0 {
		return
	}
	b.mu.Lock()
	b.histogram = append(b.histogram, rows...)
	full := b.anyBufferFullLocked()
	b.mu.Unlock()
	if full {
		b.Flush(b.internalCtx)
	}
}

func (b *Batcher) AddExponentialHistogram(rows []storage.ExponentialHistogramDatapointRow) {
	if len(rows) == 0 {
		return
	}
	b.mu.Lock()
	b.exponentialHistogram = append(b.exponentialHistogram, rows...)
	full := b.anyBufferFullLocked()
	b.mu.Unlock()
	if full {
		b.Flush(b.internalCtx)
	}
}

func (b *Batcher) AddSummary(rows []storage.SummaryDatapointRow) {
	if len(rows) == 0 {
		return
	}
	b.mu.Lock()
	b.summary = append(b.summary, rows...)
	full := b.anyBufferFullLocked()
	b.mu.Unlock()
	if full {
		b.Flush(b.internalCtx)
	}
}

// anyBufferFullLocked reports whether any buffer has crossed the maxSize
// threshold. Called with the mutex held.
func (b *Batcher) anyBufferFullLocked() bool {
	return len(b.series) >= b.maxSize ||
		len(b.gauge) >= b.maxSize ||
		len(b.sum) >= b.maxSize ||
		len(b.histogram) >= b.maxSize ||
		len(b.exponentialHistogram) >= b.maxSize ||
		len(b.summary) >= b.maxSize
}