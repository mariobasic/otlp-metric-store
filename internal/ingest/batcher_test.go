package ingest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"dash0.com/otlp-log-processor-backend/internal/config"
	"dash0.com/otlp-log-processor-backend/internal/storage"
)

// fakeStore records every Insert* call in order with table name and row count.
// Implements MetricsStore. Concurrency-safe.
type fakeStore struct {
	mu    sync.Mutex
	calls []string // e.g. "series:5", "gauge:10"
}

func (f *fakeStore) record(table string, rows int) {
	f.mu.Lock()
	f.calls = append(f.calls, fmt.Sprintf("%s:%d", table, rows))
	f.mu.Unlock()
}

func (f *fakeStore) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.calls))
	copy(out, f.calls)
	return out
}

func (f *fakeStore) InsertSeries(_ context.Context, rows []storage.SeriesRow) error {
	f.record("series", len(rows))
	return nil
}
func (f *fakeStore) InsertGauge(_ context.Context, rows []storage.GaugeDatapointRow) error {
	f.record("gauge", len(rows))
	return nil
}
func (f *fakeStore) InsertSum(_ context.Context, rows []storage.SumDatapointRow) error {
	f.record("sum", len(rows))
	return nil
}
func (f *fakeStore) InsertHistogram(_ context.Context, rows []storage.HistogramDatapointRow) error {
	f.record("histogram", len(rows))
	return nil
}
func (f *fakeStore) InsertExponentialHistogram(_ context.Context, rows []storage.ExponentialHistogramDatapointRow) error {
	f.record("exp_histogram", len(rows))
	return nil
}
func (f *fakeStore) InsertSummary(_ context.Context, rows []storage.SummaryDatapointRow) error {
	f.record("summary", len(rows))
	return nil
}

// helpers ---------------------------------------------------------------

func mkGauge(n int) []storage.GaugeDatapointRow {
	out := make([]storage.GaugeDatapointRow, n)
	for i := range out {
		out[i].SeriesID = uint64(i + 1)
		out[i].Value = float64(i)
	}
	return out
}

func mkSeries(n int) []storage.SeriesRow {
	out := make([]storage.SeriesRow, n)
	for i := range out {
		out[i].SeriesID = uint64(i + 1)
	}
	return out
}

func mustNewBatcher(t *testing.T, ctx context.Context, store MetricsStore, maxSize int, flushEvery time.Duration) *Batcher {
	t.Helper()
	return NewBatcher(ctx, store, config.BatcherConfig{MaxSize: maxSize, FlushEvery: flushEvery})
}

// tests -----------------------------------------------------------------

func TestBatcher_SizeTriggeredFlush(t *testing.T) {
	store := &fakeStore{}
	// Long flushEvery so the ticker can't be the trigger inside this test.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := mustNewBatcher(t, ctx, store, 5, time.Hour)

	b.AddGauge(mkGauge(4)) // below threshold — no flush
	if got := len(store.snapshot()); got != 0 {
		t.Fatalf("after 4/5 rows: want 0 calls, got %d (%v)", got, store.snapshot())
	}

	b.AddGauge(mkGauge(1)) // reaches threshold — flush synchronously
	calls := store.snapshot()
	if len(calls) != 1 || calls[0] != "gauge:5" {
		t.Fatalf("after 5/5 rows: want [gauge:5], got %v", calls)
	}
}

func TestBatcher_TickerTriggeredFlush(t *testing.T) {
	store := &fakeStore{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := mustNewBatcher(t, ctx, store, 1000, 25*time.Millisecond)

	b.AddGauge(mkGauge(3)) // below threshold; relies on ticker

	// Wait up to ~5 ticks; ticker should have fired and drained.
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(store.snapshot()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	calls := store.snapshot()
	if len(calls) != 1 || calls[0] != "gauge:3" {
		t.Fatalf("expected ticker flush of [gauge:3], got %v", calls)
	}
}

func TestBatcher_ShutdownDrainsBuffers(t *testing.T) {
	store := &fakeStore{}
	ctx, cancel := context.WithCancel(context.Background())
	b := mustNewBatcher(t, ctx, store, 1000, time.Hour) // ticker won't fire in this test

	b.AddSeries(mkSeries(2))
	b.AddGauge(mkGauge(3))

	if got := len(store.snapshot()); got != 0 {
		t.Fatalf("before shutdown: want 0 calls, got %d (%v)", got, store.snapshot())
	}

	cancel()
	<-b.Done()

	calls := store.snapshot()
	if len(calls) != 2 {
		t.Fatalf("after shutdown: want 2 calls, got %d (%v)", len(calls), calls)
	}
	if calls[0] != "series:2" || calls[1] != "gauge:3" {
		t.Fatalf("drain ordering wrong: want [series:2 gauge:3], got %v", calls)
	}
}

func TestBatcher_SeriesFlushedBeforeDatapoints(t *testing.T) {
	// Even when datapoints are buffered *before* series, the flush must emit
	// series first so query-time lookups against the catalogue succeed.
	store := &fakeStore{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := mustNewBatcher(t, ctx, store, 1000, time.Hour)

	b.AddGauge(mkGauge(2))
	b.AddSeries(mkSeries(1))

	b.Flush(ctx)

	calls := store.snapshot()
	if len(calls) != 2 {
		t.Fatalf("want 2 calls, got %d (%v)", len(calls), calls)
	}
	if calls[0] != "series:1" || calls[1] != "gauge:2" {
		t.Fatalf("series must precede datapoints: got %v", calls)
	}
}

// ctxRecordingStore is a fakeStore that captures the ctx of each Insert call
// so the test can assert it wasn't cancelled at insert time.
type ctxRecordingStore struct {
	fakeStore
	mu          sync.Mutex
	seriesCtxes []context.Context
}

func (s *ctxRecordingStore) InsertSeries(ctx context.Context, rows []storage.SeriesRow) error {
	s.mu.Lock()
	s.seriesCtxes = append(s.seriesCtxes, ctx)
	s.mu.Unlock()
	return s.fakeStore.InsertSeries(ctx, rows)
}

// Regression: an in-flight Add* that triggers a size flush during shutdown
// must use a non-cancelled ctx, otherwise the snapshot (already removed from
// the buffer) gets dropped when the CH driver observes the cancellation.
// See review finding #2.
func TestBatcher_SizeFlushSurvivesParentCtxCancel(t *testing.T) {
	store := &ctxRecordingStore{}
	parent, cancelParent := context.WithCancel(context.Background())

	b := NewBatcher(parent, store, config.BatcherConfig{MaxSize: 1, FlushEvery: time.Hour})

	cancelParent() // simulate SIGTERM; run-loop is heading to drain

	b.AddSeries(mkSeries(1)) // triggers a size flush via internalCtx

	// run-loop will drain on ctx.Done(); wait for it.
	<-b.Done()

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.seriesCtxes) == 0 {
		t.Fatal("expected at least one InsertSeries call")
	}
	for i, ctx := range store.seriesCtxes {
		if err := ctx.Err(); err != nil {
			t.Errorf("InsertSeries[%d] ctx was cancelled: %v — rows would have been dropped", i, err)
		}
	}
}

func TestBatcher_EmptyAddNoOps(t *testing.T) {
	store := &fakeStore{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := mustNewBatcher(t, ctx, store, 1, time.Hour)

	b.AddGauge(nil)
	b.AddGauge([]storage.GaugeDatapointRow{})
	b.AddSeries(nil)

	b.Flush(ctx)
	if got := store.snapshot(); len(got) != 0 {
		t.Fatalf("empty adds + flush should produce no store calls, got %v", got)
	}
}