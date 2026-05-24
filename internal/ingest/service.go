package ingest

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"

	"dash0.com/otlp-log-processor-backend/internal/storage"
)

// MetricsStore is the subset of the storage layer that the batcher consumes.
// Defined here so the ingest package owns the boundary; the batcher (also in
// ingest) and storage.ClickHouseMetricsStore (the implementor) both speak to
// it. storage.ClickHouseMetricsStore satisfies it via Go structural typing.
type MetricsStore interface {
	InsertSeries(ctx context.Context, rows []storage.SeriesRow) error
	InsertGauge(ctx context.Context, rows []storage.GaugeDatapointRow) error
	InsertSum(ctx context.Context, rows []storage.SumDatapointRow) error
	InsertHistogram(ctx context.Context, rows []storage.HistogramDatapointRow) error
	InsertExponentialHistogram(ctx context.Context, rows []storage.ExponentialHistogramDatapointRow) error
	InsertSummary(ctx context.Context, rows []storage.SummaryDatapointRow) error
}

type dash0MetricsServiceServer struct {
	addr    string
	batcher *Batcher
	cache   *SeriesCache

	colmetricspb.UnimplementedMetricsServiceServer
}

// NewServer constructs the gRPC MetricsService handler.
//
// `batcher` may be nil — in which case Export is a no-op (used by the unit
// test that only verifies the gRPC contract). `cache` may also be nil, in
// which case every series row is forwarded to the batcher; the batcher then
// relies on ReplacingMergeTree to dedup.
func NewServer(addr string, batcher *Batcher, cache *SeriesCache) colmetricspb.MetricsServiceServer {
	return &dash0MetricsServiceServer{addr: addr, batcher: batcher, cache: cache}
}

func (m *dash0MetricsServiceServer) Export(ctx context.Context, request *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	slog.DebugContext(ctx, "Received ExportMetricsServiceRequest")
	metricsReceivedCounter.Add(ctx, 1)

	if m.batcher == nil {
		return &colmetricspb.ExportMetricsServiceResponse{}, nil
	}

	rows := MapRows(ctx, request.GetResourceMetrics())
	recordDatapointCounts(ctx, rows)

	// Series get filtered through the cache first; the batcher only sees
	// series we haven't already written. Datapoints flow through unfiltered.
	m.batcher.AddSeries(filterNewSeries(rows.Series, m.cache))
	m.batcher.AddGauge(rows.Gauge)
	m.batcher.AddSum(rows.Sum)
	m.batcher.AddHistogram(rows.Histogram)
	m.batcher.AddExponentialHistogram(rows.ExponentialHistogram)
	m.batcher.AddSummary(rows.Summary)

	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

// recordDatapointCounts emits one counter increment per metric type with the
// number of accepted datapoints. Zero-length slices don't emit — keeps the
// `metric_type` label cardinality bounded to types actually seen.
func recordDatapointCounts(ctx context.Context, rows MappedRows) {
	type entry struct {
		typeLabel string
		count     int
	}
	for _, e := range []entry{
		{metricTypeGauge, len(rows.Gauge)},
		{metricTypeSum, len(rows.Sum)},
		{metricTypeHistogram, len(rows.Histogram)},
		{metricTypeExponentialHistogram, len(rows.ExponentialHistogram)},
		{metricTypeSummary, len(rows.Summary)},
	} {
		if e.count == 0 {
			continue
		}
		datapointsProcessedCounter.Add(ctx, int64(e.count),
			metric.WithAttributes(attribute.String("metric_type", e.typeLabel)))
	}
}

// filterNewSeries returns the subset of `series` whose SeriesID was not
// already cached. A nil cache means no filtering — every row passes through.
// The cache is updated as a side effect: each new ID is marked seen.
func filterNewSeries(series []storage.SeriesRow, cache *SeriesCache) []storage.SeriesRow {
	if cache == nil || len(series) == 0 {
		return series
	}
	var out []storage.SeriesRow
	for _, s := range series {
		if cache.MarkIfNew(s.SeriesID) {
			out = append(out, s)
		}
	}
	return out
}