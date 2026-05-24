package ingest

import (
	"context"
	"fmt"
	"log/slog"

	"dash0.com/otlp-log-processor-backend/internal/storage"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
)

const meterName = "dash0.com/otlp-log-processor-backend"

var (
	meter                  = otel.Meter(meterName)
	metricsReceivedCounter metric.Int64Counter
)

func init() {
	var err error
	metricsReceivedCounter, err = meter.Int64Counter("com.dash0.homeexercise.metrics.received",
		metric.WithDescription("The number of metrics received by otlp-metrics-processor-backend"),
		metric.WithUnit("{metric}"))
	if err != nil {
		panic(err)
	}
}

// MetricsStore is the subset of the storage layer that this package consumes.
// Defined here, not in storage/, so the consumer owns the interface boundary.
// storage.ClickHouseMetricsStore satisfies it via Go structural typing.
type MetricsStore interface {
	InsertSeries(ctx context.Context, rows []storage.SeriesRow) error
	InsertGauge(ctx context.Context, rows []storage.GaugeDatapointRow) error
	InsertSum(ctx context.Context, rows []storage.SumDatapointRow) error
	InsertHistogram(ctx context.Context, rows []storage.HistogramDatapointRow) error
	InsertExponentialHistogram(ctx context.Context, rows []storage.ExponentialHistogramDatapointRow) error
	InsertSummary(ctx context.Context, rows []storage.SummaryDatapointRow) error
}

type dash0MetricsServiceServer struct {
	addr  string
	store MetricsStore

	colmetricspb.UnimplementedMetricsServiceServer
}

// NewServer constructs the gRPC MetricsService handler.
func NewServer(addr string, store MetricsStore) colmetricspb.MetricsServiceServer {
	return &dash0MetricsServiceServer{addr: addr, store: store}
}

func (m *dash0MetricsServiceServer) Export(ctx context.Context, request *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	slog.DebugContext(ctx, "Received ExportMetricsServiceRequest")
	metricsReceivedCounter.Add(ctx, 1)

	if m.store == nil {
		return &colmetricspb.ExportMetricsServiceResponse{}, nil
	}

	rows := MapRows(request.GetResourceMetrics())

	// Series first — datapoints reference SeriesID, so writing the catalogue
	// entry before the points avoids a short window where a datapoint refers
	// to an unknown series at query time. ReplacingMergeTree dedupes
	// repeats during background merges; SeriesCache (Phase 3) will skip
	// the calls entirely for already-seen series.
	if len(rows.Series) > 0 {
		if err := m.store.InsertSeries(ctx, rows.Series); err != nil {
			return nil, fmt.Errorf("insert series: %w", err)
		}
	}
	if len(rows.Gauge) > 0 {
		if err := m.store.InsertGauge(ctx, rows.Gauge); err != nil {
			return nil, fmt.Errorf("insert gauge: %w", err)
		}
	}
	if len(rows.Sum) > 0 {
		if err := m.store.InsertSum(ctx, rows.Sum); err != nil {
			return nil, fmt.Errorf("insert sum: %w", err)
		}
	}
	if len(rows.Histogram) > 0 {
		if err := m.store.InsertHistogram(ctx, rows.Histogram); err != nil {
			return nil, fmt.Errorf("insert histogram: %w", err)
		}
	}
	if len(rows.ExponentialHistogram) > 0 {
		if err := m.store.InsertExponentialHistogram(ctx, rows.ExponentialHistogram); err != nil {
			return nil, fmt.Errorf("insert exponential histogram: %w", err)
		}
	}
	if len(rows.Summary) > 0 {
		if err := m.store.InsertSummary(ctx, rows.Summary); err != nil {
			return nil, fmt.Errorf("insert summary: %w", err)
		}
	}

	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}