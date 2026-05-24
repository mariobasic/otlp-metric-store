package ingest

import (
	"context"
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
	InsertGauge(ctx context.Context, rows []storage.GaugeRow) error
	InsertSum(ctx context.Context, rows []storage.SumRow) error
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

	if m.store != nil {
		rm := request.GetResourceMetrics()

		if gaugeRows := MapGaugeRows(rm); len(gaugeRows) > 0 {
			if err := m.store.InsertGauge(ctx, gaugeRows); err != nil {
				return nil, err
			}
		}
		if sumRows := MapSumRows(rm); len(sumRows) > 0 {
			if err := m.store.InsertSum(ctx, sumRows); err != nil {
				return nil, err
			}
		}
	}

	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}