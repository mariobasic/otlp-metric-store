package ingest

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
)

type dash0MetricsServiceServer struct {
	addr     string
	producer *Producer

	colmetricspb.UnimplementedMetricsServiceServer
}

// NewServer constructs the gRPC MetricsService handler.
// A nil producer makes Export a no-op (used by the unit test that only
// verifies the gRPC contract).
func NewServer(addr string, producer *Producer) colmetricspb.MetricsServiceServer {
	return &dash0MetricsServiceServer{addr: addr, producer: producer}
}

func (m *dash0MetricsServiceServer) Export(ctx context.Context, request *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	slog.DebugContext(ctx, "Received ExportMetricsServiceRequest")
	metricsReceivedCounter.Add(ctx, 1)

	if m.producer == nil {
		return &colmetricspb.ExportMetricsServiceResponse{}, nil
	}

	rows := MapRows(ctx, request.GetResourceMetrics())
	recordDatapointCounts(ctx, rows)

	for _, pub := range []struct {
		suffix string
		rows   any
	}{
		{"series", rows.Series},
		{"gauge", rows.Gauge},
		{"sum", rows.Sum},
		{"histogram", rows.Histogram},
		{"exponential_histogram", rows.ExponentialHistogram},
		{"summary", rows.Summary},
	} {
		if err := m.producer.Publish(ctx, pub.suffix, pub.rows); err != nil {
			return nil, err
		}
	}

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