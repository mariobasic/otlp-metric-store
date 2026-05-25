package ingest

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const meterName = "dash0.com/otlp-metric-store"

var (
	meter = otel.Meter(meterName)

	metricsReceivedCounter     metric.Int64Counter
	datapointsProcessedCounter metric.Int64Counter
	datapointsSkippedCounter   metric.Int64Counter
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func init() {
	metricsReceivedCounter = must(meter.Int64Counter("com.dash0.homeexercise.metrics.received",
		metric.WithDescription("Total ExportMetricsServiceRequests received"),
		metric.WithUnit("{request}")))

	datapointsProcessedCounter = must(meter.Int64Counter("com.dash0.homeexercise.datapoints.processed",
		metric.WithDescription("Datapoints accepted by the mapper, by metric type"),
		metric.WithUnit("{datapoint}")))

	datapointsSkippedCounter = must(meter.Int64Counter("com.dash0.homeexercise.datapoints.skipped",
		metric.WithDescription("Datapoints rejected during validation, by reason"),
		metric.WithUnit("{datapoint}")))
}