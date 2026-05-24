package ingest

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// meterName scopes every instrument this service emits. Matches the existing
// otelslog logger in cmd/main.go so logs and metrics share an instrumentation
// scope at query time.
const meterName = "dash0.com/otlp-log-processor-backend"

var (
	meter = otel.Meter(meterName)

	// metricsReceivedCounter increments once per gRPC Export request.
	metricsReceivedCounter metric.Int64Counter

	// datapointsProcessedCounter is per-metric-type (label `metric_type`).
	// Equivalent to "how many datapoints did MapRows produce that we accepted".
	datapointsProcessedCounter metric.Int64Counter

	// datapointsSkippedCounter increments for each datapoint MapRows
	// rejected during validation (label `reason`).
	datapointsSkippedCounter metric.Int64Counter

	// SeriesCache hits/misses are recorded inside MarkIfNew.
	seriesCacheHitsCounter   metric.Int64Counter
	seriesCacheMissesCounter metric.Int64Counter

	// chInsertErrorsCounter increments once per failed batch insert
	// (label `table`). Pages on-call in a real environment.
	chInsertErrorsCounter metric.Int64Counter

	// batchFlushDurationHist records the wall-clock duration of each
	// successful or failed batch insert (label `table`, unit ms).
	batchFlushDurationHist metric.Float64Histogram

	// batchSizeHist records rows per flushed batch (label `table`). Useful
	// for tuning BATCHER_MAX_SIZE and FLUSH_EVERY.
	batchSizeHist metric.Int64Histogram
)

func init() {
	var err error
	metricsReceivedCounter, err = meter.Int64Counter("com.dash0.homeexercise.metrics.received",
		metric.WithDescription("Total ExportMetricsServiceRequests received"),
		metric.WithUnit("{request}"))
	if err != nil {
		panic(err)
	}

	datapointsProcessedCounter, err = meter.Int64Counter("com.dash0.homeexercise.datapoints.processed",
		metric.WithDescription("Datapoints accepted by the mapper, by metric type"),
		metric.WithUnit("{datapoint}"))
	if err != nil {
		panic(err)
	}

	datapointsSkippedCounter, err = meter.Int64Counter("com.dash0.homeexercise.datapoints.skipped",
		metric.WithDescription("Datapoints rejected during validation, by reason"),
		metric.WithUnit("{datapoint}"))
	if err != nil {
		panic(err)
	}

	seriesCacheHitsCounter, err = meter.Int64Counter("com.dash0.homeexercise.series_cache.hits",
		metric.WithDescription("SeriesIDs found already cached (no catalogue write needed)"),
		metric.WithUnit("{series}"))
	if err != nil {
		panic(err)
	}

	seriesCacheMissesCounter, err = meter.Int64Counter("com.dash0.homeexercise.series_cache.misses",
		metric.WithDescription("SeriesIDs not in cache (catalogue write enqueued)"),
		metric.WithUnit("{series}"))
	if err != nil {
		panic(err)
	}

	chInsertErrorsCounter, err = meter.Int64Counter("com.dash0.homeexercise.clickhouse.insert_errors",
		metric.WithDescription("Failed batch inserts, by destination table"),
		metric.WithUnit("{error}"))
	if err != nil {
		panic(err)
	}

	batchFlushDurationHist, err = meter.Float64Histogram("com.dash0.homeexercise.batcher.flush_duration",
		metric.WithDescription("Wall-clock duration of each batch insert, by table"),
		metric.WithUnit("ms"))
	if err != nil {
		panic(err)
	}

	batchSizeHist, err = meter.Int64Histogram("com.dash0.homeexercise.batcher.batch_size",
		metric.WithDescription("Rows per flushed batch, by table"),
		metric.WithUnit("{row}"))
	if err != nil {
		panic(err)
	}
}