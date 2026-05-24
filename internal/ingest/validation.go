package ingest

import (
	"context"
	"log/slog"
	"math"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// Skip reasons emitted as the `reason` label on datapointsSkippedCounter.
// Centralised so log/counter labels never drift.
const (
	skipEmptyMetricName = "empty_metric_name"
	skipZeroTimestamp   = "zero_timestamp"
	skipInvalidValue    = "invalid_value" // NaN or Inf
)

// validNumberValue reports whether a NumberDataPoint's value is usable.
// Mirrors the integer arm too — AsInt is always finite, so only AsDouble
// can be NaN/Inf.
func validNumberValue(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

// hasZeroTimestamp returns true for the obviously-corrupt case of an unset
// timestamp. The OTLP spec calls 0 "invalid" — but we also reject negative
// nanoseconds (which a uint64 can't actually hold; reserved for future
// proofing if the proto type ever widens).
func hasZeroTimestamp(timeUnixNano uint64) bool {
	return timeUnixNano == 0
}

// skipMetric increments the skipped counter for the given reason and emits
// a structured warn-level log line. Caller is expected to return/continue
// the loop after this.
func skipMetric(ctx context.Context, reason, metricName string, extra ...slog.Attr) {
	datapointsSkippedCounter.Add(ctx, 1,
		metric.WithAttributes(attribute.String("reason", reason)))
	attrs := []any{slog.String("reason", reason), slog.String("metric_name", metricName)}
	for _, a := range extra {
		attrs = append(attrs, a)
	}
	slog.WarnContext(ctx, "datapoint skipped", attrs...)
}

// stripEmptyKeys filters out KV pairs with empty keys. OTLP attributes with
// empty keys are spec-invalid but seen in the wild; stripping is safer than
// rejecting the whole datapoint.
func stripEmptyKeys(attrs []*commonpb.KeyValue) []*commonpb.KeyValue {
	out := attrs[:0]
	for _, kv := range attrs {
		if kv.GetKey() != "" {
			out = append(out, kv)
		}
	}
	return out
}