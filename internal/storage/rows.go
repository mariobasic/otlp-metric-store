package storage

import "time"

// SeriesRow is one entry in the otel_metric_series catalogue. A series is
// uniquely identified by its SeriesID — an xxhash fingerprint of MetricName,
// MetricType, ResourceAttributes, ScopeName/Version, ResourceSchemaUrl, and
// the datapoint Attributes. ReplacingMergeTree(LastSeen) deduplicates by
// SeriesID during background merges, so re-inserting an already-known series
// is harmless.
type SeriesRow struct {
	SeriesID              uint64
	MetricType            string
	ServiceName           string
	MetricName            string
	MetricDescription     string
	MetricUnit            string
	ResourceAttributes    map[string]string
	ResourceSchemaUrl     string
	ScopeName             string
	ScopeVersion          string
	ScopeAttributes       map[string]string
	ScopeDroppedAttrCount uint32
	ScopeSchemaUrl        string
	Attributes            map[string]string
	FirstSeen             time.Time
	LastSeen              time.Time
}

// BaseDatapointRow is the fields every metric datapoint table shares: link
// back to the series catalogue, timestamps, OTLP flags.
type BaseDatapointRow struct {
	SeriesID      uint64
	StartTimeUnix time.Time
	TimeUnix      time.Time
	Flags         uint32
}

// GaugeDatapointRow is the row written to otel_metrics_gauge.
type GaugeDatapointRow struct {
	BaseDatapointRow
	Value float64
}

// SumDatapointRow extends GaugeDatapointRow with the cumulative-counter
// semantics: temporality (delta vs cumulative) and monotonicity.
type SumDatapointRow struct {
	GaugeDatapointRow
	AggregationTemporality int32
	IsMonotonic            bool
}

// HistogramDatapointRow is the row written to otel_metrics_histogram —
// explicit-bucket histograms with optional min/max.
type HistogramDatapointRow struct {
	BaseDatapointRow
	Count                  uint64
	Sum                    float64
	BucketCounts           []uint64
	ExplicitBounds         []float64
	Min                    float64
	Max                    float64
	AggregationTemporality int32
}

// ExponentialHistogramDatapointRow is the row written to
// otel_metrics_exponential_histogram — exponentially-bucketed histograms
// with positive/negative offset arrays.
type ExponentialHistogramDatapointRow struct {
	BaseDatapointRow
	Count                  uint64
	Sum                    float64
	Scale                  int32
	ZeroCount              uint64
	PositiveOffset         int32
	PositiveBucketCounts   []uint64
	NegativeOffset         int32
	NegativeBucketCounts   []uint64
	Min                    float64
	Max                    float64
	AggregationTemporality int32
}

// SummaryDatapointRow is the row written to otel_metrics_summary —
// quantile-based summaries.
type SummaryDatapointRow struct {
	BaseDatapointRow
	Count            uint64
	Sum              float64
	ValueAtQuantiles []SummaryQuantile
}

// SummaryQuantile maps to the ValueAtQuantiles Nested(Quantile, Value)
// column in otel_metrics_summary.
type SummaryQuantile struct {
	Quantile float64
	Value    float64
}