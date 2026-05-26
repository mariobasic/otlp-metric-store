package storage

import (
	"encoding/json"
	"time"
)

// CHNanoTime serializes as "2006-01-02 15:04:05.000000000" for DateTime64(9) columns.
type CHNanoTime time.Time

func (t CHNanoTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(t).UTC().Format("2006-01-02 15:04:05.000000000"))
}

func (t CHNanoTime) UnixNano() int64 { return time.Time(t).UnixNano() }

// CHDateTime serializes as "2006-01-02 15:04:05" for DateTime columns.
type CHDateTime time.Time

func (t CHDateTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(t).UTC().Format("2006-01-02 15:04:05"))
}

func (t CHDateTime) IsZero() bool { return time.Time(t).IsZero() }

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
	FirstSeen             CHDateTime
	LastSeen              CHDateTime
}

// BaseDatapointRow is the fields every metric datapoint table shares: link
// back to the series catalogue, timestamps, OTLP flags.
type BaseDatapointRow struct {
	SeriesID      uint64
	StartTimeUnix CHNanoTime
	TimeUnix      CHNanoTime
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

// MarshalJSON flattens ValueAtQuantiles into the dotted-key arrays that
// ClickHouse's JSONEachRow parser requires for Nested columns.
func (r SummaryDatapointRow) MarshalJSON() ([]byte, error) {
	quantiles := make([]float64, len(r.ValueAtQuantiles))
	values := make([]float64, len(r.ValueAtQuantiles))
	for i, q := range r.ValueAtQuantiles {
		quantiles[i] = q.Quantile
		values[i] = q.Value
	}
	type flat struct {
		SeriesID                 uint64     `json:"SeriesID"`
		StartTimeUnix            CHNanoTime `json:"StartTimeUnix"`
		TimeUnix                 CHNanoTime `json:"TimeUnix"`
		Flags                    uint32     `json:"Flags"`
		Count                    uint64     `json:"Count"`
		Sum                      float64    `json:"Sum"`
		ValueAtQuantilesQuantile []float64  `json:"ValueAtQuantiles.Quantile"`
		ValueAtQuantilesValue    []float64  `json:"ValueAtQuantiles.Value"`
	}
	return json.Marshal(flat{
		SeriesID:                 r.SeriesID,
		StartTimeUnix:            r.StartTimeUnix,
		TimeUnix:                 r.TimeUnix,
		Flags:                    r.Flags,
		Count:                    r.Count,
		Sum:                      r.Sum,
		ValueAtQuantilesQuantile: quantiles,
		ValueAtQuantilesValue:    values,
	})
}