package ingest

import (
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// helpers ---------------------------------------------------------------

func strKV(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func resource(attrs ...*commonpb.KeyValue) *resourcepb.Resource {
	return &resourcepb.Resource{Attributes: attrs}
}

func scope(name, version string, attrs ...*commonpb.KeyValue) *commonpb.InstrumentationScope {
	return &commonpb.InstrumentationScope{Name: name, Version: version, Attributes: attrs}
}

const testSchemaURL = "https://opentelemetry.io/schemas/1.4.0"

// tests -----------------------------------------------------------------

func TestMapRows_EmptyInput(t *testing.T) {
	out := MapRows(nil)
	if len(out.Series) != 0 || len(out.Gauge) != 0 || len(out.Sum) != 0 ||
		len(out.Histogram) != 0 || len(out.ExponentialHistogram) != 0 || len(out.Summary) != 0 {
		t.Fatalf("expected all slices empty, got %+v", out)
	}
}

func TestMapRows_Gauge(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	rm := []*metricspb.ResourceMetrics{{
		Resource:  resource(strKV("service.name", "svc-a"), strKV("host.name", "h1")),
		SchemaUrl: testSchemaURL,
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: scope("scope-a", "1.0.0", strKV("scope.attr", "x")),
			Metrics: []*metricspb.Metric{{
				Name: "cpu.utilization", Description: "cpu", Unit: "%",
				Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
					DataPoints: []*metricspb.NumberDataPoint{{
						Attributes:        []*commonpb.KeyValue{strKV("cpu", "0")},
						StartTimeUnixNano: now - 1e9, TimeUnixNano: now,
						Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 42.5},
						Flags: 7,
					}},
				}},
			}},
		}},
	}}

	out := MapRows(rm)

	if len(out.Gauge) != 1 || len(out.Series) != 1 {
		t.Fatalf("expected 1 gauge + 1 series row, got gauge=%d series=%d", len(out.Gauge), len(out.Series))
	}
	if len(out.Sum)+len(out.Histogram)+len(out.ExponentialHistogram)+len(out.Summary) != 0 {
		t.Fatalf("non-gauge slices must be empty for a Gauge-only input, got %+v", out)
	}

	g := out.Gauge[0]
	if g.Value != 42.5 || g.Flags != 7 {
		t.Fatalf("gauge value/flags wrong: %+v", g)
	}
	if g.TimeUnix.UnixNano() != int64(now) {
		t.Fatalf("TimeUnix mismatch: want %d got %d", now, g.TimeUnix.UnixNano())
	}

	s := out.Series[0]
	if s.SeriesID != g.SeriesID {
		t.Fatalf("series and datapoint SeriesID disagree: %d vs %d", s.SeriesID, g.SeriesID)
	}
	if s.MetricType != metricTypeGauge {
		t.Fatalf("MetricType: want %q got %q", metricTypeGauge, s.MetricType)
	}
	if s.ServiceName != "svc-a" || s.MetricName != "cpu.utilization" || s.MetricUnit != "%" {
		t.Fatalf("series metadata wrong: %+v", s)
	}
	if s.ResourceAttributes["host.name"] != "h1" || s.Attributes["cpu"] != "0" || s.ScopeAttributes["scope.attr"] != "x" {
		t.Fatalf("series attrs not propagated: %+v", s)
	}
	if s.FirstSeen.IsZero() || s.LastSeen.IsZero() {
		t.Fatalf("FirstSeen/LastSeen must be populated, got %+v / %+v", s.FirstSeen, s.LastSeen)
	}

	// Independently compute the expected SeriesID and confirm.
	want := computeSeriesID("cpu.utilization", metricTypeGauge,
		map[string]string{"service.name": "svc-a", "host.name": "h1"},
		testSchemaURL, "scope-a", "1.0.0",
		map[string]string{"cpu": "0"})
	if g.SeriesID != want {
		t.Fatalf("SeriesID mismatch: want %d got %d", want, g.SeriesID)
	}
}

func TestMapRows_Sum(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	rm := []*metricspb.ResourceMetrics{{
		Resource: resource(strKV("service.name", "svc-a")),
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: scope("scope", "1"),
			Metrics: []*metricspb.Metric{{
				Name: "http.requests.total",
				Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
					AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
					IsMonotonic:            true,
					DataPoints: []*metricspb.NumberDataPoint{{
						Attributes:   []*commonpb.KeyValue{strKV("method", "GET")},
						TimeUnixNano: now,
						Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 1234},
					}},
				}},
			}},
		}},
	}}

	out := MapRows(rm)
	if len(out.Sum) != 1 || len(out.Series) != 1 {
		t.Fatalf("expected 1 sum + 1 series, got sum=%d series=%d", len(out.Sum), len(out.Series))
	}
	s := out.Sum[0]
	if s.Value != 1234 {
		t.Fatalf("sum value (from AsInt): want 1234 got %v", s.Value)
	}
	if !s.IsMonotonic || s.AggregationTemporality != int32(metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE) {
		t.Fatalf("sum aggregation fields wrong: %+v", s)
	}
	if out.Series[0].MetricType != metricTypeSum {
		t.Fatalf("Series MetricType should be Sum, got %q", out.Series[0].MetricType)
	}
}

func TestMapRows_Histogram(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	rm := []*metricspb.ResourceMetrics{{
		Resource: resource(strKV("service.name", "svc")),
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: scope("s", "1"),
			Metrics: []*metricspb.Metric{{
				Name: "http.duration",
				Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
					AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
					DataPoints: []*metricspb.HistogramDataPoint{{
						TimeUnixNano:   now,
						Count:          10,
						Sum:            ptrFloat(123.4),
						Min:            ptrFloat(0.1),
						Max:            ptrFloat(50.0),
						BucketCounts:   []uint64{1, 4, 5},
						ExplicitBounds: []float64{1, 10},
					}},
				}},
			}},
		}},
	}}
	out := MapRows(rm)
	if len(out.Histogram) != 1 || len(out.Series) != 1 {
		t.Fatalf("expected 1 histogram + 1 series, got h=%d s=%d", len(out.Histogram), len(out.Series))
	}
	h := out.Histogram[0]
	if h.Count != 10 || h.Sum != 123.4 || h.Min != 0.1 || h.Max != 50.0 {
		t.Fatalf("histogram scalars wrong: %+v", h)
	}
	if len(h.BucketCounts) != 3 || len(h.ExplicitBounds) != 2 {
		t.Fatalf("bucket arrays wrong: %+v", h)
	}
	if h.AggregationTemporality != int32(metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA) {
		t.Fatalf("histogram temporality wrong: %d", h.AggregationTemporality)
	}
}

func TestMapRows_ExponentialHistogram(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	rm := []*metricspb.ResourceMetrics{{
		Resource: resource(strKV("service.name", "svc")),
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: scope("s", "1"),
			Metrics: []*metricspb.Metric{{
				Name: "rpc.duration",
				Data: &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
					AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
					DataPoints: []*metricspb.ExponentialHistogramDataPoint{{
						TimeUnixNano: now,
						Count:        100,
						Sum:          ptrFloat(9999),
						Scale:        3,
						ZeroCount:    5,
						Positive:     &metricspb.ExponentialHistogramDataPoint_Buckets{Offset: 1, BucketCounts: []uint64{2, 3, 4}},
						Negative:     &metricspb.ExponentialHistogramDataPoint_Buckets{Offset: -2, BucketCounts: []uint64{1}},
					}},
				}},
			}},
		}},
	}}
	out := MapRows(rm)
	if len(out.ExponentialHistogram) != 1 {
		t.Fatalf("expected 1 expo histogram, got %d", len(out.ExponentialHistogram))
	}
	e := out.ExponentialHistogram[0]
	if e.Count != 100 || e.Sum != 9999 || e.Scale != 3 || e.ZeroCount != 5 {
		t.Fatalf("expo scalars wrong: %+v", e)
	}
	if e.PositiveOffset != 1 || len(e.PositiveBucketCounts) != 3 {
		t.Fatalf("positive bucket fields wrong: %+v", e)
	}
	if e.NegativeOffset != -2 || len(e.NegativeBucketCounts) != 1 {
		t.Fatalf("negative bucket fields wrong: %+v", e)
	}
}

func TestMapRows_Summary(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	rm := []*metricspb.ResourceMetrics{{
		Resource: resource(strKV("service.name", "svc")),
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: scope("s", "1"),
			Metrics: []*metricspb.Metric{{
				Name: "rpc.latency",
				Data: &metricspb.Metric_Summary{Summary: &metricspb.Summary{
					DataPoints: []*metricspb.SummaryDataPoint{{
						TimeUnixNano: now,
						Count:        50,
						Sum:          1000,
						QuantileValues: []*metricspb.SummaryDataPoint_ValueAtQuantile{
							{Quantile: 0.5, Value: 10},
							{Quantile: 0.99, Value: 100},
						},
					}},
				}},
			}},
		}},
	}}
	out := MapRows(rm)
	if len(out.Summary) != 1 {
		t.Fatalf("expected 1 summary, got %d", len(out.Summary))
	}
	s := out.Summary[0]
	if s.Count != 50 || s.Sum != 1000 {
		t.Fatalf("summary scalars wrong: %+v", s)
	}
	if len(s.ValueAtQuantiles) != 2 || s.ValueAtQuantiles[1].Quantile != 0.99 || s.ValueAtQuantiles[1].Value != 100 {
		t.Fatalf("quantile values wrong: %+v", s.ValueAtQuantiles)
	}
}

func TestMapRows_SeriesIDStableAcrossDatapoints(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	rm := []*metricspb.ResourceMetrics{{
		Resource: resource(strKV("service.name", "svc")),
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: scope("s", "1"),
			Metrics: []*metricspb.Metric{{
				Name: "cpu.utilization",
				Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
					DataPoints: []*metricspb.NumberDataPoint{
						{Attributes: []*commonpb.KeyValue{strKV("cpu", "0")}, TimeUnixNano: now, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1}},
						{Attributes: []*commonpb.KeyValue{strKV("cpu", "0")}, TimeUnixNano: now + 1, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 2}},
						{Attributes: []*commonpb.KeyValue{strKV("cpu", "1")}, TimeUnixNano: now, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 3}},
					},
				}},
			}},
		}},
	}}
	out := MapRows(rm)
	if len(out.Gauge) != 3 || len(out.Series) != 3 {
		t.Fatalf("expected 3 gauge + 3 series rows, got g=%d s=%d", len(out.Gauge), len(out.Series))
	}
	// First two datapoints share attrs → same SeriesID.
	if out.Gauge[0].SeriesID != out.Gauge[1].SeriesID {
		t.Fatalf("same-attr datapoints must share SeriesID: %d vs %d", out.Gauge[0].SeriesID, out.Gauge[1].SeriesID)
	}
	// Third has different attrs → different SeriesID.
	if out.Gauge[0].SeriesID == out.Gauge[2].SeriesID {
		t.Fatalf("different-attr datapoints must have different SeriesIDs, got collision %d", out.Gauge[0].SeriesID)
	}
}

func TestMapRows_SameMetricNameDifferentTypeDiffer(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	rm := []*metricspb.ResourceMetrics{{
		Resource: resource(strKV("service.name", "svc")),
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: scope("s", "1"),
			Metrics: []*metricspb.Metric{
				{Name: "ops", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
					DataPoints: []*metricspb.NumberDataPoint{{TimeUnixNano: now, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1}}},
				}}},
				{Name: "ops", Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
					DataPoints: []*metricspb.NumberDataPoint{{TimeUnixNano: now, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 2}}},
				}}},
			},
		}},
	}}
	out := MapRows(rm)
	if len(out.Gauge) != 1 || len(out.Sum) != 1 {
		t.Fatalf("expected one Gauge + one Sum, got g=%d s=%d", len(out.Gauge), len(out.Sum))
	}
	if out.Gauge[0].SeriesID == out.Sum[0].SeriesID {
		t.Fatalf("same metric name across Gauge vs Sum must produce different SeriesIDs")
	}
}

func ptrFloat(f float64) *float64 { return &f }