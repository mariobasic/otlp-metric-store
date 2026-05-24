//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"dash0.com/otlp-log-processor-backend/internal/ingest"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// builders --------------------------------------------------------------

func strKV(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

// gaugeRequest constructs a single-datapoint Gauge ExportMetricsServiceRequest.
func gaugeRequest(svcName, metricName string, dpAttrs []*commonpb.KeyValue, value float64, ts uint64) *colmetricspb.ExportMetricsServiceRequest {
	return &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource:  &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strKV("service.name", svcName)}},
			SchemaUrl: "https://opentelemetry.io/schemas/1.4.0",
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Scope: &commonpb.InstrumentationScope{Name: "test-scope", Version: "1.0.0"},
				Metrics: []*metricspb.Metric{{
					Name: metricName, Description: "desc", Unit: "1",
					Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
						DataPoints: []*metricspb.NumberDataPoint{{
							Attributes:   dpAttrs,
							TimeUnixNano: ts,
							Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: value},
						}},
					}},
				}},
			}},
		}},
	}
}

// tests -----------------------------------------------------------------

func TestCreateTables(t *testing.T) {
	store := getStore(t)
	ctx := context.Background()

	for _, table := range allTables {
		var count uint64
		err := store.Conn.QueryRow(ctx,
			"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = $1", table,
		).Scan(&count)
		if err != nil {
			t.Fatalf("querying system.tables for %s: %v", table, err)
		}
		if count != 1 {
			t.Errorf("expected table %s to exist, got count=%d", table, count)
		}
	}
}

func TestInsertSeries(t *testing.T) {
	store := getStore(t)
	ctx := context.Background()

	// Build via mapper so the SeriesID we assert against is the same one
	// production code would generate.
	rows := ingest.MapRows(gaugeRequest("svc-a", "cpu", []*commonpb.KeyValue{strKV("cpu", "0")}, 1.0, uint64(time.Now().UnixNano())).GetResourceMetrics())
	if len(rows.Series) != 1 {
		t.Fatalf("expected mapper to produce 1 series, got %d", len(rows.Series))
	}
	if err := store.InsertSeries(ctx, rows.Series); err != nil {
		t.Fatalf("InsertSeries: %v", err)
	}

	var (
		seriesID    uint64
		metricType  string
		serviceName string
		metricName  string
		metricUnit  string
		resAttrs    map[string]string
		attrs       map[string]string
	)
	if err := store.Conn.QueryRow(ctx,
		`SELECT SeriesID, MetricType, ServiceName, MetricName, MetricUnit, ResourceAttributes, Attributes
		   FROM otel_metric_series
		  WHERE MetricName = 'cpu'`,
	).Scan(&seriesID, &metricType, &serviceName, &metricName, &metricUnit, &resAttrs, &attrs); err != nil {
		t.Fatalf("querying series: %v", err)
	}

	if seriesID != rows.Series[0].SeriesID {
		t.Errorf("SeriesID mismatch: want %d got %d", rows.Series[0].SeriesID, seriesID)
	}
	if metricType != "Gauge" || serviceName != "svc-a" || metricUnit != "1" {
		t.Errorf("series metadata wrong: type=%q svc=%q unit=%q", metricType, serviceName, metricUnit)
	}
	if resAttrs["service.name"] != "svc-a" {
		t.Errorf("ResourceAttributes not persisted: %v", resAttrs)
	}
	if attrs["cpu"] != "0" {
		t.Errorf("datapoint Attributes not persisted: %v", attrs)
	}
}

func TestInsertGauge(t *testing.T) {
	store := getStore(t)
	ctx := context.Background()

	now := uint64(time.Now().UnixNano())
	rows := ingest.MapRows(gaugeRequest("svc-g", "cpu.utilization", []*commonpb.KeyValue{strKV("cpu", "0")}, 42.5, now).GetResourceMetrics())
	if err := store.InsertSeries(ctx, rows.Series); err != nil {
		t.Fatalf("InsertSeries: %v", err)
	}
	if err := store.InsertGauge(ctx, rows.Gauge); err != nil {
		t.Fatalf("InsertGauge: %v", err)
	}

	// Slim datapoints table — query via a join to the series catalogue to
	// recover the metadata.
	var (
		serviceName string
		metricName  string
		value       float64
	)
	if err := store.Conn.QueryRow(ctx,
		`SELECT s.ServiceName, s.MetricName, g.Value
		   FROM otel_metrics_gauge g
		   JOIN otel_metric_series s ON s.SeriesID = g.SeriesID
		  WHERE s.MetricName = 'cpu.utilization'`,
	).Scan(&serviceName, &metricName, &value); err != nil {
		t.Fatalf("querying gauge: %v", err)
	}
	if serviceName != "svc-g" || metricName != "cpu.utilization" || value != 42.5 {
		t.Errorf("gauge row wrong: svc=%q metric=%q val=%v", serviceName, metricName, value)
	}

	// Confirm the slim datapoints table has no metadata columns.
	var cnt uint64
	if err := store.Conn.QueryRow(ctx,
		`SELECT count() FROM system.columns
		  WHERE database = currentDatabase() AND table = 'otel_metrics_gauge' AND name = 'ServiceName'`,
	).Scan(&cnt); err != nil {
		t.Fatalf("checking columns: %v", err)
	}
	if cnt != 0 {
		t.Errorf("otel_metrics_gauge should NOT have a ServiceName column (metadata is in series table)")
	}
}

func TestInsertSum(t *testing.T) {
	store := getStore(t)
	ctx := context.Background()

	now := uint64(time.Now().UnixNano())
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strKV("service.name", "svc-s")}},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Scope: &commonpb.InstrumentationScope{Name: "scope", Version: "1"},
				Metrics: []*metricspb.Metric{{
					Name: "http.requests.total", Unit: "{request}",
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
		}},
	}
	rows := ingest.MapRows(req.GetResourceMetrics())
	if err := store.InsertSeries(ctx, rows.Series); err != nil {
		t.Fatalf("InsertSeries: %v", err)
	}
	if err := store.InsertSum(ctx, rows.Sum); err != nil {
		t.Fatalf("InsertSum: %v", err)
	}

	var (
		value       float64
		isMonotonic bool
		temporality int32
	)
	if err := store.Conn.QueryRow(ctx,
		`SELECT Value, IsMonotonic, AggregationTemporality FROM otel_metrics_sum
		  WHERE SeriesID = $1`, rows.Sum[0].SeriesID,
	).Scan(&value, &isMonotonic, &temporality); err != nil {
		t.Fatalf("querying sum: %v", err)
	}
	if value != 1234 || !isMonotonic || temporality != int32(metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE) {
		t.Errorf("sum row wrong: val=%v mono=%v temp=%d", value, isMonotonic, temporality)
	}
}

func TestInsertHistogram(t *testing.T) {
	store := getStore(t)
	ctx := context.Background()

	now := uint64(time.Now().UnixNano())
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strKV("service.name", "svc-h")}},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Scope: &commonpb.InstrumentationScope{Name: "scope", Version: "1"},
				Metrics: []*metricspb.Metric{{
					Name: "http.duration",
					Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
						AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
						DataPoints: []*metricspb.HistogramDataPoint{{
							TimeUnixNano: now,
							Count:        10, Sum: ptrFloat(123.4),
							Min: ptrFloat(0.1), Max: ptrFloat(50),
							BucketCounts:   []uint64{1, 4, 5},
							ExplicitBounds: []float64{1, 10},
						}},
					}},
				}},
			}},
		}},
	}
	rows := ingest.MapRows(req.GetResourceMetrics())
	if err := store.InsertSeries(ctx, rows.Series); err != nil {
		t.Fatalf("InsertSeries: %v", err)
	}
	if err := store.InsertHistogram(ctx, rows.Histogram); err != nil {
		t.Fatalf("InsertHistogram: %v", err)
	}

	var (
		count          uint64
		sum, min, max  float64
		bucketCounts   []uint64
		explicitBounds []float64
	)
	if err := store.Conn.QueryRow(ctx,
		`SELECT Count, Sum, Min, Max, BucketCounts, ExplicitBounds FROM otel_metrics_histogram
		  WHERE SeriesID = $1`, rows.Histogram[0].SeriesID,
	).Scan(&count, &sum, &min, &max, &bucketCounts, &explicitBounds); err != nil {
		t.Fatalf("querying histogram: %v", err)
	}
	if count != 10 || sum != 123.4 || min != 0.1 || max != 50 {
		t.Errorf("histogram scalars wrong: c=%d s=%v min=%v max=%v", count, sum, min, max)
	}
	if len(bucketCounts) != 3 || bucketCounts[1] != 4 {
		t.Errorf("BucketCounts wrong: %v", bucketCounts)
	}
	if len(explicitBounds) != 2 || explicitBounds[0] != 1 {
		t.Errorf("ExplicitBounds wrong: %v", explicitBounds)
	}
}

func TestInsertExponentialHistogram(t *testing.T) {
	store := getStore(t)
	ctx := context.Background()

	now := uint64(time.Now().UnixNano())
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strKV("service.name", "svc-eh")}},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Scope: &commonpb.InstrumentationScope{Name: "scope", Version: "1"},
				Metrics: []*metricspb.Metric{{
					Name: "rpc.duration",
					Data: &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
						AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
						DataPoints: []*metricspb.ExponentialHistogramDataPoint{{
							TimeUnixNano: now,
							Count:        100, Sum: ptrFloat(9999),
							Scale: 3, ZeroCount: 5,
							Positive: &metricspb.ExponentialHistogramDataPoint_Buckets{Offset: 1, BucketCounts: []uint64{2, 3, 4}},
							Negative: &metricspb.ExponentialHistogramDataPoint_Buckets{Offset: -2, BucketCounts: []uint64{1}},
						}},
					}},
				}},
			}},
		}},
	}
	rows := ingest.MapRows(req.GetResourceMetrics())
	if err := store.InsertSeries(ctx, rows.Series); err != nil {
		t.Fatalf("InsertSeries: %v", err)
	}
	if err := store.InsertExponentialHistogram(ctx, rows.ExponentialHistogram); err != nil {
		t.Fatalf("InsertExponentialHistogram: %v", err)
	}

	var (
		count                          uint64
		sum                            float64
		scale, posOffset, negOffset    int32
		zeroCount                      uint64
		posBucketCounts, negBuckCounts []uint64
	)
	if err := store.Conn.QueryRow(ctx,
		`SELECT Count, Sum, Scale, ZeroCount, PositiveOffset, PositiveBucketCounts, NegativeOffset, NegativeBucketCounts
		   FROM otel_metrics_exponential_histogram WHERE SeriesID = $1`, rows.ExponentialHistogram[0].SeriesID,
	).Scan(&count, &sum, &scale, &zeroCount, &posOffset, &posBucketCounts, &negOffset, &negBuckCounts); err != nil {
		t.Fatalf("querying exp histogram: %v", err)
	}
	if count != 100 || sum != 9999 || scale != 3 || zeroCount != 5 {
		t.Errorf("expo scalars wrong: c=%d s=%v scale=%d zc=%d", count, sum, scale, zeroCount)
	}
	if posOffset != 1 || len(posBucketCounts) != 3 || posBucketCounts[2] != 4 {
		t.Errorf("positive buckets wrong: offset=%d counts=%v", posOffset, posBucketCounts)
	}
	if negOffset != -2 || len(negBuckCounts) != 1 || negBuckCounts[0] != 1 {
		t.Errorf("negative buckets wrong: offset=%d counts=%v", negOffset, negBuckCounts)
	}
}

func TestInsertSummary(t *testing.T) {
	store := getStore(t)
	ctx := context.Background()

	now := uint64(time.Now().UnixNano())
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strKV("service.name", "svc-sm")}},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Scope: &commonpb.InstrumentationScope{Name: "scope", Version: "1"},
				Metrics: []*metricspb.Metric{{
					Name: "rpc.latency",
					Data: &metricspb.Metric_Summary{Summary: &metricspb.Summary{
						DataPoints: []*metricspb.SummaryDataPoint{{
							TimeUnixNano: now,
							Count:        50, Sum: 1000,
							QuantileValues: []*metricspb.SummaryDataPoint_ValueAtQuantile{
								{Quantile: 0.5, Value: 10},
								{Quantile: 0.99, Value: 100},
							},
						}},
					}},
				}},
			}},
		}},
	}
	rows := ingest.MapRows(req.GetResourceMetrics())
	if err := store.InsertSeries(ctx, rows.Series); err != nil {
		t.Fatalf("InsertSeries: %v", err)
	}
	if err := store.InsertSummary(ctx, rows.Summary); err != nil {
		t.Fatalf("InsertSummary: %v", err)
	}

	var (
		count            uint64
		sum              float64
		quantiles, vals  []float64
	)
	if err := store.Conn.QueryRow(ctx,
		`SELECT Count, Sum, ValueAtQuantiles.Quantile, ValueAtQuantiles.Value
		   FROM otel_metrics_summary WHERE SeriesID = $1`, rows.Summary[0].SeriesID,
	).Scan(&count, &sum, &quantiles, &vals); err != nil {
		t.Fatalf("querying summary: %v", err)
	}
	if count != 50 || sum != 1000 {
		t.Errorf("summary scalars wrong: c=%d s=%v", count, sum)
	}
	if len(quantiles) != 2 || quantiles[1] != 0.99 || vals[1] != 100 {
		t.Errorf("quantile arrays wrong: q=%v v=%v", quantiles, vals)
	}
}

func TestReferentialIntegrity(t *testing.T) {
	client, closer := getServer(t)
	defer closer()
	ctx := context.Background()
	store := chStore

	// Two distinct series (different cpu labels), 5 datapoints each.
	now := uint64(time.Now().UnixNano())
	for i := 0; i < 5; i++ {
		for _, cpu := range []string{"0", "1"} {
			req := gaugeRequest("svc-ri", "ri.gauge",
				[]*commonpb.KeyValue{strKV("cpu", cpu)},
				float64(i), now+uint64(i),
			)
			if _, err := client.Export(ctx, req); err != nil {
				t.Fatalf("Export i=%d cpu=%s: %v", i, cpu, err)
			}
		}
	}

	var seriesCount, gaugeCount, orphanCount uint64
	if err := store.Conn.QueryRow(ctx,
		`SELECT count() FROM otel_metric_series WHERE MetricName = 'ri.gauge'`,
	).Scan(&seriesCount); err != nil {
		t.Fatalf("counting series: %v", err)
	}
	if err := store.Conn.QueryRow(ctx,
		`SELECT count() FROM otel_metrics_gauge g JOIN otel_metric_series s ON s.SeriesID = g.SeriesID WHERE s.MetricName = 'ri.gauge'`,
	).Scan(&gaugeCount); err != nil {
		t.Fatalf("counting gauges: %v", err)
	}
	if err := store.Conn.QueryRow(ctx,
		`SELECT count() FROM otel_metrics_gauge g LEFT JOIN otel_metric_series s ON s.SeriesID = g.SeriesID WHERE s.SeriesID = 0`,
	).Scan(&orphanCount); err != nil {
		t.Fatalf("checking orphans: %v", err)
	}

	if seriesCount != 2 {
		t.Errorf("expected 2 series rows (cpu=0, cpu=1), got %d", seriesCount)
	}
	if gaugeCount != 10 {
		t.Errorf("expected 10 datapoints (5 per series x 2 series), got %d", gaugeCount)
	}
	if orphanCount != 0 {
		t.Errorf("expected 0 orphan datapoints, got %d", orphanCount)
	}
}

func TestSeriesDedup_GRPCPath(t *testing.T) {
	// SeriesCache in the running server should keep the catalogue at 1 row
	// even when the same series is sent many times.
	client, closer := getServer(t)
	defer closer()
	ctx := context.Background()
	store := chStore

	const n = 200
	now := uint64(time.Now().UnixNano())
	for i := 0; i < n; i++ {
		req := gaugeRequest("svc-dd", "dd.gauge",
			[]*commonpb.KeyValue{strKV("k", "v")},
			float64(i), now+uint64(i),
		)
		if _, err := client.Export(ctx, req); err != nil {
			t.Fatalf("Export iter %d: %v", i, err)
		}
	}

	var seriesCount, datapointCount uint64
	if err := store.Conn.QueryRow(ctx,
		`SELECT count() FROM otel_metric_series WHERE MetricName = 'dd.gauge'`,
	).Scan(&seriesCount); err != nil {
		t.Fatalf("counting series: %v", err)
	}
	if err := store.Conn.QueryRow(ctx,
		`SELECT count() FROM otel_metrics_gauge`,
	).Scan(&datapointCount); err != nil {
		t.Fatalf("counting datapoints: %v", err)
	}
	if seriesCount != 1 {
		t.Errorf("SeriesCache should keep catalogue at 1 row, got %d", seriesCount)
	}
	if datapointCount != n {
		t.Errorf("expected %d gauge datapoints, got %d", n, datapointCount)
	}
}

func TestSeriesDedup_ReplacingMergeTree(t *testing.T) {
	// Bypass the cache: write the same SeriesRow many times directly via the
	// store. After OPTIMIZE FINAL, ReplacingMergeTree(LastSeen) collapses
	// repeats to a single row.
	store := getStore(t)
	ctx := context.Background()

	now := uint64(time.Now().UnixNano())
	rows := ingest.MapRows(gaugeRequest("svc-mt", "mt.gauge",
		[]*commonpb.KeyValue{strKV("k", "v")},
		1.0, now,
	).GetResourceMetrics())
	if len(rows.Series) != 1 {
		t.Fatalf("expected mapper to produce 1 series, got %d", len(rows.Series))
	}

	for i := 0; i < 10; i++ {
		if err := store.InsertSeries(ctx, rows.Series); err != nil {
			t.Fatalf("InsertSeries iter %d: %v", i, err)
		}
	}

	if err := store.Conn.Exec(ctx, "OPTIMIZE TABLE otel_metric_series FINAL"); err != nil {
		t.Fatalf("OPTIMIZE: %v", err)
	}

	var cnt uint64
	if err := store.Conn.QueryRow(ctx,
		`SELECT count() FROM otel_metric_series WHERE MetricName = 'mt.gauge'`,
	).Scan(&cnt); err != nil {
		t.Fatalf("counting series: %v", err)
	}
	if cnt != 1 {
		t.Errorf("after OPTIMIZE FINAL, expected 1 catalogue row, got %d", cnt)
	}
}

func TestGRPCToClickHouse(t *testing.T) {
	client, closer := getServer(t)
	defer closer()
	ctx := context.Background()
	store := chStore

	now := uint64(time.Now().UnixNano())
	if _, err := client.Export(ctx, gaugeRequest("e2e-service", "e2e.gauge", nil, 99.9, now)); err != nil {
		t.Fatalf("exporting metrics via grpc: %v", err)
	}

	var (
		svcName    string
		metricName string
		value      float64
	)
	if err := store.Conn.QueryRow(ctx,
		`SELECT s.ServiceName, s.MetricName, g.Value
		   FROM otel_metrics_gauge g
		   JOIN otel_metric_series s ON s.SeriesID = g.SeriesID
		  WHERE s.MetricName = 'e2e.gauge'`,
	).Scan(&svcName, &metricName, &value); err != nil {
		t.Fatalf("querying e2e: %v", err)
	}
	if svcName != "e2e-service" || value != 99.9 {
		t.Errorf("e2e row wrong: svc=%q val=%v", svcName, value)
	}
}

// ptrFloat is a helper for the *float64 fields on histogram/exp histogram
// datapoints in the OTLP proto.
func ptrFloat(f float64) *float64 { return &f }