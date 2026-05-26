//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"dash0.com/otlp-metric-store/internal/ingest"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// builders --------------------------------------------------------------

func strKV(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

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

	tables := append(allTables,
		"otel_metric_series_queue",
		"otel_metrics_gauge_queue",
		"otel_metrics_sum_queue",
		"otel_metrics_histogram_queue",
		"otel_metrics_exponential_histogram_queue",
		"otel_metrics_summary_queue",
		"otel_metric_series_mv",
		"otel_metrics_gauge_mv",
		"otel_metrics_sum_mv",
		"otel_metrics_histogram_mv",
		"otel_metrics_exponential_histogram_mv",
		"otel_metrics_summary_mv",
	)

	for _, table := range tables {
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

	rows := ingest.MapRows(ctx, gaugeRequest("svc-a", "cpu", []*commonpb.KeyValue{strKV("cpu", "0")}, 1.0, uint64(time.Now().UnixNano())).GetResourceMetrics())
	if len(rows.Series) != 1 {
		t.Fatalf("expected mapper to produce 1 series, got %d", len(rows.Series))
	}
	if err := testProducer.Publish(ctx, "series", rows.Series); err != nil {
		t.Fatalf("Publish series: %v", err)
	}

	waitForRows(t, store, "SELECT count() FROM otel_metric_series WHERE MetricName = 'cpu'", 1, 10*time.Second)

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
	rows := ingest.MapRows(ctx, gaugeRequest("svc-g", "cpu.utilization", []*commonpb.KeyValue{strKV("cpu", "0")}, 42.5, now).GetResourceMetrics())
	if err := testProducer.Publish(ctx, "series", rows.Series); err != nil {
		t.Fatalf("Publish series: %v", err)
	}
	if err := testProducer.Publish(ctx, "gauge", rows.Gauge); err != nil {
		t.Fatalf("Publish gauge: %v", err)
	}

	waitForRows(t, store, "SELECT count() FROM otel_metric_series WHERE MetricName = 'cpu.utilization'", 1, 10*time.Second)
	waitForRows(t, store, "SELECT count() FROM otel_metrics_gauge", 1, 10*time.Second)

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
	rows := ingest.MapRows(ctx, req.GetResourceMetrics())
	if err := testProducer.Publish(ctx, "series", rows.Series); err != nil {
		t.Fatalf("Publish series: %v", err)
	}
	if err := testProducer.Publish(ctx, "sum", rows.Sum); err != nil {
		t.Fatalf("Publish sum: %v", err)
	}

	waitForRows(t, store, "SELECT count() FROM otel_metrics_sum", 1, 10*time.Second)

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
							Count:        10, Sum: new(123.4),
							Min: new(0.1), Max: new(50.0),
							BucketCounts:   []uint64{1, 4, 5},
							ExplicitBounds: []float64{1, 10},
						}},
					}},
				}},
			}},
		}},
	}
	rows := ingest.MapRows(ctx, req.GetResourceMetrics())
	if err := testProducer.Publish(ctx, "series", rows.Series); err != nil {
		t.Fatalf("Publish series: %v", err)
	}
	if err := testProducer.Publish(ctx, "histogram", rows.Histogram); err != nil {
		t.Fatalf("Publish histogram: %v", err)
	}

	waitForRows(t, store, "SELECT count() FROM otel_metrics_histogram", 1, 10*time.Second)

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
							Count:        100, Sum: new(9999.0),
							Scale: 3, ZeroCount: 5,
							Positive: &metricspb.ExponentialHistogramDataPoint_Buckets{Offset: 1, BucketCounts: []uint64{2, 3, 4}},
							Negative: &metricspb.ExponentialHistogramDataPoint_Buckets{Offset: -2, BucketCounts: []uint64{1}},
						}},
					}},
				}},
			}},
		}},
	}
	rows := ingest.MapRows(ctx, req.GetResourceMetrics())
	if err := testProducer.Publish(ctx, "series", rows.Series); err != nil {
		t.Fatalf("Publish series: %v", err)
	}
	if err := testProducer.Publish(ctx, "exponential_histogram", rows.ExponentialHistogram); err != nil {
		t.Fatalf("Publish exp histogram: %v", err)
	}

	waitForRows(t, store, "SELECT count() FROM otel_metrics_exponential_histogram", 1, 10*time.Second)

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
	rows := ingest.MapRows(ctx, req.GetResourceMetrics())
	if err := testProducer.Publish(ctx, "series", rows.Series); err != nil {
		t.Fatalf("Publish series: %v", err)
	}
	if err := testProducer.Publish(ctx, "summary", rows.Summary); err != nil {
		t.Fatalf("Publish summary: %v", err)
	}

	waitForRows(t, store, "SELECT count() FROM otel_metrics_summary", 1, 10*time.Second)

	var (
		count           uint64
		sum             float64
		quantiles, vals []float64
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

	rows0 := ingest.MapRows(ctx, gaugeRequest("svc-ri", "ri.gauge",
		[]*commonpb.KeyValue{strKV("cpu", "0")}, 0, 1).GetResourceMetrics())
	rows1 := ingest.MapRows(ctx, gaugeRequest("svc-ri", "ri.gauge",
		[]*commonpb.KeyValue{strKV("cpu", "1")}, 0, 1).GetResourceMetrics())
	sid0, sid1 := rows0.Series[0].SeriesID, rows1.Series[0].SeriesID
	sidFilter := fmt.Sprintf("SeriesID IN (%d, %d)", sid0, sid1)

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

	waitForRows(t, store, "SELECT count() FROM otel_metrics_gauge WHERE "+sidFilter, 10, 15*time.Second)

	var seriesCount, gaugeCount, orphanCount uint64
	if err := store.Conn.QueryRow(ctx,
		"SELECT count() FROM otel_metric_series FINAL WHERE "+sidFilter,
	).Scan(&seriesCount); err != nil {
		t.Fatalf("counting series: %v", err)
	}
	if err := store.Conn.QueryRow(ctx,
		"SELECT count() FROM otel_metrics_gauge WHERE "+sidFilter,
	).Scan(&gaugeCount); err != nil {
		t.Fatalf("counting gauges: %v", err)
	}
	if err := store.Conn.QueryRow(ctx,
		"SELECT count() FROM otel_metrics_gauge WHERE "+sidFilter+
			" AND SeriesID NOT IN (SELECT SeriesID FROM otel_metric_series)",
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

func TestSeriesDedup_KafkaPath(t *testing.T) {
	client, closer := getServer(t)
	defer closer()
	ctx := context.Background()
	store := chStore

	sample := ingest.MapRows(ctx, gaugeRequest("svc-dd", "dd.gauge",
		[]*commonpb.KeyValue{strKV("k", "v")}, 0, 1).GetResourceMetrics())
	sid := sample.Series[0].SeriesID
	sidFilter := fmt.Sprintf("SeriesID = %d", sid)

	const n = 20
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

	waitForRows(t, store, "SELECT count() FROM otel_metrics_gauge WHERE "+sidFilter, uint64(n), 15*time.Second)

	// FINAL modifier applies ReplacingMergeTree dedup at query time — no forced merge needed.
	var seriesCount, datapointCount uint64
	if err := store.Conn.QueryRow(ctx,
		"SELECT count() FROM otel_metric_series FINAL WHERE "+sidFilter,
	).Scan(&seriesCount); err != nil {
		t.Fatalf("counting series: %v", err)
	}
	if err := store.Conn.QueryRow(ctx,
		"SELECT count() FROM otel_metrics_gauge WHERE "+sidFilter,
	).Scan(&datapointCount); err != nil {
		t.Fatalf("counting datapoints: %v", err)
	}
	if seriesCount != 1 {
		t.Errorf("expected 1 series row (FINAL dedup), got %d", seriesCount)
	}
	if datapointCount != n {
		t.Errorf("expected %d gauge datapoints, got %d", n, datapointCount)
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

	waitForRows(t, store, "SELECT count() FROM otel_metrics_gauge g JOIN otel_metric_series s ON s.SeriesID = g.SeriesID WHERE s.MetricName = 'e2e.gauge'", 1, 10*time.Second)

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
