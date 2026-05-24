package ingest

import (
	"fmt"
	"time"

	"dash0.com/otlp-log-processor-backend/internal/storage"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// MetricType labels stored in otel_metric_series.MetricType. Included in the
// SeriesID hash so two metrics with the same name but different types (e.g.
// `http.requests` Gauge vs Sum) get distinct SeriesIDs.
const (
	metricTypeGauge                = "Gauge"
	metricTypeSum                  = "Sum"
	metricTypeHistogram            = "Histogram"
	metricTypeExponentialHistogram = "ExponentialHistogram"
	metricTypeSummary              = "Summary"
)

// MappedRows is the result of a single walk over an ExportMetricsServiceRequest.
// Each slice corresponds to one ClickHouse table. Adding a new metric type =
// adding a new field here; no other signature changes propagate.
type MappedRows struct {
	Series               []storage.SeriesRow
	Gauge                []storage.GaugeDatapointRow
	Sum                  []storage.SumDatapointRow
	Histogram            []storage.HistogramDatapointRow
	ExponentialHistogram []storage.ExponentialHistogramDatapointRow
	Summary              []storage.SummaryDatapointRow
}

// MapRows walks the OTLP ResourceMetrics tree once and emits typed rows for
// every datapoint. SeriesID is computed inline per datapoint; the matching
// SeriesRow is also emitted. Series dedup is the SeriesCache's job (Phase 3)
// — the mapper always emits one SeriesRow per datapoint and lets later layers
// collapse duplicates.
func MapRows(resourceMetrics []*metricspb.ResourceMetrics) MappedRows {
	var out MappedRows
	now := time.Now()

	for _, rm := range resourceMetrics {
		svcName := serviceName(rm.GetResource())
		resAttrs := kvToMap(rm.GetResource().GetAttributes())
		resSchemaURL := rm.GetSchemaUrl()

		for _, sm := range rm.GetScopeMetrics() {
			scope := sm.GetScope()
			scopeAttrs := kvToMap(scope.GetAttributes())
			scopeName := scope.GetName()
			scopeVersion := scope.GetVersion()
			scopeSchemaURL := sm.GetSchemaUrl()

			for _, m := range sm.GetMetrics() {
				metricName := m.GetName()
				metricDesc := m.GetDescription()
				metricUnit := m.GetUnit()

				switch data := m.GetData().(type) {

				case *metricspb.Metric_Gauge:
					for _, dp := range data.Gauge.GetDataPoints() {
						dpAttrs := kvToMap(dp.GetAttributes())
						id := computeSeriesID(metricName, metricTypeGauge, resAttrs, resSchemaURL, scopeName, scopeVersion, dpAttrs)
						out.Series = append(out.Series, newSeriesRow(id, metricTypeGauge, svcName, metricName, metricDesc, metricUnit,
							resAttrs, resSchemaURL, scopeName, scopeVersion, scopeAttrs, scope.GetDroppedAttributesCount(), scopeSchemaURL, dpAttrs, now))
						out.Gauge = append(out.Gauge, storage.GaugeDatapointRow{
							BaseDatapointRow: storage.BaseDatapointRow{
								SeriesID:      id,
								StartTimeUnix: nanosToTime(dp.GetStartTimeUnixNano()),
								TimeUnix:      nanosToTime(dp.GetTimeUnixNano()),
								Flags:         dp.GetFlags(),
							},
							Value: numberDataPointValue(dp),
						})
					}

				case *metricspb.Metric_Sum:
					for _, dp := range data.Sum.GetDataPoints() {
						dpAttrs := kvToMap(dp.GetAttributes())
						id := computeSeriesID(metricName, metricTypeSum, resAttrs, resSchemaURL, scopeName, scopeVersion, dpAttrs)
						out.Series = append(out.Series, newSeriesRow(id, metricTypeSum, svcName, metricName, metricDesc, metricUnit,
							resAttrs, resSchemaURL, scopeName, scopeVersion, scopeAttrs, scope.GetDroppedAttributesCount(), scopeSchemaURL, dpAttrs, now))
						out.Sum = append(out.Sum, storage.SumDatapointRow{
							GaugeDatapointRow: storage.GaugeDatapointRow{
								BaseDatapointRow: storage.BaseDatapointRow{
									SeriesID:      id,
									StartTimeUnix: nanosToTime(dp.GetStartTimeUnixNano()),
									TimeUnix:      nanosToTime(dp.GetTimeUnixNano()),
									Flags:         dp.GetFlags(),
								},
								Value: numberDataPointValue(dp),
							},
							AggregationTemporality: int32(data.Sum.GetAggregationTemporality()),
							IsMonotonic:            data.Sum.GetIsMonotonic(),
						})
					}

				case *metricspb.Metric_Histogram:
					for _, dp := range data.Histogram.GetDataPoints() {
						dpAttrs := kvToMap(dp.GetAttributes())
						id := computeSeriesID(metricName, metricTypeHistogram, resAttrs, resSchemaURL, scopeName, scopeVersion, dpAttrs)
						out.Series = append(out.Series, newSeriesRow(id, metricTypeHistogram, svcName, metricName, metricDesc, metricUnit,
							resAttrs, resSchemaURL, scopeName, scopeVersion, scopeAttrs, scope.GetDroppedAttributesCount(), scopeSchemaURL, dpAttrs, now))
						out.Histogram = append(out.Histogram, storage.HistogramDatapointRow{
							BaseDatapointRow: storage.BaseDatapointRow{
								SeriesID:      id,
								StartTimeUnix: nanosToTime(dp.GetStartTimeUnixNano()),
								TimeUnix:      nanosToTime(dp.GetTimeUnixNano()),
								Flags:         dp.GetFlags(),
							},
							Count:                  dp.GetCount(),
							Sum:                    dp.GetSum(),
							BucketCounts:           dp.GetBucketCounts(),
							ExplicitBounds:         dp.GetExplicitBounds(),
							Min:                    dp.GetMin(),
							Max:                    dp.GetMax(),
							AggregationTemporality: int32(data.Histogram.GetAggregationTemporality()),
						})
					}

				case *metricspb.Metric_ExponentialHistogram:
					for _, dp := range data.ExponentialHistogram.GetDataPoints() {
						dpAttrs := kvToMap(dp.GetAttributes())
						id := computeSeriesID(metricName, metricTypeExponentialHistogram, resAttrs, resSchemaURL, scopeName, scopeVersion, dpAttrs)
						out.Series = append(out.Series, newSeriesRow(id, metricTypeExponentialHistogram, svcName, metricName, metricDesc, metricUnit,
							resAttrs, resSchemaURL, scopeName, scopeVersion, scopeAttrs, scope.GetDroppedAttributesCount(), scopeSchemaURL, dpAttrs, now))
						pos := dp.GetPositive()
						neg := dp.GetNegative()
						out.ExponentialHistogram = append(out.ExponentialHistogram, storage.ExponentialHistogramDatapointRow{
							BaseDatapointRow: storage.BaseDatapointRow{
								SeriesID:      id,
								StartTimeUnix: nanosToTime(dp.GetStartTimeUnixNano()),
								TimeUnix:      nanosToTime(dp.GetTimeUnixNano()),
								Flags:         dp.GetFlags(),
							},
							Count:                  dp.GetCount(),
							Sum:                    dp.GetSum(),
							Scale:                  dp.GetScale(),
							ZeroCount:              dp.GetZeroCount(),
							PositiveOffset:         pos.GetOffset(),
							PositiveBucketCounts:   pos.GetBucketCounts(),
							NegativeOffset:         neg.GetOffset(),
							NegativeBucketCounts:   neg.GetBucketCounts(),
							Min:                    dp.GetMin(),
							Max:                    dp.GetMax(),
							AggregationTemporality: int32(data.ExponentialHistogram.GetAggregationTemporality()),
						})
					}

				case *metricspb.Metric_Summary:
					for _, dp := range data.Summary.GetDataPoints() {
						dpAttrs := kvToMap(dp.GetAttributes())
						id := computeSeriesID(metricName, metricTypeSummary, resAttrs, resSchemaURL, scopeName, scopeVersion, dpAttrs)
						out.Series = append(out.Series, newSeriesRow(id, metricTypeSummary, svcName, metricName, metricDesc, metricUnit,
							resAttrs, resSchemaURL, scopeName, scopeVersion, scopeAttrs, scope.GetDroppedAttributesCount(), scopeSchemaURL, dpAttrs, now))
						out.Summary = append(out.Summary, storage.SummaryDatapointRow{
							BaseDatapointRow: storage.BaseDatapointRow{
								SeriesID:      id,
								StartTimeUnix: nanosToTime(dp.GetStartTimeUnixNano()),
								TimeUnix:      nanosToTime(dp.GetTimeUnixNano()),
								Flags:         dp.GetFlags(),
							},
							Count:            dp.GetCount(),
							Sum:              dp.GetSum(),
							ValueAtQuantiles: quantileValues(dp.GetQuantileValues()),
						})
					}
				}
			}
		}
	}
	return out
}

// newSeriesRow centralises SeriesRow construction so each metric-type arm
// stays compact.
func newSeriesRow(
	id uint64, metricType, svcName, metricName, metricDesc, metricUnit string,
	resAttrs map[string]string, resSchemaURL string,
	scopeName, scopeVersion string, scopeAttrs map[string]string, scopeDropped uint32, scopeSchemaURL string,
	dpAttrs map[string]string, now time.Time,
) storage.SeriesRow {
	return storage.SeriesRow{
		SeriesID:              id,
		MetricType:            metricType,
		ServiceName:           svcName,
		MetricName:            metricName,
		MetricDescription:     metricDesc,
		MetricUnit:            metricUnit,
		ResourceAttributes:    resAttrs,
		ResourceSchemaUrl:     resSchemaURL,
		ScopeName:             scopeName,
		ScopeVersion:          scopeVersion,
		ScopeAttributes:       scopeAttrs,
		ScopeDroppedAttrCount: scopeDropped,
		ScopeSchemaUrl:        scopeSchemaURL,
		Attributes:            dpAttrs,
		FirstSeen:             now,
		LastSeen:              now,
	}
}

func quantileValues(qvs []*metricspb.SummaryDataPoint_ValueAtQuantile) []storage.SummaryQuantile {
	if len(qvs) == 0 {
		return nil
	}
	out := make([]storage.SummaryQuantile, len(qvs))
	for i, q := range qvs {
		out[i] = storage.SummaryQuantile{Quantile: q.GetQuantile(), Value: q.GetValue()}
	}
	return out
}

// serviceName extracts the service.name from resource attributes, returning "" if not found.
func serviceName(resource *resourcepb.Resource) string {
	if resource == nil {
		return ""
	}
	for _, attr := range resource.GetAttributes() {
		if attr.GetKey() == "service.name" {
			return attr.GetValue().GetStringValue()
		}
	}
	return ""
}

// kvToMap converts a slice of OTLP KeyValue pairs to a Go map.
func kvToMap(attrs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, kv := range attrs {
		m[kv.GetKey()] = anyValueToString(kv.GetValue())
	}
	return m
}

// anyValueToString converts an OTLP AnyValue to its string representation.
func anyValueToString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return v.GetStringValue()
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", v.GetIntValue())
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", v.GetDoubleValue())
	case *commonpb.AnyValue_BoolValue:
		return fmt.Sprintf("%t", v.GetBoolValue())
	default:
		return fmt.Sprintf("%v", v)
	}
}

// nanosToTime converts a uint64 nanoseconds-since-epoch to time.Time.
func nanosToTime(nanos uint64) time.Time {
	return time.Unix(0, int64(nanos))
}

// numberDataPointValue extracts the float64 value from a NumberDataPoint.
func numberDataPointValue(dp *metricspb.NumberDataPoint) float64 {
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	default:
		return 0
	}
}