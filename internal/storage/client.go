package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ClickHouseMetricsStore implements the metrics store backed by ClickHouse.
type ClickHouseMetricsStore struct {
	Conn driver.Conn // exported for integration tests (TRUNCATE + raw SELECT queries)
}

// NewClickHouseMetricsStore creates a new ClickHouseMetricsStore connected to the given address.
func NewClickHouseMetricsStore(ctx context.Context, addr string, database string, username string, password string) (*ClickHouseMetricsStore, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("opening clickhouse connection: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("pinging clickhouse: %w", err)
	}
	return &ClickHouseMetricsStore{Conn: conn}, nil
}

// CreateTables executes DDL for the series catalogue and all 5 datapoints tables.
// Series table goes first — datapoints reference it by SeriesID.
func (s *ClickHouseMetricsStore) CreateTables(ctx context.Context) error {
	ddls := []string{
		createSeriesTableSQL,
		createGaugeTableSQL,
		createSumTableSQL,
		createHistogramTableSQL,
		createExponentialHistogramTableSQL,
		createSummaryTableSQL,
	}
	for _, ddl := range ddls {
		if err := s.Conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("creating table: %w", err)
		}
	}
	return nil
}

// CreateKafkaTables creates the Kafka engine queue tables and materialized
// views that bridge rows from Kafka topics into the destination MergeTree
// tables. Must be called after CreateTables.
func (s *ClickHouseMetricsStore) CreateKafkaTables(ctx context.Context, chBrokers, topicPrefix string) error {
	for _, def := range kafkaTableDefs {
		if err := s.Conn.Exec(ctx, queueTableDDL(def, chBrokers, topicPrefix)); err != nil {
			return fmt.Errorf("creating kafka queue table %s: %w", def.queueTable, err)
		}
		if err := s.Conn.Exec(ctx, mvDDL(def)); err != nil {
			return fmt.Errorf("creating kafka MV for %s: %w", def.destTable, err)
		}
	}
	return nil
}

// InsertSeries batch-inserts catalogue rows into otel_metric_series.
// Repeats with the same SeriesID are collapsed by ReplacingMergeTree(LastSeen)
// during background merges — callers don't need to dedup before calling.
// Column order must match createSeriesTableSQL.
func (s *ClickHouseMetricsStore) InsertSeries(ctx context.Context, rows []SeriesRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := s.Conn.PrepareBatch(ctx, "INSERT INTO otel_metric_series")
	if err != nil {
		return fmt.Errorf("preparing series batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.SeriesID,
			r.MetricType,
			r.ServiceName,
			r.MetricName,
			r.MetricDescription,
			r.MetricUnit,
			r.ResourceAttributes,
			r.ResourceSchemaUrl,
			r.ScopeName,
			r.ScopeVersion,
			r.ScopeAttributes,
			r.ScopeDroppedAttrCount,
			r.ScopeSchemaUrl,
			r.Attributes,
			r.FirstSeen,
			r.LastSeen,
		); err != nil {
			return fmt.Errorf("appending series row: %w", err)
		}
	}
	return batch.Send()
}

// InsertGauge batch-inserts slim gauge datapoints into otel_metrics_gauge.
// Column order must match createGaugeTableSQL.
func (s *ClickHouseMetricsStore) InsertGauge(ctx context.Context, rows []GaugeDatapointRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := s.Conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_gauge")
	if err != nil {
		return fmt.Errorf("preparing gauge batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.SeriesID,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Value,
			r.Flags,
		); err != nil {
			return fmt.Errorf("appending gauge row: %w", err)
		}
	}
	return batch.Send()
}

// InsertSum batch-inserts slim sum datapoints into otel_metrics_sum.
// Column order must match createSumTableSQL.
func (s *ClickHouseMetricsStore) InsertSum(ctx context.Context, rows []SumDatapointRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := s.Conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_sum")
	if err != nil {
		return fmt.Errorf("preparing sum batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.SeriesID,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Value,
			r.Flags,
			r.AggregationTemporality,
			r.IsMonotonic,
		); err != nil {
			return fmt.Errorf("appending sum row: %w", err)
		}
	}
	return batch.Send()
}

// InsertHistogram batch-inserts histogram datapoints into otel_metrics_histogram.
// Column order must match createHistogramTableSQL.
func (s *ClickHouseMetricsStore) InsertHistogram(ctx context.Context, rows []HistogramDatapointRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := s.Conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_histogram")
	if err != nil {
		return fmt.Errorf("preparing histogram batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.SeriesID,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Count,
			r.Sum,
			r.BucketCounts,
			r.ExplicitBounds,
			r.Min,
			r.Max,
			r.Flags,
			r.AggregationTemporality,
		); err != nil {
			return fmt.Errorf("appending histogram row: %w", err)
		}
	}
	return batch.Send()
}

// InsertExponentialHistogram batch-inserts into otel_metrics_exponential_histogram.
// Column order must match createExponentialHistogramTableSQL.
func (s *ClickHouseMetricsStore) InsertExponentialHistogram(ctx context.Context, rows []ExponentialHistogramDatapointRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := s.Conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_exponential_histogram")
	if err != nil {
		return fmt.Errorf("preparing exponential histogram batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.SeriesID,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Count,
			r.Sum,
			r.Scale,
			r.ZeroCount,
			r.PositiveOffset,
			r.PositiveBucketCounts,
			r.NegativeOffset,
			r.NegativeBucketCounts,
			r.Min,
			r.Max,
			r.Flags,
			r.AggregationTemporality,
		); err != nil {
			return fmt.Errorf("appending exponential histogram row: %w", err)
		}
	}
	return batch.Send()
}

// InsertSummary batch-inserts summary datapoints into otel_metrics_summary.
// Column order must match createSummaryTableSQL. The Nested(Quantile, Value)
// column is passed as two parallel slices per row, in the same position as
// the Nested column declaration.
func (s *ClickHouseMetricsStore) InsertSummary(ctx context.Context, rows []SummaryDatapointRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := s.Conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_summary")
	if err != nil {
		return fmt.Errorf("preparing summary batch: %w", err)
	}
	for _, r := range rows {
		quantiles, values := splitQuantiles(r.ValueAtQuantiles)
		if err := batch.Append(
			r.SeriesID,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Count,
			r.Sum,
			quantiles,
			values,
			r.Flags,
		); err != nil {
			return fmt.Errorf("appending summary row: %w", err)
		}
	}
	return batch.Send()
}

// splitQuantiles flattens []SummaryQuantile into the two parallel arrays
// that clickhouse-go expects for a Nested column.
func splitQuantiles(qs []SummaryQuantile) ([]float64, []float64) {
	quantiles := make([]float64, len(qs))
	values := make([]float64, len(qs))
	for i, q := range qs {
		quantiles[i] = q.Quantile
		values[i] = q.Value
	}
	return quantiles, values
}

// Close closes the underlying ClickHouse connection.
func (s *ClickHouseMetricsStore) Close() error {
	return s.Conn.Close()
}