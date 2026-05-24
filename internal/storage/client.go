package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// errNotImplemented is returned by Insert paths that will be implemented in
// Phase 3. Returning an error rather than silently dropping data makes any
// accidental wire-up loud during integration testing.
var errNotImplemented = errors.New("storage: insert not implemented yet")

// ClickHouseMetricsStore implements the metrics store backed by ClickHouse.
type ClickHouseMetricsStore struct {
	Conn driver.Conn
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

// InsertGauge batch-inserts slim gauge datapoints into otel_metrics_gauge.
// Column order must match createGaugeTableSQL.
func (s *ClickHouseMetricsStore) InsertGauge(ctx context.Context, rows []GaugeDatapointRow) error {
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

// InsertSeries is a Phase 3 stub. The real implementation will batch-insert
// into otel_metric_series and rely on ReplacingMergeTree(LastSeen) for dedup.
func (s *ClickHouseMetricsStore) InsertSeries(ctx context.Context, rows []SeriesRow) error {
	if len(rows) == 0 {
		return nil
	}
	return errNotImplemented
}

// InsertHistogram is a Phase 3 stub.
func (s *ClickHouseMetricsStore) InsertHistogram(ctx context.Context, rows []HistogramDatapointRow) error {
	if len(rows) == 0 {
		return nil
	}
	return errNotImplemented
}

// InsertExponentialHistogram is a Phase 3 stub.
func (s *ClickHouseMetricsStore) InsertExponentialHistogram(ctx context.Context, rows []ExponentialHistogramDatapointRow) error {
	if len(rows) == 0 {
		return nil
	}
	return errNotImplemented
}

// InsertSummary is a Phase 3 stub.
func (s *ClickHouseMetricsStore) InsertSummary(ctx context.Context, rows []SummaryDatapointRow) error {
	if len(rows) == 0 {
		return nil
	}
	return errNotImplemented
}

// Close closes the underlying ClickHouse connection.
func (s *ClickHouseMetricsStore) Close() error {
	return s.Conn.Close()
}