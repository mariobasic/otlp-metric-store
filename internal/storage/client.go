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

// Close closes the underlying ClickHouse connection.
func (s *ClickHouseMetricsStore) Close() error {
	return s.Conn.Close()
}