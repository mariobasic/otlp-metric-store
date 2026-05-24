//go:build integration

package integration_test

import (
	"context"
	"log"
	"net"
	"testing"
	"time"

	"dash0.com/otlp-log-processor-backend/internal/config"
	"dash0.com/otlp-log-processor-backend/internal/ingest"
	"dash0.com/otlp-log-processor-backend/internal/storage"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// getStore returns the shared ClickHouse store and clears every table so each
// test starts from an empty database. Spins the container on first call.
func getStore(t *testing.T) *storage.ClickHouseMetricsStore {
	t.Helper()
	chOnce.Do(startClickHouse)
	if chSetupErr != nil {
		t.Fatalf("clickhouse setup failed: %v", chSetupErr)
	}
	truncateAll(t, chStore)
	return chStore
}

// allTables is the source of truth for "every table the store writes to".
// Used by truncateAll and TestCreateTables.
var allTables = []string{
	"otel_metric_series",
	"otel_metrics_gauge",
	"otel_metrics_sum",
	"otel_metrics_histogram",
	"otel_metrics_exponential_histogram",
	"otel_metrics_summary",
}

func truncateAll(t *testing.T, s *storage.ClickHouseMetricsStore) {
	t.Helper()
	ctx := context.Background()
	for _, table := range allTables {
		if err := s.Conn.Exec(ctx, "TRUNCATE TABLE IF EXISTS "+table); err != nil {
			t.Fatalf("truncating %s: %v", table, err)
		}
	}
}

// getServer wires the gRPC MetricsService over a bufconn with the shared
// store, a fresh SeriesCache, and a fresh Batcher. Returns the client, the
// batcher (so the test can Flush before querying), and a teardown func.
//
// Batcher uses MaxSize=1 + FlushEvery=20ms — effectively eager, so most
// inserts land in CH on the size trigger. Tests should still call
// batcher.Flush(ctx) before querying to be deterministic about timing.
func getServer(t *testing.T) (colmetricspb.MetricsServiceClient, *ingest.Batcher, func()) {
	t.Helper()
	store := getStore(t)

	cache, err := ingest.NewSeriesCache(1000)
	if err != nil {
		t.Fatalf("NewSeriesCache: %v", err)
	}

	batcherCtx, cancelBatcher := context.WithCancel(context.Background())
	batcher := ingest.NewBatcher(batcherCtx, store, config.BatcherConfig{
		MaxSize:    1,
		FlushEvery: 20 * time.Millisecond,
	})

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(grpcServer, ingest.NewServer("bufconn", batcher, cache))
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("error serving server: %v", err)
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dialing bufconn: %v", err)
	}

	closer := func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = lis.Close()
		cancelBatcher()
		<-batcher.Done()
	}

	return colmetricspb.NewMetricsServiceClient(conn), batcher, closer
}