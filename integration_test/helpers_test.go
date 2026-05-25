//go:build integration

package integration_test

import (
	"context"
	"log"
	"net"
	"testing"
	"time"

	"dash0.com/otlp-metric-store/internal/ingest"
	"dash0.com/otlp-metric-store/internal/storage"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func getStore(t *testing.T) *storage.ClickHouseMetricsStore {
	t.Helper()
	infraOnce.Do(startInfra)
	if infraErr != nil {
		t.Fatalf("infra setup failed: %v", infraErr)
	}
	truncateAll(t, chStore)
	return chStore
}

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

func getServer(t *testing.T) (colmetricspb.MetricsServiceClient, func()) {
	t.Helper()
	_ = getStore(t)

	producer := ingest.NewProducer([]string{rpBroker}, "otlp")

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(grpcServer, ingest.NewServer("bufconn", producer))
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
		_ = producer.Close()
	}

	return colmetricspb.NewMetricsServiceClient(conn), closer
}

func waitForRows(t *testing.T, store *storage.ClickHouseMetricsStore, countQuery string, want uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got uint64
	for time.Now().Before(deadline) {
		if err := store.Conn.QueryRow(context.Background(), countQuery).Scan(&got); err != nil {
			t.Fatalf("waitForRows: %v", err)
		}
		if got >= want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("waitForRows timed out after %s: query=%q want=%d got=%d", timeout, countQuery, want, got)
}