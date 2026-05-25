//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"dash0.com/otlp-metric-store/internal/storage"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// One ClickHouse container is started lazily and shared across every test in
// this package. Tests get a clean slate by truncating tables in the helper
// (see helpers_test.go), which is roughly two orders of magnitude faster than
// spinning a fresh container per test.

var (
	chOnce      sync.Once
	chContainer testcontainers.Container
	chStore     *storage.ClickHouseMetricsStore
	chSetupErr  error
)

const (
	chImage    = "clickhouse/clickhouse-server:26.2"
	chDatabase = "default"
	chUsername = "default"
	chPassword = "test"
)

func TestMain(m *testing.M) {
	code := m.Run()
	// Best-effort cleanup. If startup failed, the container may already be gone.
	if chStore != nil {
		_ = chStore.Close()
	}
	if chContainer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := chContainer.Terminate(ctx); err != nil {
			log.Printf("terminating clickhouse container: %v", err)
		}
		cancel()
	}
	os.Exit(code)
}

// startClickHouse spins the container, opens a connection, and creates all
// tables. Called exactly once, the first time any test asks for a store.
func startClickHouse() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ctr, err := testcontainers.Run(ctx, chImage,
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithEnv(map[string]string{
			"CLICKHOUSE_USER":     chUsername,
			"CLICKHOUSE_PASSWORD": chPassword,
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("9000/tcp").WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		chSetupErr = fmt.Errorf("starting clickhouse container: %w", err)
		return
	}
	chContainer = ctr

	host, err := ctr.Host(ctx)
	if err != nil {
		chSetupErr = fmt.Errorf("getting container host: %w", err)
		return
	}
	mappedPort, err := ctr.MappedPort(ctx, "9000/tcp")
	if err != nil {
		chSetupErr = fmt.Errorf("getting mapped port: %w", err)
		return
	}

	addr := fmt.Sprintf("%s:%s", host, mappedPort.Port())
	store, err := storage.NewClickHouseMetricsStore(ctx, addr, chDatabase, chUsername, chPassword)
	if err != nil {
		chSetupErr = fmt.Errorf("opening store: %w", err)
		return
	}
	if err := store.CreateTables(ctx); err != nil {
		chSetupErr = fmt.Errorf("creating tables: %w", err)
		return
	}
	chStore = store
}