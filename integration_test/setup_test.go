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

	"dash0.com/otlp-metric-store/internal/ingest"
	"dash0.com/otlp-metric-store/internal/storage"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	infraOnce    sync.Once
	chContainer  testcontainers.Container
	rpContainer  *redpanda.Container
	testNet      *testcontainers.DockerNetwork
	chStore      *storage.ClickHouseMetricsStore
	rpBroker     string
	testProducer *ingest.Producer
	infraErr     error
)

const (
	chImage    = "clickhouse/clickhouse-server:26.2"
	chDatabase = "default"
	chUsername  = "default"
	chPassword = "test"
)

func TestMain(m *testing.M) {
	code := m.Run()
	if testProducer != nil {
		_ = testProducer.Close()
	}
	if chStore != nil {
		_ = chStore.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if chContainer != nil {
		if err := chContainer.Terminate(ctx); err != nil {
			log.Printf("terminating clickhouse container: %v", err)
		}
	}
	if rpContainer != nil {
		if err := rpContainer.Terminate(ctx); err != nil {
			log.Printf("terminating redpanda container: %v", err)
		}
	}
	if testNet != nil {
		if err := testNet.Remove(ctx); err != nil {
			log.Printf("removing test network: %v", err)
		}
	}
	os.Exit(code)
}

func startInfra() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	nw, err := network.New(ctx)
	if err != nil {
		infraErr = fmt.Errorf("creating docker network: %w", err)
		return
	}
	testNet = nw

	rpCtr, err := redpanda.Run(ctx, "redpandadata/redpanda:latest",
		network.WithNetwork([]string{}, nw),
		redpanda.WithListener("redpanda:29092"),
		redpanda.WithAutoCreateTopics(),
	)
	if err != nil {
		infraErr = fmt.Errorf("starting redpanda: %w", err)
		return
	}
	rpContainer = rpCtr

	broker, err := rpCtr.KafkaSeedBroker(ctx)
	if err != nil {
		infraErr = fmt.Errorf("getting redpanda broker: %w", err)
		return
	}
	rpBroker = broker

	chCtr, err := testcontainers.Run(ctx, chImage,
		network.WithNetwork([]string{"clickhouse"}, nw),
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
		infraErr = fmt.Errorf("starting clickhouse: %w", err)
		return
	}
	chContainer = chCtr

	host, err := chCtr.Host(ctx)
	if err != nil {
		infraErr = fmt.Errorf("getting container host: %w", err)
		return
	}
	mappedPort, err := chCtr.MappedPort(ctx, "9000/tcp")
	if err != nil {
		infraErr = fmt.Errorf("getting mapped port: %w", err)
		return
	}

	addr := fmt.Sprintf("%s:%s", host, mappedPort.Port())
	store, err := storage.NewClickHouseMetricsStore(ctx, addr, chDatabase, chUsername, chPassword)
	if err != nil {
		infraErr = fmt.Errorf("opening store: %w", err)
		return
	}
	if err := store.CreateTables(ctx); err != nil {
		infraErr = fmt.Errorf("creating tables: %w", err)
		return
	}
	if err := store.CreateKafkaTables(ctx, "redpanda:29092", "otlp"); err != nil {
		infraErr = fmt.Errorf("creating kafka tables: %w", err)
		return
	}
	chStore = store

	testProducer = ingest.NewProducer([]string{broker}, "otlp")
}