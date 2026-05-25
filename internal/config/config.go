package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config bundles all runtime configuration for the service.
// Values come from environment variables; sensible defaults apply when unset.
type Config struct {
	ClickHouse  ClickHouseConfig
	GRPC        GRPCConfig
	Health      HealthConfig
	Kafka       KafkaConfig
	Batcher     BatcherConfig
	SeriesCache SeriesCacheConfig
}

type ClickHouseConfig struct {
	Addr     string
	Database string
	Username string
	Password string
}

type GRPCConfig struct {
	ListenAddr            string
	MaxReceiveMessageSize int
}

type HealthConfig struct {
	// ListenAddr for the HTTP /health endpoint. Separate from the gRPC port
	// so liveness/readiness probes don't fight the OTLP traffic. Default
	// matches the OTel collector convention.
	ListenAddr string
}

type KafkaConfig struct {
	Brokers     string // Go producer → Kafka broker (host-accessible address)
	CHBrokers   string // ClickHouse Kafka engine → broker (Docker-internal address)
	TopicPrefix string
}

type BatcherConfig struct {
	MaxSize    int
	FlushEvery time.Duration
}

type SeriesCacheConfig struct {
	Size int
}

// Load reads configuration from the process environment. It never errors —
// invalid values fall back to defaults so the service stays bootable.
func Load() Config {
	return Config{
		ClickHouse: ClickHouseConfig{
			Addr:     env("CLICKHOUSE_ADDR", "localhost:9000"),
			Database: env("CLICKHOUSE_DATABASE", "default"),
			Username: env("CLICKHOUSE_USERNAME", "default"),
			Password: env("CLICKHOUSE_PASSWORD", "clickhouse"),
		},
		GRPC: GRPCConfig{
			ListenAddr:            env("GRPC_LISTEN_ADDR", "localhost:4317"),
			MaxReceiveMessageSize: envInt("GRPC_MAX_RECEIVE_BYTES", 16_777_216),
		},
		Health: HealthConfig{
			ListenAddr: env("HEALTH_LISTEN_ADDR", "localhost:13133"),
		},
		Kafka: KafkaConfig{
			Brokers:     env("KAFKA_BROKERS", "localhost:9092"),
			CHBrokers:   env("KAFKA_CH_BROKERS", "redpanda:29092"),
			TopicPrefix: env("KAFKA_TOPIC_PREFIX", "otlp"),
		},
		Batcher: BatcherConfig{
			MaxSize:    envInt("BATCHER_MAX_SIZE", 10_000),
			FlushEvery: envDuration("BATCHER_FLUSH_EVERY", time.Second),
		},
		SeriesCache: SeriesCacheConfig{
			Size: envInt("SERIES_CACHE_SIZE", 100_000),
		},
	}
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: invalid int for %s=%q, using default %d: %v\n", key, v, fallback, err)
		return fallback
	}
	return n
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: invalid duration for %s=%q, using default %s: %v\n", key, v, fallback, err)
		return fallback
	}
	return d
}