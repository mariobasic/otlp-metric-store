package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"dash0.com/otlp-metric-store/internal/config"
	"dash0.com/otlp-metric-store/internal/ingest"
	"dash0.com/otlp-metric-store/internal/storage"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const name = "dash0.com/otlp-metric-store"

var logger = otelslog.NewLogger(name)

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Load()

	otelShutdown, err := setupOTelSDK(context.Background())
	if err != nil {
		return
	}
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	store, err := storage.NewClickHouseMetricsStore(
		ctx,
		cfg.ClickHouse.Addr,
		cfg.ClickHouse.Database,
		cfg.ClickHouse.Username,
		cfg.ClickHouse.Password,
	)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, store.Close())
	}()
	if err := store.CreateTables(ctx); err != nil {
		return err
	}
	if err := store.CreateKafkaTables(ctx, cfg.Kafka.CHBrokers, cfg.Kafka.TopicPrefix); err != nil {
		return err
	}

	producer := ingest.NewProducer(
		strings.Split(cfg.Kafka.Brokers, ","),
		cfg.Kafka.TopicPrefix,
	)
	defer func() {
		err = errors.Join(err, producer.Close())
	}()

	healthSrv := newHealthServer(cfg.Health.ListenAddr, store)
	go func() {
		slog.Info("Starting health endpoint", "addr", cfg.Health.ListenAddr)
		if err := healthSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("health server crashed", "err", err)
		}
	}()

	slog.Debug("Starting listener", slog.String("listenAddr", cfg.GRPC.ListenAddr))
	listener, err := net.Listen("tcp", cfg.GRPC.ListenAddr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(cfg.GRPC.MaxReceiveMessageSize),
		grpc.Creds(insecure.NewCredentials()),
	)
	colmetricspb.RegisterMetricsServiceServer(grpcServer, ingest.NewServer(cfg.GRPC.ListenAddr, producer))

	slog.SetDefault(logger)
	slog.Info("Starting application")

	go func() {
		<-ctx.Done()
		slog.Info("Shutdown signal received; stopping servers")
		grpcServer.GracefulStop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = healthSrv.Shutdown(shutdownCtx)
	}()

	slog.Debug("Starting gRPC server")
	return grpcServer.Serve(listener)
}