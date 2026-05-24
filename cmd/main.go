package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net"

	"dash0.com/otlp-log-processor-backend/internal/config"
	"dash0.com/otlp-log-processor-backend/internal/ingest"
	"dash0.com/otlp-log-processor-backend/internal/storage"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const name = "dash0.com/otlp-log-processor-backend"

var logger = otelslog.NewLogger(name)

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {
	ctx := context.Background()
	cfg := config.Load()

	slog.SetDefault(logger)
	logger.Info("Starting application")

	otelShutdown, err := setupOTelSDK(ctx)
	if err != nil {
		return
	}
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	store, err := storage.NewClickHouseMetricsStore(ctx, cfg.ClickHouse.Addr, cfg.ClickHouse.Database, cfg.ClickHouse.Username, cfg.ClickHouse.Password)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, store.Close())
	}()
	if err := store.CreateTables(ctx); err != nil {
		return err
	}

	cache, err := ingest.NewSeriesCache(cfg.SeriesCache.Size)
	if err != nil {
		return err
	}

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
	colmetricspb.RegisterMetricsServiceServer(grpcServer, ingest.NewServer(cfg.GRPC.ListenAddr, store, cache))

	slog.Debug("Starting gRPC server")

	return grpcServer.Serve(listener)
}