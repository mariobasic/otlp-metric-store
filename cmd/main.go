package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"

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
	// SIGINT / SIGTERM cancel this ctx. The batcher's run-loop and the gRPC
	// server's graceful-stop both watch it for shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Load()

	slog.SetDefault(logger)
	logger.Info("Starting application")

	otelShutdown, err := setupOTelSDK(context.Background())
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

	batcher := ingest.NewBatcher(ctx, store, cfg.Batcher)

	// Health endpoint on its own HTTP listener — keeps liveness probes
	// independent of OTLP traffic on the gRPC port.
	healthSrv := newHealthServer(cfg.Health.ListenAddr, store)
	go func() {
		slog.Info("Starting health endpoint", "addr", cfg.Health.ListenAddr)
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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
	colmetricspb.RegisterMetricsServiceServer(grpcServer, ingest.NewServer(cfg.GRPC.ListenAddr, batcher, cache))

	// On signal: gracefully stop gRPC (drains in-flight requests) and the
	// health endpoint; the batcher loop drains via ctx cancellation.
	go func() {
		<-ctx.Done()
		slog.Info("Shutdown signal received; stopping servers")
		grpcServer.GracefulStop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = healthSrv.Shutdown(shutdownCtx)
	}()

	slog.Debug("Starting gRPC server")
	serveErr := grpcServer.Serve(listener)

	// After GracefulStop, the batcher's run-loop is heading to its final
	// drain (ctx is already cancelled). Wait for it so we don't lose buffered
	// rows on exit.
	<-batcher.Done()
	return serveErr
}