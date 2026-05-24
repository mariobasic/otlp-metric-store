package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"log/slog"
	"net"

	"dash0.com/otlp-log-processor-backend/internal/ingest"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	listenAddr            = flag.String("listenAddr", "localhost:4317", "The listen address")
	maxReceiveMessageSize = flag.Int("maxReceiveMessageSize", 16777216, "The max message size in bytes the server can receive")
)

const name = "dash0.com/otlp-log-processor-backend"

var logger = otelslog.NewLogger(name)

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {
	slog.SetDefault(logger)
	logger.Info("Starting application")

	otelShutdown, err := setupOTelSDK(context.Background())
	if err != nil {
		return
	}
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	flag.Parse()

	slog.Debug("Starting listener", slog.String("listenAddr", *listenAddr))
	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(*maxReceiveMessageSize),
		grpc.Creds(insecure.NewCredentials()),
	)
	colmetricspb.RegisterMetricsServiceServer(grpcServer, ingest.NewServer(*listenAddr, nil))

	slog.Debug("Starting gRPC server")

	return grpcServer.Serve(listener)
}