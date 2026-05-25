package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"dash0.com/otlp-metric-store/internal/storage"
)

// newHealthServer builds an http.Server that exposes GET /health.
// Returns 200 OK if it can ping ClickHouse, 503 otherwise. Used by liveness
// and readiness probes; deliberately cheap.
func newHealthServer(addr string, store *storage.ClickHouseMetricsStore) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := store.Conn.Ping(ctx); err != nil {
			slog.WarnContext(ctx, "health: clickhouse ping failed", "err", err)
			http.Error(w, "clickhouse unreachable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}