package ingest

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"dash0.com/otlp-metric-store/internal/storage"
)

type mockWriter struct {
	mu       sync.Mutex
	messages []kafka.Message
	closed   bool
}

func (w *mockWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.mu.Lock()
	w.messages = append(w.messages, msgs...)
	w.mu.Unlock()
	return nil
}

func (w *mockWriter) Close() error {
	w.closed = true
	return nil
}

func newTestProducer(suffix string, mock *mockWriter) *Producer {
	topic := "otlp." + suffix
	return &Producer{
		topicPrefix: "otlp",
		writers:     map[string]messageWriter{topic: mock},
	}
}

func TestProducer_Publish_Gauge(t *testing.T) {
	mock := &mockWriter{}
	p := newTestProducer("gauge", mock)

	ts := storage.CHNanoTime(time.Date(2024, 1, 1, 12, 0, 0, 500, time.UTC))
	rows := []storage.GaugeDatapointRow{{
		BaseDatapointRow: storage.BaseDatapointRow{
			SeriesID: 123, StartTimeUnix: ts, TimeUnix: ts, Flags: 0,
		},
		Value: 42.5,
	}}

	if err := p.Publish(context.Background(), "gauge", rows); err != nil {
		t.Fatal(err)
	}
	if len(mock.messages) != 1 {
		t.Fatalf("want 1 message, got %d", len(mock.messages))
	}

	var m map[string]any
	if err := json.Unmarshal(mock.messages[0].Value, &m); err != nil {
		t.Fatal(err)
	}
	if got := m["SeriesID"].(float64); got != 123 {
		t.Errorf("SeriesID: want 123, got %v", got)
	}
	if got := m["Value"].(float64); got != 42.5 {
		t.Errorf("Value: want 42.5, got %v", got)
	}
	wantTime := "2024-01-01 12:00:00.000000500"
	if got := m["TimeUnix"].(string); got != wantTime {
		t.Errorf("TimeUnix: want %q, got %q", wantTime, got)
	}
}

func TestProducer_Publish_Summary_FlattensNested(t *testing.T) {
	mock := &mockWriter{}
	p := newTestProducer("summary", mock)

	ts := storage.CHNanoTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	rows := []storage.SummaryDatapointRow{{
		BaseDatapointRow: storage.BaseDatapointRow{
			SeriesID: 456, StartTimeUnix: ts, TimeUnix: ts,
		},
		Count: 10,
		Sum:   100.5,
		ValueAtQuantiles: []storage.SummaryQuantile{
			{Quantile: 0.5, Value: 50},
			{Quantile: 0.99, Value: 99},
		},
	}}

	if err := p.Publish(context.Background(), "summary", rows); err != nil {
		t.Fatal(err)
	}

	var m map[string]any
	if err := json.Unmarshal(mock.messages[0].Value, &m); err != nil {
		t.Fatal(err)
	}

	if _, ok := m["ValueAtQuantiles"]; ok {
		t.Error("ValueAtQuantiles should be flattened into dotted-key arrays")
	}
	quantiles := m["ValueAtQuantiles.Quantile"].([]any)
	values := m["ValueAtQuantiles.Value"].([]any)
	if len(quantiles) != 2 || quantiles[0].(float64) != 0.5 || quantiles[1].(float64) != 0.99 {
		t.Errorf("quantiles: want [0.5, 0.99], got %v", quantiles)
	}
	if len(values) != 2 || values[0].(float64) != 50 || values[1].(float64) != 99 {
		t.Errorf("values: want [50, 99], got %v", values)
	}
}

func TestProducer_Publish_Series_UsesUnixSeconds(t *testing.T) {
	mock := &mockWriter{}
	p := newTestProducer("series", mock)

	ts := storage.CHDateTime(time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC))
	rows := []storage.SeriesRow{{
		SeriesID:   789,
		MetricType: "Gauge",
		MetricName: "test",
		FirstSeen:  ts,
		LastSeen:   ts,
	}}

	if err := p.Publish(context.Background(), "series", rows); err != nil {
		t.Fatal(err)
	}

	var m map[string]any
	if err := json.Unmarshal(mock.messages[0].Value, &m); err != nil {
		t.Fatal(err)
	}
	wantTime := "2024-06-15 10:30:00"
	if got := m["FirstSeen"].(string); got != wantTime {
		t.Errorf("FirstSeen: want %q (DateTime format), got %q", wantTime, got)
	}
}

func TestProducer_Publish_Empty(t *testing.T) {
	mock := &mockWriter{}
	p := newTestProducer("gauge", mock)

	if err := p.Publish(context.Background(), "gauge", []storage.GaugeDatapointRow{}); err != nil {
		t.Fatal(err)
	}
	if err := p.Publish(context.Background(), "gauge", nil); err != nil {
		t.Fatal(err)
	}
	if len(mock.messages) != 0 {
		t.Fatalf("want 0 messages for empty/nil rows, got %d", len(mock.messages))
	}
}

func TestProducer_Close(t *testing.T) {
	mock := &mockWriter{}
	p := newTestProducer("gauge", mock)

	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if !mock.closed {
		t.Fatal("writer should be closed after Close()")
	}
}