package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/segmentio/kafka-go"

	"dash0.com/otlp-metric-store/internal/storage"
)

type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Producer publishes typed row structs to Kafka topics as JSONEachRow messages.
// One Writer per topic; Close flushes all in-flight messages.
type Producer struct {
	topicPrefix string
	writers     map[string]messageWriter
}

func NewProducer(brokers []string, topicPrefix string) *Producer {
	suffixes := []string{"series", "gauge", "sum", "histogram", "exponential_histogram", "summary"}
	writers := make(map[string]messageWriter, len(suffixes))
	for _, s := range suffixes {
		topic := topicPrefix + "." + s
		writers[topic] = &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		}
	}
	return &Producer{topicPrefix: topicPrefix, writers: writers}
}

// Publish marshals each element of the rows slice to JSON and sends them as a
// Kafka message batch. rows must be a slice of typed row structs; nil or empty
// slices are no-ops.
func (p *Producer) Publish(ctx context.Context, topicSuffix string, rows any) error {
	v := reflect.ValueOf(rows)
	if !v.IsValid() || v.Kind() != reflect.Slice || v.Len() == 0 {
		return nil
	}
	msgs := make([]kafka.Message, v.Len())
	for i := range v.Len() {
		data, err := encodeRow(v.Index(i).Interface())
		if err != nil {
			return fmt.Errorf("encoding row %d: %w", i, err)
		}
		msgs[i] = kafka.Message{Value: data}
	}
	topic := p.topicPrefix + "." + topicSuffix
	w, ok := p.writers[topic]
	if !ok {
		return fmt.Errorf("unknown topic: %s", topic)
	}
	return w.WriteMessages(ctx, msgs...)
}

func (p *Producer) Close() error {
	var errs []error
	for _, w := range p.writers {
		if err := w.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// encodeRow marshals a single row struct to JSON. Handles ClickHouse-specific
// encoding: time.Time → UnixNano (DateTime64) or Unix (DateTime), and
// SummaryQuantile slices → dotted-key arrays for Nested columns.
func encodeRow(row any) ([]byte, error) {
	m := make(map[string]any)
	flattenStruct(reflect.ValueOf(row), m)
	return json.Marshal(m)
}

func flattenStruct(rv reflect.Value, m map[string]any) {
	rt := rv.Type()
	for i := range rt.NumField() {
		field := rt.Field(i)
		val := rv.Field(i)

		if field.Anonymous {
			flattenStruct(val, m)
			continue
		}

		switch v := val.Interface().(type) {
		case time.Time:
			// String format avoids float64 precision loss for large int64
			// nanosecond timestamps. ClickHouse parses both formats natively.
			if field.Name == "FirstSeen" || field.Name == "LastSeen" {
				m[field.Name] = v.UTC().Format("2006-01-02 15:04:05")
			} else {
				m[field.Name] = v.UTC().Format("2006-01-02 15:04:05.000000000")
			}
		case []storage.SummaryQuantile:
			quantiles := make([]float64, len(v))
			values := make([]float64, len(v))
			for j, q := range v {
				quantiles[j] = q.Quantile
				values[j] = q.Value
			}
			m["ValueAtQuantiles.Quantile"] = quantiles
			m["ValueAtQuantiles.Value"] = values
		default:
			m[field.Name] = val.Interface()
		}
	}
}