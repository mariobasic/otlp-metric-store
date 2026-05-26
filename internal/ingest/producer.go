package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/segmentio/kafka-go"
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
// Kafka message batch. rows must be a slice; nil or empty slices are no-ops.
func (p *Producer) Publish(ctx context.Context, topicSuffix string, rows any) error {
	v := reflect.ValueOf(rows)
	if !v.IsValid() || v.Kind() != reflect.Slice || v.Len() == 0 {
		return nil
	}
	msgs := make([]kafka.Message, v.Len())
	for i := range v.Len() {
		data, err := json.Marshal(v.Index(i).Interface())
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

