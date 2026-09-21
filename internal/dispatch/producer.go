package dispatch

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer publishes dispatch messages to Kafka. Used by the scheduler
// (cmd/scheduler) in place of the Phase 1 in-process dispatch call.
type Producer struct {
	client *kgo.Client
}

func NewProducer(brokers []string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		// Idempotent producer: safe to retry on transient errors without
		// risking duplicate dispatch messages from producer-side retries.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka producer init: %w", err)
	}
	return &Producer{client: client}, nil
}

// PublishDispatch sends a job to its type's dispatch topic. Synchronous —
// returns once the broker has acknowledged the write, so a scheduler crash
// right after this call can't silently lose the dispatch.
func (p *Producer) PublishDispatch(ctx context.Context, msg DispatchMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal dispatch message: %w", err)
	}

	record := &kgo.Record{
		Topic: DispatchTopic(msg.JobType),
		Key:   []byte(msg.JobID), // same job always lands on the same partition
		Value: body,
	}

	result := p.client.ProduceSync(ctx, record)
	return result.FirstErr()
}

// PublishRetry sends a job to the retry topic (for consumers that implement
// delayed redelivery) or is used as a stepping stone before Mongo's own
// scheduledAt-based retry (see jobs_repo.go MarkFailed) takes over. In this
// design Mongo already handles retry timing, so this is here for future use
// if you want a Kafka-native delay queue instead.
func (p *Producer) PublishRetry(ctx context.Context, msg DispatchMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal retry message: %w", err)
	}
	record := &kgo.Record{Topic: RetryTopic, Key: []byte(msg.JobID), Value: body}
	return p.client.ProduceSync(ctx, record).FirstErr()
}

// PublishDLQ sends a job that exhausted retries to the dead-letter topic.
func (p *Producer) PublishDLQ(ctx context.Context, msg DispatchMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal dlq message: %w", err)
	}
	record := &kgo.Record{Topic: DLQTopic, Key: []byte(msg.JobID), Value: body}
	return p.client.ProduceSync(ctx, record).FirstErr()
}

func (p *Producer) Close() {
	p.client.Close()
}
