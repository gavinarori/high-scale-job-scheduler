package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
)

// MessageHandler processes one dispatch message. Returning an error does
// NOT trigger a Kafka-level retry — the job's own retry/backoff lives in
// Mongo (see jobs_repo.go), so a handler error here just gets logged; the
// job itself was already marked failed/re-queued by the runner before this
// returns. Kafka's job here is purely "deliver the claim signal," not
// "guarantee job success."
type MessageHandler func(ctx context.Context, msg DispatchMessage) error

// Consumer wraps a Kafka consumer group for a single job type's dispatch
// topic. Run one Consumer per job type per executor pool — each pool
// scales independently based on that topic's lag.
type Consumer struct {
	client  *kgo.Client
	handler MessageHandler
}

// NewConsumer creates a consumer group member for the given job type.
// groupID should be shared across all executor instances handling this
// job type, e.g. "executor-<jobType>".
func NewConsumer(brokers []string, jobType, groupID string, handler MessageHandler) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(DispatchTopic(jobType)),
		// Commit offsets only after successful processing (handled in Run).
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer init: %w", err)
	}
	return &Consumer{client: client, handler: handler}, nil
}

// Run blocks, polling for messages until ctx is cancelled. Each message is
// processed synchronously and its offset committed only after the handler
// returns — if the process crashes mid-handling, the message is redelivered
// to another consumer in the group on rebalance, which is exactly the
// at-least-once semantic this system is designed around (idempotency keys
// and the reaper cover the rest).
func (c *Consumer) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println("consumer stopping")
			c.client.Close()
			return
		default:
		}

		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("fetch error on %s/%d: %v", topic, partition, err)
		})

		fetches.EachRecord(func(record *kgo.Record) {
			var msg DispatchMessage
			if err := json.Unmarshal(record.Value, &msg); err != nil {
				log.Printf("failed to unmarshal dispatch message: %v", err)
				return // commit anyway — a malformed message will never parse
			}
			if err := c.handler(ctx, msg); err != nil {
				log.Printf("handler error for job %s: %v", msg.JobID, err)
			}
		})

		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			log.Printf("commit offsets failed: %v", err)
		}
	}
}
