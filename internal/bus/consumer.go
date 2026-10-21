package bus

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer reads job messages as a consumer group. Offsets are committed
// explicitly after processing: a crash before the commit means redelivery,
// which Claim absorbs, while committing early would silently lose work.
type KafkaConsumer struct {
	cl *kgo.Client
}

func NewKafkaConsumer(brokers []string, group string, topics []string) (*KafkaConsumer, error) {
	kafkaTopics := make([]string, len(topics))
	for i, topic := range topics {
		kafkaTopics[i] = jobTopic(topic)
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(kafkaTopics...),
		kgo.DisableAutoCommit(),
		// With no stored offset, start from the beginning: messages are work
		// to do, not events to observe.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Commit what is finished before partitions move to another member,
		// so a rebalance does not redeliver work we already completed.
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client,
			revoked map[string][]int32) {
			if err := cl.CommitUncommittedOffsets(ctx); err != nil {
				slog.Error("commit on revoke", "error", err)
			}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("new kafka consumer: %w", err)
	}
	return &KafkaConsumer{cl: cl}, nil
}

func (c *KafkaConsumer) Close() {
	c.cl.Close()
}

// Poll blocks until records arrive, the context is cancelled or the client is
// closed.
// Блокируется, пока не придут записи, не отменится контекст или не закроется
// клиент.
func (c *KafkaConsumer) Poll(ctx context.Context) (core.Batch, error) {
	fetches := c.cl.PollFetches(ctx)

	if fetches.IsClientClosed() {
		return core.Batch{Closed: true}, nil
	}
	if errs := fetches.Errors(); len(errs) > 0 {
		// Context cancellation surfaces here as an error; it is a shutdown,
		// not a failure.
		// Отмена контекста приходит сюда как ошибка; это остановка, а не сбой.
		if ctx.Err() != nil {
			return core.Batch{Closed: true}, nil
		}
		return core.Batch{}, fmt.Errorf("fetch from %s: %w",
			errs[0].Topic, errs[0].Err)
	}

	out := core.Batch{Records: make([]core.Record, 0, fetches.NumRecords())}
	iter := fetches.RecordIter()
	for !iter.Done() {
		r := iter.Next()
		out.Records = append(out.Records, core.Record{
			Partition: r.Partition,
			Offset:    r.Offset,
			Value:     r.Value,
		})
	}
	return out, nil
}

func (c *KafkaConsumer) Commit(ctx context.Context) error {
	if err := c.cl.CommitUncommittedOffsets(ctx); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}
	return nil
}
