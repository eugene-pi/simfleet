package bus

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/eugene-pi/simfleet/internal/config"
	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	defaultCommitInterval = time.Second
)

// Consumer reads job messages as a consumer group. Offsets are committed
// explicitly after processing: a crash before the commit means redelivery,
// which Claim absorbs, while committing early would silently lose work.
type KafkaConsumer struct {
	cl      *kgo.Client
	topic   string
	mtx     sync.Mutex
	pending map[int32]int64
	stop    chan struct{}
	wg      sync.WaitGroup
}

func NewKafkaConsumer(cfg config.KafkaConfig) (*KafkaConsumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
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
	return &KafkaConsumer{
		cl:    cl,
		topic: cfg.Topic,
	}, nil
}

func (c *KafkaConsumer) Close() {
	close(c.stop)
	c.wg.Wait()
	c.cl.Close()
}

func (c *KafkaConsumer) commitLoop() {
	defer c.wg.Done()

	t := time.NewTicker(defaultCommitInterval)
	defer t.Stop()

	for {
		select {
		case <-c.stop:
			c.flush(context.Background())
			return
		case <-t.C:
			c.flush(context.Background())
		}
	}
}

// Poll blocks until records arrive, the context is cancelled or the client is
// closed.
func (c *KafkaConsumer) Poll(ctx context.Context) (core.Batch, error) {
	fetches := c.cl.PollFetches(ctx)

	if fetches.IsClientClosed() {
		return core.Batch{Closed: true}, nil
	}
	if errs := fetches.Errors(); len(errs) > 0 {
		// Context cancellation surfaces here as an error; it is a shutdown, not a failure.
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

func (c *KafkaConsumer) Done(rec core.Record) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if cur, ok := c.pending[rec.Partition]; !ok || rec.Offset > cur {
		c.pending[rec.Partition] = rec.Offset
	}
}

func (c *KafkaConsumer) Commit(ctx context.Context) error {
	if err := c.cl.CommitUncommittedOffsets(ctx); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}
	return nil
}

func (c *KafkaConsumer) CommitOffsets(ctx context.Context, offsets map[int32]int64) error {
	//c.cl.CommitOffsets()
	return nil
}

// flush commits the tracked offsets and clears the map. Kafka stores the
// offset of the NEXT record to read, hence the +1.
func (c *KafkaConsumer) flush(ctx context.Context) {
	c.mtx.Lock()
	if len(c.pending) == 0 {
		c.mtx.Unlock()
		return
	}
	offsets := map[string]map[int32]kgo.EpochOffset{c.topic: {}}
	for part, off := range c.pending {
		offsets[c.topic][part] = kgo.EpochOffset{Epoch: -1, Offset: off + 1}
	}
	clear(c.pending)
	c.mtx.Unlock()

	fn := func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest,
		_ *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			slog.Error("commit offsets failed", "error", err)
		}
	}
	c.cl.CommitOffsets(ctx, offsets, fn)
}
