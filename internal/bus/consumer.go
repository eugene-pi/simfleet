package bus

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"time"

	"github.com/eugene-pi/simfleet/internal/config"
	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	defaultCommitInterval = time.Second
)

// KafkaConsumer reads job messages as a consumer group. Offsets are committed
// explicitly after processing: a crash before the commit means redelivery,
// which Claim absorbs, while committing early would silently lose work.
//
// The worker reports finished records through Done; the consumer tracks the
// highest finished offset per partition and commits it on a ticker, on
// partition revoke and on Close. Offsets never leave this package.
type KafkaConsumer struct {
	cl    *kgo.Client
	topic string

	mtx     sync.Mutex
	pending map[int32]int64 // partition → last processed offset, not yet committed
	owned   map[int32]bool  // partitions currently assigned to this member

	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

func NewKafkaConsumer(cfg config.KafkaConfig) (*KafkaConsumer, error) {
	c := &KafkaConsumer{
		topic:   cfg.Topic,
		pending: map[int32]int64{},
		owned:   map[int32]bool{},
		stop:    make(chan struct{}),
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(),
		// With no stored offset, start from the beginning: messages are work
		// to do, not events to observe.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.OnPartitionsAssigned(c.onAssigned),
		kgo.OnPartitionsRevoked(c.onRevoked),
		kgo.OnPartitionsLost(c.onLost),
	)
	if err != nil {
		return nil, fmt.Errorf("new kafka consumer: %w", err)
	}
	c.cl = cl

	c.wg.Add(1)
	go c.commitLoop()
	return c, nil
}

// Close commits what is finished, then leaves the group. Safe to call twice.
func (c *KafkaConsumer) Close() {
	c.closeOnce.Do(func() {
		close(c.stop)
		c.wg.Wait() // commitLoop does the final flush
		c.cl.Close()
	})
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

// Done marks a record as processed. Records of a partition this member no
// longer owns are ignored: committing them could move the new owner's offset,
// and the redelivery they cause is absorbed by Claim.
func (c *KafkaConsumer) Done(rec core.Record) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if !c.owned[rec.Partition] {
		return
	}
	if cur, ok := c.pending[rec.Partition]; !ok || rec.Offset > cur {
		c.pending[rec.Partition] = rec.Offset
	}
}

func (c *KafkaConsumer) onAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	for _, p := range assigned[c.topic] {
		c.owned[p] = true
	}
}

// onRevoked commits what is finished before partitions move to another
// member, so a rebalance does not redeliver work we already completed.
func (c *KafkaConsumer) onRevoked(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
	c.flush(ctx)
	c.disown(revoked[c.topic])
}

// onLost drops tracked offsets without committing: the partitions already
// belong to someone else, and our generation can no longer commit anyway.
func (c *KafkaConsumer) onLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	c.disown(lost[c.topic])
}

func (c *KafkaConsumer) disown(parts []int32) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	for _, p := range parts {
		delete(c.owned, p)
		delete(c.pending, p)
	}
}

// flush commits the tracked offsets synchronously. Kafka stores the offset of
// the NEXT record to read, hence the +1. On failure the offsets go back into
// pending (unless a newer one arrived meanwhile), so the next flush retries.
func (c *KafkaConsumer) flush(ctx context.Context) {
	c.mtx.Lock()
	if len(c.pending) == 0 {
		c.mtx.Unlock()
		return
	}
	batch := maps.Clone(c.pending)
	clear(c.pending)
	c.mtx.Unlock()

	offsets := map[string]map[int32]kgo.EpochOffset{c.topic: {}}
	for part, off := range batch {
		offsets[c.topic][part] = kgo.EpochOffset{Epoch: -1, Offset: off + 1}
	}

	var commitErr error
	c.cl.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest,
		resp *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			commitErr = err
			return
		}
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if err := kerr.ErrorForCode(p.ErrorCode); err != nil && commitErr == nil {
					commitErr = fmt.Errorf("partition %d: %w", p.Partition, err)
				}
			}
		}
	})
	if commitErr == nil {
		return
	}

	slog.Error("commit offsets failed, will retry", "error", commitErr)
	c.mtx.Lock()
	defer c.mtx.Unlock()
	for part, off := range batch {
		if !c.owned[part] {
			continue
		}
		if cur, ok := c.pending[part]; !ok || off > cur {
			c.pending[part] = off
		}
	}
}
