package bus

import (
	"context"
	"maps"
	"testing"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"
)

// These tests cover the offset bookkeeping only; nothing here talks to a
// broker, so the client is never created.
func newTestConsumer() *KafkaConsumer {
	return &KafkaConsumer{
		topic:   "t",
		pending: map[int32]int64{},
		owned:   map[int32]bool{},
	}
}

func rec(p int32, off int64) core.Record { return core.Record{Partition: p, Offset: off} }

func TestDoneTracksHighestOffsetPerPartition(t *testing.T) {
	c := newTestConsumer()
	c.onAssigned(context.Background(), nil, map[string][]int32{"t": {0, 1}})

	c.Done(rec(0, 5))
	c.Done(rec(0, 3)) // older record finishing late must not move the offset back
	c.Done(rec(1, 7))

	want := map[int32]int64{0: 5, 1: 7}
	if !maps.Equal(c.pending, want) {
		t.Errorf("pending = %v, want %v", c.pending, want)
	}
}

// A job can finish after its partition moved to another member. Committing
// it could overwrite the new owner's offset, so it is dropped.
func TestDoneIgnoresPartitionsNotOwned(t *testing.T) {
	c := newTestConsumer()
	c.onAssigned(context.Background(), nil, map[string][]int32{"t": {0, 1}})
	c.Done(rec(1, 4))

	c.onLost(context.Background(), nil, map[string][]int32{"t": {1}})
	c.Done(rec(1, 9))
	c.Done(rec(2, 1)) // never assigned
	c.Done(rec(0, 2))

	want := map[int32]int64{0: 2}
	if !maps.Equal(c.pending, want) {
		t.Errorf("pending = %v, want %v", c.pending, want)
	}
}

func TestOtherTopicsDoNotChangeOwnership(t *testing.T) {
	c := newTestConsumer()
	c.onAssigned(context.Background(), nil, map[string][]int32{"other": {0}})
	c.Done(rec(0, 1))

	if len(c.pending) != 0 {
		t.Errorf("pending = %v, want empty", c.pending)
	}
}

// The gauge counts partitions, not topics, and follows every way of losing one.
func TestPartitionsOwnedFollowsAssignment(t *testing.T) {
	ctx := context.Background()
	c := newTestConsumer()

	steps := []struct {
		name string
		do   func()
		want float64
	}{
		{"assigned", func() { c.onAssigned(ctx, nil, map[string][]int32{"t": {0, 1, 2}}) }, 3},
		{"assigned more", func() { c.onAssigned(ctx, nil, map[string][]int32{"t": {3}}) }, 4},
		{"assigned again", func() { c.onAssigned(ctx, nil, map[string][]int32{"t": {3}}) }, 4},
		{"other topic", func() { c.onAssigned(ctx, nil, map[string][]int32{"other": {7}}) }, 4},
		{"revoked", func() { c.onRevoked(ctx, nil, map[string][]int32{"t": {2, 3}}) }, 2},
		{"lost", func() { c.onLost(ctx, nil, map[string][]int32{"t": {1}}) }, 1},
	}
	for _, s := range steps {
		s.do()
		if got := testutil.ToFloat64(metrics.PartitionsOwned); got != s.want {
			t.Errorf("after %s: owned = %v, want %v", s.name, got, s.want)
		}
	}
}

func TestFailedCommitIsCountedAndKeptForRetry(t *testing.T) {
	// A client with no group fails the commit without any network call.
	cl, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	c := newTestConsumer()
	c.cl = cl
	c.onAssigned(context.Background(), nil, map[string][]int32{"t": {0}})
	c.Done(rec(0, 5))

	before := testutil.ToFloat64(metrics.CommitFailures)
	c.flush(context.Background())

	if got := testutil.ToFloat64(metrics.CommitFailures) - before; got != 1 {
		t.Errorf("commit failures rose by %v, want 1", got)
	}
	if want := (map[int32]int64{0: 5}); !maps.Equal(c.pending, want) {
		t.Errorf("pending = %v, want %v", c.pending, want)
	}
}
