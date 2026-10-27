package bus

import (
	"context"
	"maps"
	"testing"

	"github.com/eugene-pi/simfleet/internal/core"
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
