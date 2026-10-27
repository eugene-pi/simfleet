package worker

import (
	"context"
	"encoding/json"
	"slices"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/eugene-pi/simfleet/internal/config"
	"github.com/eugene-pi/simfleet/internal/core"
)

// fakeConsumer hands out one batch, then blocks until the context ends, the
// way a real consumer does when the topic is drained.
type fakeConsumer struct {
	mu     sync.Mutex
	batch  []core.Record
	served bool
	done   []int64 // offsets reported through Done, in order
}

func (f *fakeConsumer) Poll(ctx context.Context) (core.Batch, error) {
	f.mu.Lock()
	if !f.served {
		f.served = true
		b := f.batch
		f.mu.Unlock()
		return core.Batch{Records: b}, nil
	}
	f.mu.Unlock()
	<-ctx.Done()
	return core.Batch{Closed: true}, nil
}

func (f *fakeConsumer) Done(rec core.Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.done = append(f.done, rec.Offset)
}

func (f *fakeConsumer) reported() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.done)
}

// fakeExecutor calls hook for every job, so a test can stop the worker
// in the middle of one.
type fakeExecutor struct {
	hook func(core.JobID)
}

func (f *fakeExecutor) Execute(ctx context.Context, id core.JobID) (bool, error) {
	if f.hook != nil {
		f.hook(id)
	}
	return true, nil
}

func record(t *testing.T, offset int64, id core.JobID) core.Record {
	t.Helper()
	v, err := json.Marshal(core.JobMessage{JobID: id})
	if err != nil {
		t.Fatal(err)
	}
	return core.Record{Partition: 0, Offset: offset, Value: v}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

// Every handled record is reported back, in order — including a malformed
// one, which will never parse and must not block the partition.
func TestEveryHandledRecordIsDone(t *testing.T) {
	cons := &fakeConsumer{batch: []core.Record{
		record(t, 10, core.JobID(uuid.NewV7())),
		{Partition: 0, Offset: 11, Value: []byte("not json")},
		record(t, 12, core.JobID(uuid.NewV7())),
	}}
	w := New(cons, &fakeExecutor{}, config.WorkerConfig{BatchSize: 1})

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()

	waitFor(t, func() bool { return len(cons.reported()) == 3 })
	cancel()
	if err := <-errc; err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := cons.reported(); !slices.Equal(got, []int64{10, 11, 12}) {
		t.Errorf("done = %v, want [10 11 12]", got)
	}
}

// A record interrupted by shutdown is not reported: its job may still be
// queued, and only redelivery will get it run.
func TestRecordInterruptedByShutdownIsNotDone(t *testing.T) {
	first, second := core.JobID(uuid.NewV7()), core.JobID(uuid.NewV7())
	cons := &fakeConsumer{batch: []core.Record{
		record(t, 1, first),
		record(t, 2, second),
	}}

	ctx, cancel := context.WithCancel(context.Background())
	exec := &fakeExecutor{hook: func(id core.JobID) {
		if id == second {
			cancel() // SIGTERM arrives while the second job runs
		}
	}}
	w := New(cons, exec, config.WorkerConfig{BatchSize: 1})

	if err := w.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := cons.reported(); !slices.Equal(got, []int64{1}) {
		t.Errorf("done = %v, want [1]", got)
	}
}
