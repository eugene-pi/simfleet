package controller_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/eugene-pi/simfleet/internal/controller"
	"github.com/eugene-pi/simfleet/internal/dbtest"
	"github.com/eugene-pi/simfleet/internal/store"
)

// fakePublisher records what was sent and can be told to fail, so tests can
// check both the happy path and what happens when Kafka is unavailable.
type testPublisher struct {
	mu   sync.Mutex
	sent []store.OutboxRow
	err  error // when set, PublishBatch fails
}

func (f *testPublisher) PublishBatch(ctx context.Context, rows []store.OutboxRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, rows...)
	return nil
}

func (f *testPublisher) Sent() []store.OutboxRow {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.sent)
}

func (f *testPublisher) EnsureTopic(ctx context.Context, topic string) error {
	return nil
}

// A failed publish must leave the rows unpublished so the next pass retries.
// Неудачная публикация должна оставить записи неопубликованными, чтобы
// следующий проход отправил их снова.
func TestPublishFailureKeepsRowsPending(t *testing.T) {
	ctx := context.Background()
	st, pool := dbtest.NewPostgresAndPool(t)
	pub := &testPublisher{err: errors.New("broker down")}
	c := controller.New(st, pub, time.Millisecond*10, 100)

	_ = dbtest.SeedExperimentWithJobs(t, pool, 3)

	// Pass fails; nothing is marked as published.
	published, err := c.PublishOutbox(ctx)
	if err == nil {
		t.Fatal("want error from reconcile")
	}
	if n := dbtest.CountUnpublished(t, pool); n != 3 {
		t.Errorf("unpublished = %d, want 3", n)
	}
	if published != 0 {
		t.Errorf("expected noting would be published, got %d", published)
	}

	// Broker recovers; the same rows go out.
	pub.err = nil
	published, err = c.PublishOutbox(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(pub.Sent()); got != 3 {
		t.Errorf("sent = %d, want 3", got)
	}
	if n := dbtest.CountUnpublished(t, pool); n != 0 {
		t.Errorf("unpublished = %d, want 0", n)
	}
	if published != 3 {
		t.Errorf("expected 3 would be published, got %d", published)
	}

}
