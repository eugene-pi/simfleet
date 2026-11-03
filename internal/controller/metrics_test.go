package controller_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/eugene-pi/simfleet/internal/controller"
	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/metrics"
	"github.com/eugene-pi/simfleet/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

// fakeStore answers the calls these tests exercise; anything else panics on
// the nil embedded interface.
type fakeStore struct {
	controller.Store
	expired      []store.ExpiredJob
	exhausted    int
	reconcileErr error
	publishErr   error

	// When block is set, both loops report in and wait for ctx to end, so a
	// test can cancel while a pass of each is in flight.
	block       bool
	publishing  sync.Once
	reconciling sync.Once
	inPublish   chan struct{}
	inReconcile chan struct{}
}

func newBlockingStore() *fakeStore {
	return &fakeStore{block: true, inPublish: make(chan struct{}), inReconcile: make(chan struct{})}
}

func (f *fakeStore) ReclaimExpired(ctx context.Context, _ int) ([]store.ExpiredJob, error) {
	if f.block {
		f.reconciling.Do(func() { close(f.inReconcile) })
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.expired, f.reconcileErr
}

func (f *fakeStore) FailExhausted(context.Context, int) (int, error) { return f.exhausted, nil }

func (f *fakeStore) UnscoredResults(context.Context, int) ([]core.PendingEvaluation, error) {
	return nil, nil
}

func (f *fakeStore) FinalizeCompleted(context.Context) (int, error) { return 0, nil }

func (f *fakeStore) FetchUnpublished(ctx context.Context, _ int) ([]store.OutboxRow, pgx.Tx, error) {
	if f.block {
		f.publishing.Do(func() { close(f.inPublish) })
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	return nil, nil, f.publishErr
}

func loopErrors(loop string) float64 {
	return testutil.ToFloat64(metrics.LoopErrors.WithLabelValues(loop))
}

func reconcilePasses(t *testing.T) uint64 {
	t.Helper()
	var m dto.Metric
	if err := metrics.ReconcileDuration.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetHistogram().GetSampleCount()
}

func TestReconcileCountsWhatItRetired(t *testing.T) {
	st := &fakeStore{expired: make([]store.ExpiredJob, 2), exhausted: 3}
	c := controller.New(st, &testPublisher{}, time.Hour, 10)

	expired := testutil.ToFloat64(metrics.LeasesExpired)
	exhausted := testutil.ToFloat64(metrics.AttemptsExhausted)

	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got := testutil.ToFloat64(metrics.LeasesExpired) - expired; got != 2 {
		t.Errorf("leases expired rose by %v, want 2", got)
	}
	if got := testutil.ToFloat64(metrics.AttemptsExhausted) - exhausted; got != 3 {
		t.Errorf("attempts exhausted rose by %v, want 3", got)
	}
}

func TestLoopsCountErrorsAndPasses(t *testing.T) {
	st := &fakeStore{
		reconcileErr: errors.New("db down"),
		publishErr:   errors.New("db down"),
	}
	c := controller.New(st, &testPublisher{}, time.Millisecond, 10)

	publish, reconcile := loopErrors(metrics.LoopPublish), loopErrors(metrics.LoopReconcile)
	passes := reconcilePasses(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for loopErrors(metrics.LoopPublish) == publish || loopErrors(metrics.LoopReconcile) == reconcile ||
		reconcilePasses(t) == passes {
		if time.Now().After(deadline) {
			t.Fatal("loops did not report errors and passes in time")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// Errors caused by shutdown are not failures: counting them would spike the
// error rate on every rollout.
func TestShutdownIsNotCountedAsLoopError(t *testing.T) {
	st := newBlockingStore()
	c := controller.New(st, &testPublisher{}, time.Millisecond, 10)

	publish, reconcile := loopErrors(metrics.LoopPublish), loopErrors(metrics.LoopReconcile)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	<-st.inPublish
	<-st.inReconcile
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if got := loopErrors(metrics.LoopPublish) - publish; got != 0 {
		t.Errorf("publish errors rose by %v, want 0", got)
	}
	if got := loopErrors(metrics.LoopReconcile) - reconcile; got != 0 {
		t.Errorf("reconcile errors rose by %v, want 0", got)
	}
}
