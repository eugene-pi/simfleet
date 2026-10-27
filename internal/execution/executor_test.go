package execution_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/eugene-pi/simfleet/internal/config"
	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/execution"
	"github.com/eugene-pi/simfleet/internal/job"
	"github.com/eugene-pi/simfleet/internal/runner"
	"github.com/eugene-pi/simfleet/internal/store"
)

// call is one store write as the executor made it. ctxAlive records whether
// the context was still usable at that moment — the point of the shutdown tests.
type call struct {
	method   string
	attempt  int
	cause    string
	ctxAlive bool
}

type fakeStore struct {
	mu        sync.Mutex
	attempt   int    // returned by Claim
	runner    string // returned by LoadTask
	renewErrs []error
	renewLD   []time.Duration
	releaseFn func() error // optional override for ReleaseForRetry
	calls     []call
}

func (f *fakeStore) record(ctx context.Context, method string, attempt int, cause string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call{method, attempt, cause, ctx.Err() == nil})
}

func (f *fakeStore) writes() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call(nil), f.calls...)
}

func (f *fakeStore) Claim(ctx context.Context, _ core.JobID, _ string, _ time.Duration) (int, error) {
	return f.attempt, nil
}

func (f *fakeStore) LoadTask(ctx context.Context, id core.JobID) (core.Task, error) {
	return core.Task{JobID: id, Runner: f.runner}, nil
}

func (f *fakeStore) SaveResult(ctx context.Context, r store.ResultRecord) error {
	f.record(ctx, "SaveResult", r.Attempt, "")
	return nil
}

func (f *fakeStore) ReleaseForRetry(ctx context.Context, _ core.JobID, attempt int, cause string) error {
	f.record(ctx, "ReleaseForRetry", attempt, cause)
	if f.releaseFn != nil {
		return f.releaseFn()
	}
	return nil
}

func (f *fakeStore) MarkFailed(ctx context.Context, _ core.JobID, attempt int, cause string) error {
	f.record(ctx, "MarkFailed", attempt, cause)
	return nil
}

func (f *fakeStore) RenewLease(ctx context.Context, _ core.JobID, _ int, ld time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewLD = append(f.renewLD, ld)
	if len(f.renewErrs) == 0 {
		return nil
	}
	err := f.renewErrs[0]
	if len(f.renewErrs) > 1 {
		f.renewErrs = f.renewErrs[1:]
	}
	return err
}

type nopBlob struct{}

func (nopBlob) Put(context.Context, string, io.Reader) error { return nil }

type runFunc func(ctx context.Context, t core.Task) (core.Result, error)

func (f runFunc) Run(ctx context.Context, t core.Task) (core.Result, error) { return f(ctx, t) }

const testRunner = "test"

func newExecutor(t *testing.T, st *fakeStore, r runner.Runner) *execution.Executor {
	t.Helper()
	if st.runner == "" {
		st.runner = testRunner
	}
	desc := runner.Descriptor{Name: testRunner, Profile: core.Profile{Timeout: 5 * time.Second}}
	wc := config.WorkerConfig{
		WorkerId:      "test", // not "w1": that one carries the crash-test code
		LeaseDuration: 30 * time.Millisecond,
		RenewEvery:    10 * time.Millisecond,
	}
	e, err := execution.NewExecutor(st, nopBlob{}, desc, r, wc)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func newJob() core.JobID { return core.JobID(uuid.NewV7()) }

func onlyWrite(t *testing.T, st *fakeStore) call {
	t.Helper()
	w := st.writes()
	if len(w) != 1 {
		t.Fatalf("store writes = %+v, want exactly one", w)
	}
	return w[0]
}

func TestZeroTimeoutIsRejected(t *testing.T) {
	desc := runner.Descriptor{Name: "x"}
	if _, err := execution.NewExecutor(&fakeStore{}, nopBlob{}, desc, nil, config.WorkerConfig{}); err == nil {
		t.Fatal("NewExecutor accepted a zero Profile.Timeout")
	}
}

// A transient failure is retried while attempts remain, and retired on the last.
func TestTransientFailureRetriesUntilExhausted(t *testing.T) {
	boom := runFunc(func(context.Context, core.Task) (core.Result, error) {
		return core.Result{}, errors.New("flaky")
	})
	for _, tc := range []struct {
		attempt int
		want    string
	}{
		{1, "ReleaseForRetry"},
		{job.MaxAttempts - 1, "ReleaseForRetry"},
		{job.MaxAttempts, "MarkFailed"},
	} {
		t.Run(fmt.Sprint("attempt ", tc.attempt), func(t *testing.T) {
			st := &fakeStore{attempt: tc.attempt}
			if _, err := newExecutor(t, st, boom).Execute(context.Background(), newJob()); err != nil {
				t.Fatalf("execute: %v", err)
			}
			if w := onlyWrite(t, st); w.method != tc.want {
				t.Errorf("write = %s, want %s", w.method, tc.want)
			}
		})
	}
}

// Retrying a permanent failure only burns attempts: it fails on the first.
func TestPermanentFailureFailsAtOnce(t *testing.T) {
	st := &fakeStore{attempt: 1}
	bad := runFunc(func(context.Context, core.Task) (core.Result, error) {
		return core.Result{}, fmt.Errorf("%w: bad params", runner.ErrPermanent)
	})
	if _, err := newExecutor(t, st, bad).Execute(context.Background(), newJob()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if w := onlyWrite(t, st); w.method != "MarkFailed" {
		t.Errorf("write = %s, want MarkFailed", w.method)
	}
}

func TestRunnerMismatchFailsAtOnce(t *testing.T) {
	st := &fakeStore{attempt: 1, runner: "other"}
	ran := false
	r := runFunc(func(context.Context, core.Task) (core.Result, error) {
		ran = true
		return core.Result{}, nil
	})
	if _, err := newExecutor(t, st, r).Execute(context.Background(), newJob()); err == nil {
		t.Error("want an error for a runner mismatch")
	}
	if ran {
		t.Error("job ran on the wrong runner")
	}
	if w := onlyWrite(t, st); w.method != "MarkFailed" {
		t.Errorf("write = %s, want MarkFailed", w.method)
	}
}

// SIGTERM mid-run: the job goes straight back to the queue, through a context
// that shutdown has not cancelled, and it is not counted as a failure.
func TestShutdownReleasesJobWithLiveContext(t *testing.T) {
	st := &fakeStore{attempt: job.MaxAttempts} // would be MarkFailed if counted
	ctx, cancel := context.WithCancel(context.Background())
	r := runFunc(func(rctx context.Context, _ core.Task) (core.Result, error) {
		cancel()
		<-rctx.Done()
		return core.Result{}, rctx.Err()
	})
	if _, err := newExecutor(t, st, r).Execute(ctx, newJob()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	w := onlyWrite(t, st)
	if w.method != "ReleaseForRetry" || w.cause != "worker shutdown" {
		t.Errorf("write = %+v, want ReleaseForRetry(worker shutdown)", w)
	}
	if !w.ctxAlive {
		t.Error("release used the cancelled context; it would never reach the DB")
	}
}

// A run that finished just as shutdown began still saves its result.
func TestResultSavedAfterShutdownBegins(t *testing.T) {
	st := &fakeStore{attempt: 1}
	ctx, cancel := context.WithCancel(context.Background())
	r := runFunc(func(context.Context, core.Task) (core.Result, error) {
		cancel()
		return core.Result{Metrics: map[string]float64{"m": 1}}, nil
	})
	if _, err := newExecutor(t, st, r).Execute(ctx, newJob()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if w := onlyWrite(t, st); w.method != "SaveResult" || !w.ctxAlive {
		t.Errorf("write = %+v, want SaveResult with a live context", w)
	}
}

// Renewal extends by a full LeaseDuration, and one failed renewal does not
// stop the job: there is still lease left until the next tick.
func TestLeaseRenewalSurvivesTransientError(t *testing.T) {
	st := &fakeStore{attempt: 1, renewErrs: []error{errors.New("db hiccup"), nil}}
	r := runFunc(func(ctx context.Context, _ core.Task) (core.Result, error) {
		select {
		case <-time.After(60 * time.Millisecond): // several renew ticks
			return core.Result{}, nil
		case <-ctx.Done():
			return core.Result{}, ctx.Err()
		}
	})
	if _, err := newExecutor(t, st, r).Execute(context.Background(), newJob()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if w := onlyWrite(t, st); w.method != "SaveResult" {
		t.Fatalf("write = %s, want SaveResult (job must survive one failed renewal)", w.method)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.renewLD) < 2 {
		t.Fatalf("renewals = %d, want at least 2", len(st.renewLD))
	}
	for _, ld := range st.renewLD {
		if ld != 30*time.Millisecond {
			t.Errorf("renewed by %v, want LeaseDuration (30ms)", ld)
		}
	}
}

// A definite loss cancels the run; the stale attempt is fenced off, which is
// not an error for the executor.
func TestLeaseLossCancelsRun(t *testing.T) {
	st := &fakeStore{
		attempt:   1,
		renewErrs: []error{store.ErrLeaseLost},
		releaseFn: func() error { return store.ErrFenced },
	}
	r := runFunc(func(ctx context.Context, _ core.Task) (core.Result, error) {
		select {
		case <-time.After(2 * time.Second):
			return core.Result{}, errors.New("run was not cancelled")
		case <-ctx.Done():
			return core.Result{}, ctx.Err()
		}
	})
	if _, err := newExecutor(t, st, r).Execute(context.Background(), newJob()); err != nil {
		t.Fatalf("execute: %v (fenced write must not surface)", err)
	}
	if w := onlyWrite(t, st); w.method != "ReleaseForRetry" {
		t.Errorf("write = %s, want ReleaseForRetry", w.method)
	}
}
