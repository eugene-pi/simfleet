package execution_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/job"
	"github.com/eugene-pi/simfleet/internal/metrics"
	"github.com/eugene-pi/simfleet/internal/runner"
	"github.com/eugene-pi/simfleet/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

var allOutcomes = []string{
	metrics.OutcomeSuccess, metrics.OutcomeFailed, metrics.OutcomeRetried,
	metrics.OutcomeSkipped, metrics.OutcomeFenced, metrics.OutcomeError,
}

func outcomeCounts() map[string]float64 {
	out := make(map[string]float64, len(allOutcomes))
	for _, o := range allOutcomes {
		out[o] = testutil.ToFloat64(metrics.JobsTotal.WithLabelValues(testRunner, o))
	}
	return out
}

func runsObserved(t *testing.T) uint64 {
	t.Helper()
	var m dto.Metric
	if err := metrics.JobDuration.WithLabelValues(testRunner).(prometheus.Metric).Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetHistogram().GetSampleCount()
}

// Every delivery ends in exactly one outcome, and the outcome says what really
// happened to the job. The safety counters fire only on their own paths.
func TestEveryDeliveryCountsOneOutcome(t *testing.T) {
	dbDown := errors.New("db down")
	ok := runFunc(func(context.Context, core.Task) (core.Result, error) {
		return core.Result{}, nil
	})
	flaky := runFunc(func(context.Context, core.Task) (core.Result, error) {
		return core.Result{}, errors.New("flaky")
	})
	permanent := runFunc(func(context.Context, core.Task) (core.Result, error) {
		return core.Result{}, fmt.Errorf("%w: bad params", runner.ErrPermanent)
	})
	fenced := func() error { return store.ErrFenced }

	for _, tc := range []struct {
		name      string
		st        *fakeStore
		r         runFunc
		shutdown  bool // SIGTERM arrives during the run
		want      string
		wantErr   bool
		ran       bool
		isFenced  bool
		exhausted bool
	}{
		{name: "saved", st: &fakeStore{attempt: 1}, r: ok,
			want: metrics.OutcomeSuccess, ran: true},
		{name: "already claimed", st: &fakeStore{claimErr: store.ErrNotClaimable}, r: ok,
			want: metrics.OutcomeSkipped},
		{name: "claim hits the db", st: &fakeStore{claimErr: dbDown}, r: ok,
			want: metrics.OutcomeError, wantErr: true},
		{name: "transient failure", st: &fakeStore{attempt: 1}, r: flaky,
			want: metrics.OutcomeRetried, ran: true},
		{name: "last attempt fails", st: &fakeStore{attempt: job.MaxAttempts}, r: flaky,
			want: metrics.OutcomeFailed, ran: true, exhausted: true},
		{name: "permanent failure uses no attempts up", st: &fakeStore{attempt: 1}, r: permanent,
			want: metrics.OutcomeFailed, ran: true},
		{name: "runner mismatch", st: &fakeStore{attempt: 1, runner: "other"}, r: ok,
			want: metrics.OutcomeFailed, wantErr: true},
		{name: "task cannot load", st: &fakeStore{attempt: 1, loadErr: dbDown}, r: ok,
			want: metrics.OutcomeRetried, wantErr: true},
		{name: "shutdown mid-run", st: &fakeStore{attempt: job.MaxAttempts}, r: flaky, shutdown: true,
			want: metrics.OutcomeRetried, ran: true},
		{name: "stale result", st: &fakeStore{attempt: 1, saveErr: store.ErrFenced}, r: ok,
			want: metrics.OutcomeFenced, ran: true, isFenced: true},
		{name: "stale retry", st: &fakeStore{attempt: 1, releaseFn: fenced}, r: flaky,
			want: metrics.OutcomeFenced, ran: true, isFenced: true},
		{name: "stale failure", st: &fakeStore{attempt: job.MaxAttempts, markErr: store.ErrFenced}, r: flaky,
			want: metrics.OutcomeFenced, ran: true, isFenced: true},
		{name: "result cannot be saved", st: &fakeStore{attempt: 1, saveErr: dbDown}, r: ok,
			want: metrics.OutcomeError, wantErr: true, ran: true},
		{name: "failure cannot be recorded", st: &fakeStore{attempt: job.MaxAttempts, markErr: dbDown}, r: flaky,
			want: metrics.OutcomeError, wantErr: true, ran: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r := runFunc(func(rctx context.Context, task core.Task) (core.Result, error) {
				if tc.shutdown {
					cancel()
				}
				return tc.r(rctx, task)
			})

			before := outcomeCounts()
			runsBefore := runsObserved(t)
			fencedBefore := testutil.ToFloat64(metrics.ResultsFenced)
			exhaustedBefore := testutil.ToFloat64(metrics.AttemptsExhausted)

			_, err := newExecutor(t, tc.st, r).Execute(ctx, newJob())
			if (err != nil) != tc.wantErr {
				t.Errorf("err = %v, wantErr %v", err, tc.wantErr)
			}

			after := outcomeCounts()
			for _, o := range allOutcomes {
				want := 0.0
				if o == tc.want {
					want = 1
				}
				if got := after[o] - before[o]; got != want {
					t.Errorf("outcome %s rose by %v, want %v", o, got, want)
				}
			}
			if got := runsObserved(t) - runsBefore; (got == 1) != tc.ran {
				t.Errorf("duration observations = %d, ran = %v", got, tc.ran)
			}
			if got := testutil.ToFloat64(metrics.ResultsFenced) - fencedBefore; (got == 1) != tc.isFenced {
				t.Errorf("fenced rose by %v, want fenced = %v", got, tc.isFenced)
			}
			if got := testutil.ToFloat64(metrics.AttemptsExhausted) - exhaustedBefore; (got == 1) != tc.exhausted {
				t.Errorf("exhausted rose by %v, want exhausted = %v", got, tc.exhausted)
			}
		})
	}
}

// The record stays uncommitted and is redelivered, so a claim cut short by
// shutdown is not an outcome: counting it would spike the error rate on every
// scale-down.
func TestClaimInterruptedByShutdownIsNotCounted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	st := &fakeStore{claimErr: context.Canceled}
	r := runFunc(func(context.Context, core.Task) (core.Result, error) { return core.Result{}, nil })

	before := outcomeCounts()
	if _, err := newExecutor(t, st, r).Execute(ctx, newJob()); err == nil {
		t.Error("want the claim error back")
	}
	after := outcomeCounts()
	for _, o := range allOutcomes {
		if after[o] != before[o] {
			t.Errorf("outcome %s rose by %v, want 0", o, after[o]-before[o])
		}
	}
}
