package metrics

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

type fakeStore struct {
	pending     int
	byState     map[string]int
	experiments int
	err         error
}

func (f *fakeStore) OutboxPending(context.Context) (int, error) { return f.pending, f.err }
func (f *fakeStore) ActiveJobsByState(context.Context) (map[string]int, error) {
	return f.byState, f.err
}
func (f *fakeStore) ActiveExperiments(context.Context) (int, error) { return f.experiments, f.err }

type fakeLag struct {
	lags map[int32]int64
	err  error
}

func (f *fakeLag) ConsumerLag(context.Context) (map[int32]int64, error) { return f.lags, f.err }

func TestDBCollectorReportsStoreState(t *testing.T) {
	c := NewDBCollector(&fakeStore{
		pending:     7,
		byState:     map[string]int{"queued": 3, "running": 0},
		experiments: 2,
	})

	const want = `
# HELP simfleet_experiments_active Experiments currently running.
# TYPE simfleet_experiments_active gauge
simfleet_experiments_active 2
# HELP simfleet_jobs_active Unfinished jobs of running experiments, by state.
# TYPE simfleet_jobs_active gauge
simfleet_jobs_active{state="queued"} 3
simfleet_jobs_active{state="running"} 0
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(want),
		"simfleet_experiments_active", "simfleet_jobs_active"); err != nil {
		t.Error(err)
	}
	if n := testutil.CollectAndCount(c, "simfleet_outbox_pending"); n != 1 {
		t.Errorf("outbox series = %d, want 1", n)
	}
}

// A failed query must not report 0, which would read as a healthy empty queue,
// and must not fail the scrape, which would lose every other metric just when
// they are needed. It leaves the series out and counts the error.
func TestDBCollectorLeavesSeriesOutOnError(t *testing.T) {
	c := NewDBCollector(&fakeStore{err: errors.New("db down")})
	before := testutil.ToFloat64(LoopErrors.WithLabelValues(LoopCollect))

	if n := testutil.CollectAndCount(c); n != 0 {
		t.Errorf("series = %d, want 0", n)
	}
	if got := testutil.ToFloat64(LoopErrors.WithLabelValues(LoopCollect)) - before; got != 3 {
		t.Errorf("collect errors rose by %v, want 3", got)
	}
}

func TestLagCollectorReportsPerPartition(t *testing.T) {
	c := NewLagCollector(&fakeLag{lags: map[int32]int64{0: 5, 11: 0}})

	if n := testutil.CollectAndCount(c, "simfleet_consumer_lag"); n != 2 {
		t.Errorf("series = %d, want 2", n)
	}
}

func TestLagCollectorLeavesSeriesOutOnError(t *testing.T) {
	c := NewLagCollector(&fakeLag{err: errors.New("broker down")})
	before := testutil.ToFloat64(LoopErrors.WithLabelValues(LoopCollect))

	if n := testutil.CollectAndCount(c); n != 0 {
		t.Errorf("series = %d, want 0", n)
	}
	if got := testutil.ToFloat64(LoopErrors.WithLabelValues(LoopCollect)) - before; got != 1 {
		t.Errorf("collect errors rose by %v, want 1", got)
	}
}

// stuckLag ignores its context, as the Kafka client does with a black-holed
// broker. It stays stuck until the test releases it.
type stuckLag struct {
	calls   atomic.Int32
	release chan struct{}
}

func (s *stuckLag) ConsumerLag(context.Context) (map[int32]int64, error) {
	s.calls.Add(1)
	<-s.release
	return map[int32]int64{0: 1}, nil
}

// A stuck broker must not stall the scrape, and scrapes arriving meanwhile must
// not start more calls. Once the stuck call returns, collection recovers.
func TestLagCollectorSurvivesStuckBroker(t *testing.T) {
	src := &stuckLag{release: make(chan struct{})}
	c := NewLagCollector(src)
	c.timeout = 10 * time.Millisecond

	for range 3 {
		start := time.Now()
		if n := testutil.CollectAndCount(c); n != 0 {
			t.Errorf("series = %d, want 0", n)
		}
		if took := time.Since(start); took > 2*time.Second {
			t.Errorf("scrape took %v: it waited for the stuck call", took)
		}
	}
	if n := src.calls.Load(); n != 1 {
		t.Errorf("calls = %d, want 1: scrapes piled up behind the stuck one", n)
	}

	close(src.release)
	c.timeout = 2 * time.Second
	deadline := time.Now().Add(2 * time.Second)
	for testutil.CollectAndCount(c, "simfleet_consumer_lag") != 1 {
		if time.Now().After(deadline) {
			t.Fatal("collection did not recover after the call returned")
		}
		time.Sleep(time.Millisecond)
	}
}

// Series that exist from the start report 0 before they first fire, so the
// first rise is visible to rate() and increase().
func TestSeriesExistBeforeFirstEvent(t *testing.T) {
	if n := testutil.CollectAndCount(LoopErrors); n != 3 {
		t.Errorf("loop error series = %d, want 3", n)
	}
	InitRunner("init-test")
	if got := testutil.ToFloat64(JobsTotal.WithLabelValues("init-test", OutcomeFenced)); got != 0 {
		t.Errorf("fresh outcome = %v, want 0", got)
	}
}

// The buckets must separate the workloads the project runs: both runners'
// 30s timeout has to be finite, and a 5s slowcrossing job must not land in a
// bucket so wide that its quantile estimate is off by a factor of two.
func TestJobDurationBucketsResolveKnownWorkloads(t *testing.T) {
	b := jobDurationBuckets
	if top := b[len(b)-1]; top <= 30 {
		t.Errorf("top bucket = %vs, must exceed the 30s Profile.Timeout", top)
	}
	for i := 1; i < len(b); i++ {
		if b[i-1] < 5 && b[i] >= 5 && b[i]/b[i-1] > 2.01 {
			t.Errorf("5s falls in (%v, %v], too wide to estimate", b[i-1], b[i])
		}
	}
}
