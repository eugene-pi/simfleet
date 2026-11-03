package metrics

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// collectTimeout keeps a scrape from hanging: a slow query would stall
// Prometheus and, worse, pile up as scrapes keep arriving every 15s.
const collectTimeout = 5 * time.Second

// collectFailed makes a failed scrape-time query visible. The series is left
// out rather than reported as 0: a zero would read as a healthy empty queue.
func collectFailed(what string, err error) {
	LoopErrors.WithLabelValues(LoopCollect).Inc()
	slog.Warn("collect metric", "metric", what, "error", err)
}

// DBCollector reports state that lives in Postgres, not in this process:
// queue depths and job counts. Collected at scrape time rather than kept in
// memory, because a restart must not reset them. Every controller instance
// reports the same numbers, so aggregate these with max(), never sum().
type DBCollector struct {
	store       Store
	outbox      *prometheus.Desc
	byState     *prometheus.Desc
	experiments *prometheus.Desc
}

type Store interface {
	OutboxPending(ctx context.Context) (int, error)
	ActiveJobsByState(ctx context.Context) (map[string]int, error)
	ActiveExperiments(ctx context.Context) (int, error)
}

func NewDBCollector(st Store) *DBCollector {
	return &DBCollector{
		store: st,
		outbox: prometheus.NewDesc(
			"simfleet_outbox_pending",
			"Outbox rows waiting to be published. Sustained growth means the "+
				"controller is not keeping up; a dead controller shows as up == 0, "+
				"because this gauge is served by the controller itself.",
			nil, nil),
		byState: prometheus.NewDesc(
			"simfleet_jobs_active",
			"Unfinished jobs of running experiments, by state.",
			[]string{"state"}, nil),
		experiments: prometheus.NewDesc(
			"simfleet_experiments_active",
			"Experiments currently running.",
			nil, nil),
	}
}

func (c *DBCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.outbox
	ch <- c.byState
	ch <- c.experiments
}

func (c *DBCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), collectTimeout)
	defer cancel()

	if n, err := c.store.OutboxPending(ctx); err != nil {
		collectFailed("outbox pending", err)
	} else {
		ch <- prometheus.MustNewConstMetric(c.outbox, prometheus.GaugeValue, float64(n))
	}
	if counts, err := c.store.ActiveJobsByState(ctx); err != nil {
		collectFailed("jobs by state", err)
	} else {
		for state, n := range counts {
			ch <- prometheus.MustNewConstMetric(c.byState,
				prometheus.GaugeValue, float64(n), state)
		}
	}
	if n, err := c.store.ActiveExperiments(ctx); err != nil {
		collectFailed("active experiments", err)
	} else {
		ch <- prometheus.MustNewConstMetric(c.experiments, prometheus.GaugeValue, float64(n))
	}
}

var errLagBusy = errors.New("previous call still running")

// LagSource reports how far the worker group is behind, per partition.
type LagSource interface {
	ConsumerLag(ctx context.Context) (map[int32]int64, error)
}

// LagCollector reports consumer group lag at scrape time. It lives in the
// controller, not the workers: workers scale to zero, which is exactly when
// someone needs to see the backlog.
type LagCollector struct {
	src     LagSource
	lag     *prometheus.Desc
	timeout time.Duration
	busy    atomic.Bool // a lag call is still running
}

func NewLagCollector(src LagSource) *LagCollector {
	return &LagCollector{
		src:     src,
		timeout: collectTimeout,
		lag: prometheus.NewDesc(
			"simfleet_consumer_lag",
			"Records not yet committed by the worker group, by partition. "+
				"Uneven values mean a slow worker is pinning its partitions.",
			[]string{"partition"}, nil),
	}
}

func (c *LagCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.lag
}

// Collect does not wait for the lag call past the timeout. With a black-holed
// broker the call outlives its context by far (30s measured), and a scrape that
// waits for it passes Prometheus's own timeout, losing every metric of this
// process just when they are needed. So the call runs aside, one at a time:
// scrapes arriving meanwhile do not pile up behind it.
func (c *LagCollector) Collect(ch chan<- prometheus.Metric) {
	if !c.busy.CompareAndSwap(false, true) {
		collectFailed("consumer lag", errLagBusy)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	type result struct {
		lags map[int32]int64
		err  error
	}
	done := make(chan result, 1) // buffered: nobody reads after a timeout
	go func() {
		defer c.busy.Store(false)
		defer cancel()
		lags, err := c.src.ConsumerLag(ctx)
		done <- result{lags, err}
	}()

	var res result
	select {
	case res = <-done:
	case <-ctx.Done():
		res.err = ctx.Err()
	}
	if res.err != nil {
		collectFailed("consumer lag", res.err)
		return
	}
	for part, n := range res.lags {
		ch <- prometheus.MustNewConstMetric(c.lag, prometheus.GaugeValue,
			float64(n), strconv.Itoa(int(part)))
	}
}
