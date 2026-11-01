package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// DBCollector reports state that lives in Postgres, not in this process:
// queue depths and job counts. Collected at scrape time rather than kept in
// memory, because a restart must not reset them and several controller
// instances must not double-count.
type DBCollector struct {
	store   Store
	outbox  *prometheus.Desc
	byState *prometheus.Desc
}

type Store interface {
	OutboxPending(ctx context.Context) (int, error)
	ActiveJobsByState(ctx context.Context) (map[string]int, error)
	// ActiveExperiments(ctx context.Context) (int, error)
}

func NewDBCollector(st Store) *DBCollector {
	return &DBCollector{
		store: st,
		outbox: prometheus.NewDesc(
			"simfleet_outbox_pending",
			"Outbox rows waiting to be published. Sustained growth means the "+
				"controller is not keeping up or is down.",
			nil, nil),
		byState: prometheus.NewDesc(
			"simfleet_jobs",
			"Jobs by state.",
			[]string{"state"}, nil),
	}
}

func (c *DBCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.outbox
	ch <- c.byState
}

func (c *DBCollector) Collect(ch chan<- prometheus.Metric) {
	// Scrapes must not hang: a slow query would stall Prometheus and, worse,
	// pile up as scrapes keep arriving every 15s.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if n, err := c.store.OutboxPending(ctx); err == nil {
		ch <- prometheus.MustNewConstMetric(c.outbox, prometheus.GaugeValue, float64(n))
	}
	if counts, err := c.store.ActiveJobsByState(ctx); err == nil {
		for state, n := range counts {
			ch <- prometheus.MustNewConstMetric(c.byState,
				prometheus.GaugeValue, float64(n), state)
		}
	}
}
