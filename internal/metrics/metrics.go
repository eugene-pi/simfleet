package metrics

// Package metrics declares the process-level Prometheus metrics. Every metric
// is registered once at package init; labels are deliberately bounded —
// never a job, experiment or worker id, because each new label value creates
// a new time series and autoscaling would create them without limit.

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Outcome values for JobsTotal. Keep this set small and closed.
const (
	OutcomeSuccess = "success"
	OutcomeFailed  = "failed"
	OutcomeRetried = "retried"
	OutcomeSkipped = "skipped" // claimed by someone else
)

var (
	// Buckets span 1ms to ~16s: fast simulations at the bottom, a deliberately
	// slow runner at the top. Pick boundaries so the interesting range spans
	// several buckets — quantiles are interpolated inside a bucket, so a
	// distribution that lands entirely in one is reported as a guess.
	JobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "simfleet_job_duration_seconds",
		Help:    "Time spent inside Runner.Run, excluding claim and result write.",
		Buckets: prometheus.ExponentialBuckets(0.001, 4, 8),
	}, []string{"runner"})

	JobsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "simfleet_jobs_total",
		Help: "Jobs finished, by outcome.",
	}, []string{"runner", "outcome"})

	// The next three are the failure-tolerance mechanisms firing. Their normal
	// value is near zero; a sustained rise is the signal, not the absolute.

	LeasesExpired = promauto.NewCounter(prometheus.CounterOpts{
		Name: "simfleet_leases_expired_total",
		Help: "Jobs requeued by the controller after their lease expired.",
	})

	ResultsFenced = promauto.NewCounter(prometheus.CounterOpts{
		Name: "simfleet_results_fenced_total",
		Help: "Result writes rejected by the fencing check. A sustained rise " +
			"means leases are shorter than real job duration.",
	})

	AttemptsExhausted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "simfleet_attempts_exhausted_total",
		Help: "Jobs moved to failed after using all attempts.",
	})

	// Controller loops.

	ReconcileDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "simfleet_reconcile_duration_seconds",
		Help:    "Duration of one reconciliation pass.",
		Buckets: prometheus.ExponentialBuckets(0.01, 3, 7), // 10ms … ~20s
	})

	LoopErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "simfleet_loop_errors_total",
		Help: "Errors from the controller loops, by loop.",
	}, []string{"loop"}) // publish | reconcile

	Published = promauto.NewCounter(prometheus.CounterOpts{
		Name: "simfleet_outbox_published_total",
		Help: "Outbox rows successfully published to the bus.",
	})

	// Consumer side.

	RecordsConsumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "simfleet_records_consumed_total",
		Help: "Records taken from the bus.",
	})

	RecordsMalformed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "simfleet_records_malformed_total",
		Help: "Records skipped because they could not be parsed.",
	})

	PartitionsOwned = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "simfleet_partitions_owned",
		Help: "Partitions currently assigned to this consumer.",
	})

	CommitFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "simfleet_commit_failures_total",
		Help: "Failed offset commits. Each one means work will be redelivered.",
	})
)
