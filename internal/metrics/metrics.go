// Package metrics declares the process-level Prometheus metrics. Every metric
// is registered once at package init; labels are deliberately bounded —
// never a job, experiment or worker id, because each new label value creates
// a new time series and autoscaling would create them without limit.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Outcome values for JobsTotal. Keep this set small and closed.
const (
	OutcomeSuccess = "success"
	OutcomeFailed  = "failed"  // moved to failed: permanent error or no attempts left
	OutcomeRetried = "retried" // back in the queue: failed attempt or shutdown
	OutcomeSkipped = "skipped" // claimed by someone else
	OutcomeFenced  = "fenced"  // write rejected: the job moved on without this attempt
	OutcomeError   = "error"   // claim or state write failed: only lease expiry or redelivery recovers the job
)

// Loop values for LoopErrors.
const (
	LoopPublish   = "publish"
	LoopReconcile = "reconcile"
	LoopCollect   = "collect" // scrape-time queries of the collectors
)

// Buckets span 100µs to ~52s: the 30s Profile.Timeout near the top, so a run
// that times out still lands in a finite bucket. Pick boundaries so the
// interesting range spans several buckets — quantiles are interpolated inside
// a bucket, so a distribution that lands entirely in one is reported as a
// guess. The exception is crossing, at about 2.5µs a run: all of it lands in
// the first bucket, and "under 100µs" is as precise as that number needs to be.
var jobDurationBuckets = prometheus.ExponentialBuckets(0.0001, 2, 20)

var (
	JobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "simfleet_job_duration_seconds",
		Help:    "Time spent inside Runner.Run, excluding claim and result write. Failed runs included.",
		Buckets: jobDurationBuckets,
	}, []string{"runner"})

	JobsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "simfleet_jobs_total",
		Help: "Job deliveries handled by a worker, by outcome. Exactly one per delivery.",
	}, []string{"runner", "outcome"})

	// The next three are the failure-tolerance mechanisms firing. Their normal
	// value is near zero; a sustained rise is the signal, not the absolute.

	LeasesExpired = promauto.NewCounter(prometheus.CounterOpts{
		Name: "simfleet_leases_expired_total",
		Help: "Jobs requeued by the controller after their lease expired.",
	})

	ResultsFenced = promauto.NewCounter(prometheus.CounterOpts{
		Name: "simfleet_results_fenced_total",
		Help: "Worker state writes rejected by the fencing check. A sustained rise " +
			"means leases are shorter than real job duration.",
	})

	// Reported by whichever side retires the job: the worker when the last
	// attempt failed, the controller when the last lease expired. Sum both.
	AttemptsExhausted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "simfleet_attempts_exhausted_total",
		Help: "Jobs moved to failed after using all attempts.",
	})

	// Controller loops.

	ReconcileDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "simfleet_reconcile_duration_seconds",
		Help:    "Duration of one reconciliation pass.",
		Buckets: prometheus.ExponentialBuckets(0.001, 3, 10), // 1ms … ~20s
	})

	LoopErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "simfleet_loop_errors_total",
		Help: "Errors from the controller's background work, by loop.",
	}, []string{"loop"})

	Published = promauto.NewCounter(prometheus.CounterOpts{
		Name: "simfleet_outbox_published_total",
		Help: "Outbox rows published to the bus and marked as sent.",
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
		Help: "Failed offset commits. Retried on the next tick; work is redelivered " +
			"only if the partition moves before a retry succeeds.",
	})
)

func init() {
	for _, l := range []string{LoopPublish, LoopReconcile, LoopCollect} {
		LoopErrors.WithLabelValues(l)
	}
}

// InitRunner pre-creates the series of a runner, so a metric that has not
// fired yet reports 0 instead of being absent.
func InitRunner(name string) {
	JobDuration.WithLabelValues(name)
	for _, o := range []string{OutcomeSuccess, OutcomeFailed, OutcomeRetried,
		OutcomeSkipped, OutcomeFenced, OutcomeError} {
		JobsTotal.WithLabelValues(name, o)
	}
}
