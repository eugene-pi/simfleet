package report

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/store"
)

// Source is what the report reads from; *store.Postgres implements it.
type Source interface {
	Progress(ctx context.Context, id core.ExperimentID) (store.ProgressData, error)
	ReportData(ctx context.Context, id core.ExperimentID, n int) (store.ReportData, error)
}

// LiveView is one frame of the live statistics.
type LiveView struct {
	Report
	Done int           // jobs in a final state
	Rate float64       // finished jobs per second over the recent window; 0 = unknown
	ETA  time.Duration // < 0 = unknown
	// Scoring is set once every job has finished but results are still
	// waiting for the scorer: the experiment is done, the report is not.
	Scoring bool
	Err     error // last poll failed; the numbers are from the previous one
	At      time.Time
}

type WatchOptions struct {
	Interval time.Duration // between polls; default 2s
	// ScoreWait bounds the wait for the scorer once the experiment has
	// finished. The controller scores in batches and can lag minutes behind
	// the workers, so the clock restarts whenever the backlog shrinks: the
	// watch gives up only after scoring makes no progress for ScoreWait.
	// The report is then built anyway and says what is unscored.
	// Default 2m; negative means do not wait.
	ScoreWait time.Duration
	N         int                  // worst/best list length of the final report
	OnTick    func(LiveView) error // renders a frame; an error stops the watch
	// MaxPollFailures consecutive failed polls end the watch. Default 5.
	MaxPollFailures int
}

// Watch polls an experiment, hands a LiveView to OnTick after every poll,
// and returns the full report once the experiment has finished and its
// results are scored (or ScoreWait ran out).
func Watch(ctx context.Context, src Source, id core.ExperimentID, opt WatchOptions) (Report, error) {
	if opt.Interval <= 0 {
		opt.Interval = 2 * time.Second
	}
	if opt.ScoreWait == 0 {
		opt.ScoreWait = 2 * time.Minute
	}
	if opt.MaxPollFailures <= 0 {
		opt.MaxPollFailures = 5
	}
	if opt.OnTick == nil {
		opt.OnTick = func(LiveView) error { return nil }
	}

	meter := rateMeter{window: max(10*opt.Interval, 30*time.Second)}
	ticker := time.NewTicker(opt.Interval)
	defer ticker.Stop()

	var (
		last     LiveView
		failures int
		// Scoring progress once the experiment has finished: when the
		// backlog last shrank, and its size then.
		scoredAt time.Time
		backlog  int
	)
	for {
		now := time.Now()
		d, err := src.Progress(ctx, id)
		switch {
		case err == nil:
			failures = 0
			last = liveView(d, &meter, now)
		case errors.Is(err, store.ErrExperimentNotFound), ctx.Err() != nil:
			return Report{}, err
		default:
			failures++
			if failures >= opt.MaxPollFailures {
				return Report{}, fmt.Errorf("%d polls in a row failed: %w", failures, err)
			}
			last.Err, last.At = err, now
		}

		if err == nil && last.Finished {
			if left := last.Results.Unscored; scoredAt.IsZero() || left < backlog {
				scoredAt, backlog = now, left
			}
			waiting := last.Results.Unscored > 0 && now.Sub(scoredAt) < opt.ScoreWait
			last.Scoring = waiting
			if !waiting {
				if err := opt.OnTick(last); err != nil {
					return Report{}, err
				}
				rd, err := src.ReportData(ctx, id, opt.N)
				if err != nil {
					return Report{}, err
				}
				return Build(rd, opt.N, time.Now()), nil
			}
		}
		if err := opt.OnTick(last); err != nil {
			return Report{}, err
		}

		select {
		case <-ctx.Done():
			return Report{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func liveView(d store.ProgressData, m *rateMeter, now time.Time) LiveView {
	j := d.Jobs
	v := LiveView{
		Report: BuildProgress(d, now),
		Done:   j.Completed + j.Failed + j.Cancelled,
		ETA:    -1,
		At:     now,
	}
	v.Rate = m.observe(now, v.Done)
	if left := j.Total - v.Done; left == 0 {
		v.ETA = 0
	} else if v.Rate > 0 {
		v.ETA = time.Duration(float64(left) / v.Rate * float64(time.Second))
	}
	return v
}

// rateMeter measures throughput over a sliding window rather than since
// submit: workers join and leave (KEDA scales them), and the average since
// the start would hide that.
type rateMeter struct {
	window  time.Duration
	samples []sample
}

type sample struct {
	at   time.Time
	done int
}

func (m *rateMeter) observe(at time.Time, done int) float64 {
	m.samples = append(m.samples, sample{at, done})
	cut := 0
	for cut < len(m.samples)-2 && at.Sub(m.samples[cut+1].at) >= m.window {
		cut++
	}
	m.samples = m.samples[cut:]

	first := m.samples[0]
	dt := at.Sub(first.at).Seconds()
	if dt <= 0 {
		return 0
	}
	return float64(done-first.done) / dt
}

// --- rendering ---

// WriteLive renders a frame for a terminal that is redrawn in place.
func WriteLive(w io.Writer, v LiveView, st TextStyle) error {
	p := &printer{w: w}
	r := v.Report

	status := r.State
	if v.Scoring {
		status = "finished, waiting for scoring"
	}
	p.f("%s  (%s)\n", r.Name, r.ID)
	p.f("%s   elapsed %s   updated %s\n\n", status, FormatDuration(r.WallTime),
		v.At.Local().Format("15:04:05"))

	p.f("progress  %s  %d/%d jobs  %s\n", progressBar(v.Done, r.Jobs.Total, st),
		v.Done, r.Jobs.Total, pct(ratio(v.Done, r.Jobs.Total)))
	p.f("          %s\n\n", rateLine(v))

	p.f("outcomes  %s\n", renderBar(r.Overall, st))
	writeLegend(p, r.Overall, st)
	p.f("\n")

	res := r.Results
	if res.Count > 0 {
		p.f("run time  mean %s/job   max %s   total %s\n",
			RunDuration(res.MeanDuration), RunDuration(res.MaxDuration),
			RunDuration(res.TotalDuration))
	}
	p.f("scoring   %d waiting   %d errors\n", res.Unscored, res.ScoringErrors)
	if v.Err != nil {
		p.f("\n! last poll failed: %v\n", v.Err)
	}
	return p.err
}

// LiveLine renders a frame as one line, for logs and pipes where a redrawn
// screen would be noise.
func LiveLine(v LiveView) string {
	r := v.Report
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s %s %d/%d done (%s)", v.At.Local().Format("15:04:05"),
		r.State, v.Done, r.Jobs.Total, pct(ratio(v.Done, r.Jobs.Total)))
	for _, s := range r.Overall.Segments {
		if s.Count > 0 {
			fmt.Fprintf(&sb, " %s=%d", strings.ReplaceAll(s.Label, " ", "_"), s.Count)
		}
	}
	fmt.Fprintf(&sb, " | %s", rateLine(v))
	if v.Scoring {
		sb.WriteString(" | waiting for scoring")
	}
	if v.Err != nil {
		fmt.Fprintf(&sb, " | poll failed: %v", v.Err)
	}
	return sb.String()
}

func rateLine(v LiveView) string {
	if v.ETA == 0 {
		// Nothing left to run: a rate decaying towards zero means nothing.
		return "all jobs finished"
	}
	rate := "— jobs/s"
	if v.Rate > 0 {
		rate = fmt.Sprintf("%.1f jobs/s", v.Rate)
	}
	eta := "ETA —"
	switch {
	case v.ETA == 0:
		eta = "all jobs finished"
	case v.ETA > 0:
		eta = "ETA " + FormatDuration(v.ETA.Round(time.Second))
	}
	return rate + "   " + eta
}

func progressBar(done, total int, st TextStyle) string {
	width := st.width()
	n := 0
	if total > 0 {
		n = min(done*width/total, width)
	}
	full, empty := strings.Repeat("█", n), strings.Repeat("░", width-n)
	if st.Color {
		empty = ansiDim + empty + ansiReset
	}
	return "[" + full + empty + "]"
}
