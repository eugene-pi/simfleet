package report

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/store"
)

// fakeSource replays a scripted sequence of progress snapshots, one per poll;
// the last one repeats.
type fakeSource struct {
	frames  []store.ProgressData
	errs    map[int]error // poll number → error
	polls   int
	reports int
}

func (f *fakeSource) Progress(context.Context, core.ExperimentID) (store.ProgressData, error) {
	i := f.polls
	f.polls++
	if err := f.errs[i]; err != nil {
		return store.ProgressData{}, err
	}
	return f.frames[min(i, len(f.frames)-1)], nil
}

func (f *fakeSource) ReportData(_ context.Context, _ core.ExperimentID, n int) (store.ReportData, error) {
	f.reports++
	return store.ReportData{ProgressData: f.frames[len(f.frames)-1]}, nil
}

func frame(state string, done, total, unscored int) store.ProgressData {
	return store.ProgressData{
		Experiment: store.ExperimentInfo{Name: "x", State: state, CreatedAt: time.Now()},
		Jobs:       store.Summary{Total: total, Completed: done, Queued: total - done},
		Results:    store.ResultStats{Count: done, Scored: done - unscored, Unscored: unscored},
		Zones:      map[string]int{"green": done - unscored},
	}
}

func TestWatchWaitsForScoringThenReports(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &fakeSource{frames: []store.ProgressData{
			frame("running", 0, 100, 0),
			frame("running", 40, 100, 10),
			frame("completed", 100, 100, 30), // workers done, scorer behind
			frame("completed", 100, 100, 5),
			frame("completed", 100, 100, 0),
		}}
		var views []LiveView
		r, err := Watch(t.Context(), src, core.ExperimentID{}, WatchOptions{
			Interval: time.Second, ScoreWait: time.Minute, N: 3,
			OnTick: func(v LiveView) error { views = append(views, v); return nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		if src.polls != 5 || src.reports != 1 {
			t.Errorf("polls = %d, reports = %d; want 5 and 1", src.polls, src.reports)
		}
		if !r.Finished || r.N != 3 {
			t.Errorf("final report = finished %v, n %d", r.Finished, r.N)
		}
		if views[1].Rate != 40 || views[1].ETA != 1500*time.Millisecond {
			t.Errorf("rate/eta = %v/%v, want 40 jobs/s, 1.5s", views[1].Rate, views[1].ETA)
		}
		if !views[2].Scoring || !views[3].Scoring || views[4].Scoring {
			t.Errorf("scoring flags = %v %v %v", views[2].Scoring, views[3].Scoring, views[4].Scoring)
		}
	})
}

func TestWatchGivesUpOnScoringAfterScoreWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The scorer never catches up (e.g. stuck on bad rows).
		src := &fakeSource{frames: []store.ProgressData{frame("completed", 10, 10, 4)}}
		start := time.Now()
		r, err := Watch(t.Context(), src, core.ExperimentID{}, WatchOptions{
			Interval: time.Second, ScoreWait: 5 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		if waited := time.Since(start); waited != 5*time.Second {
			t.Errorf("waited %v, want ScoreWait 5s", waited)
		}
		if r.Complete() || !strings.Contains(strings.Join(r.Warnings, " "), "4 results are not scored") {
			t.Errorf("report does not say what is unscored: %q", r.Warnings)
		}
	})
}

// A slow scorer that keeps making progress is waited for, however long the
// whole backlog takes; ScoreWait only bounds a stall.
func TestWatchWaitsForSlowButProgressingScorer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var frames []store.ProgressData
		for left := 100; left >= 0; left -= 10 { // 10 per 5s poll, like the controller
			frames = append(frames, frame("completed", 100, 100, left))
		}
		src := &fakeSource{frames: frames}
		start := time.Now()
		r, err := Watch(t.Context(), src, core.ExperimentID{}, WatchOptions{
			Interval: 5 * time.Second, ScoreWait: 6 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		if waited := time.Since(start); waited != 50*time.Second {
			t.Errorf("waited %v, want 50s: the whole backlog, well past ScoreWait", waited)
		}
		if r.Results.Unscored != 0 {
			t.Errorf("report built with %d unscored", r.Results.Unscored)
		}
	})
}

func TestWatchToleratesTransientPollErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		blip := errors.New("connection reset")
		src := &fakeSource{
			frames: []store.ProgressData{frame("running", 1, 10, 0), frame("running", 1, 10, 0), frame("running", 1, 10, 0), frame("completed", 10, 10, 0)},
			errs:   map[int]error{1: blip, 2: blip},
		}
		var sawErr bool
		_, err := Watch(t.Context(), src, core.ExperimentID{}, WatchOptions{
			Interval: time.Second, MaxPollFailures: 3,
			OnTick: func(v LiveView) error {
				if errors.Is(v.Err, blip) {
					sawErr = true
				}
				return nil
			},
		})
		if err != nil {
			t.Fatalf("two failed polls ended the watch: %v", err)
		}
		if !sawErr {
			t.Error("a failed poll was not shown")
		}

		src = &fakeSource{
			frames: []store.ProgressData{frame("running", 1, 10, 0)},
			errs:   map[int]error{0: blip, 1: blip, 2: blip},
		}
		_, err = Watch(t.Context(), src, core.ExperimentID{}, WatchOptions{Interval: time.Second, MaxPollFailures: 3})
		if !errors.Is(err, blip) {
			t.Errorf("err = %v, want the watch to stop after 3 failures", err)
		}
	})
}

func TestWatchStopsOnUnknownExperiment(t *testing.T) {
	src := &fakeSource{
		frames: []store.ProgressData{{}},
		errs:   map[int]error{0: store.ErrExperimentNotFound},
	}
	_, err := Watch(context.Background(), src, core.ExperimentID{}, WatchOptions{})
	if !errors.Is(err, store.ErrExperimentNotFound) || src.polls != 1 {
		t.Errorf("err = %v after %d polls", err, src.polls)
	}
}
