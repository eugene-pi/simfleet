package report

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/store"
)

var t0 = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

func zones() []core.Zone {
	// Deliberately out of order: the report must sort by Min, best first.
	return []core.Zone{{Name: "red", Min: 0}, {Name: "green", Min: 80}, {Name: "yellow", Min: 50}}
}

// sample: 10 jobs over speed {10, 14} × appears {20} (fixed).
// speed 10: 4 green, 1 yellow. speed 14: 1 green, 2 red, 1 failed, 1 unscored.
func sampleData(state string) store.ReportData {
	done := t0.Add(90 * time.Second)
	var completedAt *time.Time
	if state == "completed" {
		completedAt = &done
	}
	num := core.NumFloat
	cell := func(param string, v core.ParamValue, zone, st string, n int, sum float64) store.AxisCell {
		return store.AxisCell{Param: param, Value: v, Zone: zone, State: st, Count: n, ScoreSum: sum}
	}
	return store.ReportData{
		ProgressData: store.ProgressData{
			Experiment: store.ExperimentInfo{
				Name: "crossing", Runner: "crossing", State: state,
				Spec:      core.Spec{Zones: zones()},
				CreatedAt: t0, CompletedAt: completedAt,
			},
			Jobs: store.Summary{Total: 10, Completed: 9, Failed: 1},
			Results: store.ResultStats{
				Count: 9, Scored: 8, Unscored: 1,
				TotalDuration: 900 * time.Millisecond, MeanDuration: 100 * time.Millisecond,
				MinScore: 0, MaxScore: 100, MinScoreTies: 2, MaxScoreTies: 5,
			},
			Zones: map[string]int{"green": 5, "yellow": 1, "red": 2},
		},
		Axes: []store.AxisCell{
			cell("speed", num(14), "green", "completed", 1, 100),
			cell("speed", num(14), "red", "completed", 2, 0),
			cell("speed", num(14), "", "failed", 1, 0),
			cell("speed", num(14), "", "completed", 1, 0),
			cell("speed", num(10), "green", "completed", 4, 400),
			cell("speed", num(10), "yellow", "completed", 1, 60),
			cell("appears", num(20), "green", "completed", 5, 500),
			cell("appears", num(20), "yellow", "completed", 1, 60),
			cell("appears", num(20), "red", "completed", 2, 0),
			cell("appears", num(20), "", "failed", 1, 0),
			cell("appears", num(20), "", "completed", 1, 0),
		},
		Worst: []store.CaseRow{{
			Idx: 7, Score: 0, Zone: "red", GatedBy: "collision",
			Params:  map[string]core.ParamValue{"speed": num(14), "appears": num(20)},
			Metrics: map[string]float64{"min_distance_m": -1},
		}},
		Best: []store.CaseRow{{
			Idx: 0, Score: 100, Zone: "green",
			Params:  map[string]core.ParamValue{"speed": num(10), "appears": num(20)},
			Metrics: map[string]float64{"min_distance_m": 4.2},
		}},
	}
}

func TestBuildOverallSharesAreOfAllJobs(t *testing.T) {
	r := Build(sampleData("completed"), 1, t0.Add(time.Hour))

	want := []struct {
		label string
		n     int
	}{{"green", 5}, {"yellow", 1}, {"red", 2}, {"failed", 1}, {"unfinished", 0}, {"not scored", 1}}
	if len(r.Overall.Segments) != len(want) {
		t.Fatalf("segments = %+v", r.Overall.Segments)
	}
	for i, w := range want {
		s := r.Overall.Segments[i]
		if s.Label != w.label || s.Count != w.n {
			t.Errorf("segment %d = %s:%d, want %s:%d", i, s.Label, s.Count, w.label, w.n)
		}
		if got := s.Share; math.Abs(got-float64(w.n)/10) > 1e-9 {
			t.Errorf("%s share = %v, want of all 10 jobs", s.Label, got)
		}
	}
	if r.WallTime != 90*time.Second {
		t.Errorf("wall time = %v, want submit→completion 90s", r.WallTime)
	}
}

// While an experiment runs, queued and running jobs are "unfinished", not
// "not scored": the two mean different things and need different action.
func TestBuildSeparatesUnfinishedFromUnscored(t *testing.T) {
	d := sampleData("running")
	d.Jobs = store.Summary{Total: 10, Completed: 6, Queued: 3, Running: 1}
	d.Zones = map[string]int{"green": 4, "red": 1} // 5 of 6 finished scored
	b := Build(d, 1, t0).Overall
	if b.Count("unfinished") != 4 || b.Count("not scored") != 1 {
		t.Errorf("unfinished = %d, not scored = %d; want 4 and 1",
			b.Count("unfinished"), b.Count("not scored"))
	}

	// Per value too, from the job state of unscored cells.
	cells := []store.AxisCell{
		{Param: "p", Value: core.NumFloat(1), State: "queued", Count: 2},
		{Param: "p", Value: core.NumFloat(1), State: "completed", Count: 1},
		{Param: "p", Value: core.NumFloat(2), Zone: "green", State: "completed", Count: 3, ScoreSum: 300},
	}
	axes, _ := buildAxes(cells, []string{"green"})
	if o := axes[0].Values[0].Outcomes; o.Count("unfinished") != 2 || o.Count("not scored") != 1 {
		t.Errorf("p=1 outcomes = %+v", o.Segments)
	}
}

func TestColumnsSkipEmptyBuckets(t *testing.T) {
	d := sampleData("completed")
	d.Jobs = store.Summary{Total: 8, Completed: 8}
	var got []string
	for _, s := range Build(d, 1, t0).Columns() {
		got = append(got, s.Label)
	}
	if strings.Join(got, ",") != "green,yellow,red" {
		t.Errorf("columns of a complete run = %v", got)
	}
}

func TestShareCellNeverRoundsToZeroOrAll(t *testing.T) {
	for _, c := range []struct {
		n, total int
		want     string
	}{{0, 702, "·"}, {1, 702, "<1%"}, {3, 702, "<1%"}, {4, 702, "1%"}, {351, 702, "50%"}, {701, 702, ">99%"}, {702, 702, "100%"}} {
		if got := ShareCell(c.n, c.total); got != c.want {
			t.Errorf("ShareCell(%d, %d) = %q, want %q", c.n, c.total, got, c.want)
		}
	}
}

func TestRunDurationHidesSubMillisecondNoise(t *testing.T) {
	// Durations are stored in whole ms; a mean of 3µs is an artefact.
	if got := RunDuration(3133 * time.Nanosecond); got != "<1ms" {
		t.Errorf("RunDuration(3.1µs) = %q", got)
	}
	if got := RunDuration(94 * time.Millisecond); got != "94ms" {
		t.Errorf("RunDuration(94ms) = %q", got)
	}
}

func TestBuildWarnsWhenIncomplete(t *testing.T) {
	r := Build(sampleData("completed"), 1, t0)
	if r.Complete() {
		t.Fatal("report with a failed job and an unscored result claims to be complete")
	}
	all := strings.Join(r.Warnings, "\n")
	for _, want := range []string{"scores cover 8 of 10 jobs", "1 job failed", "1 result is not scored"} {
		if !strings.Contains(all, want) {
			t.Errorf("warnings %q lack %q", all, want)
		}
	}

	d := sampleData("running")
	d.Jobs = store.Summary{Total: 10, Completed: 6, Queued: 3, Running: 1}
	if w := Build(d, 1, t0).Warnings; len(w) < 2 || !strings.Contains(w[1], "still running: 3 queued, 1 running") {
		t.Errorf("running experiment warnings = %q", w)
	}
}

func TestBuildCompleteHasNoWarnings(t *testing.T) {
	d := sampleData("completed")
	d.Jobs = store.Summary{Total: 8, Completed: 8}
	d.Results.Count, d.Results.Unscored = 8, 0
	if r := Build(d, 1, t0); !r.Complete() {
		t.Errorf("complete experiment warned: %q", r.Warnings)
	}
}

func TestBuildAxes(t *testing.T) {
	r := Build(sampleData("completed"), 1, t0)

	if len(r.Axes) != 1 || r.Axes[0].Param != "speed" {
		t.Fatalf("axes = %+v, want only speed (appears has one value)", r.Axes)
	}
	if len(r.Fixed) != 1 || r.Fixed[0].Param != "appears" {
		t.Errorf("fixed = %+v", r.Fixed)
	}

	ax := r.Axes[0]
	if v, _ := ax.Values[0].Value.AsFloat(); v != 10 {
		t.Errorf("values not in numeric order: first is %v", ax.Values[0].Value)
	}
	slow, fast := ax.Values[0], ax.Values[1]
	if slow.MeanScore != 92 || fast.MeanScore != 100.0/3 {
		t.Errorf("mean scores = %v, %v; want 92, 33.3", slow.MeanScore, fast.MeanScore)
	}
	if math.Abs(ax.Spread-(92-100.0/3)) > 1e-9 {
		t.Errorf("spread = %v", ax.Spread)
	}
	if fast.Outcomes.Count("failed") != 1 || fast.Outcomes.Count("not scored") != 1 || fast.Outcomes.Total != 5 {
		t.Errorf("speed 14 outcomes = %+v", fast.Outcomes)
	}

	// Fixed parameters are left out of the case tables.
	if len(r.CaseParams) != 1 || r.CaseParams[0] != "speed" {
		t.Errorf("case params = %v", r.CaseParams)
	}
}

func TestAxesOrderedBySpread(t *testing.T) {
	cells := []store.AxisCell{
		{Param: "a_weak", Value: core.NumFloat(1), Zone: "green", Count: 1, ScoreSum: 90},
		{Param: "a_weak", Value: core.NumFloat(2), Zone: "green", Count: 1, ScoreSum: 80},
		{Param: "b_strong", Value: core.NumFloat(1), Zone: "green", Count: 1, ScoreSum: 100},
		{Param: "b_strong", Value: core.NumFloat(2), Zone: "red", Count: 1, ScoreSum: 0},
	}
	axes, _ := buildAxes(cells, []string{"green", "red"})
	if axes[0].Param != "b_strong" {
		t.Errorf("most decisive axis is %s, want b_strong", axes[0].Param)
	}
}

func TestAllocateKeepsSmallSegmentsVisible(t *testing.T) {
	b := makeBar([]string{"green", "red"}, map[string]int{"green": 3500, "red": 7}, 3, 0, 3510)
	cells := allocate(b, 40)
	sum := 0
	for i, c := range cells {
		sum += c
		if b.Segments[i].Count > 0 && c == 0 {
			t.Errorf("segment %s with %d jobs got no cell", b.Segments[i].Label, b.Segments[i].Count)
		}
	}
	if sum != 40 {
		t.Errorf("cells sum to %d, want 40", sum)
	}
}

func TestWriteText(t *testing.T) {
	var buf bytes.Buffer
	r := Build(sampleData("completed"), 1, t0)
	if err := WriteText(&buf, r, TextStyle{Width: 20}); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{
		"INCOMPLETE", "Jobs        10", "mean 100ms/job", "Outcomes of all 10 jobs",
		"speed", "Fixed: appears = 20", "Worst 1 — 2 jobs share score 0.0", "collision", "Best 1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("text report lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "\x1b[") {
		t.Error("escape codes in a colourless report")
	}
}

func TestWriteHTMLEscapes(t *testing.T) {
	d := sampleData("completed")
	d.Experiment.Name = `<script>alert(1)</script>`
	var buf bytes.Buffer
	if err := WriteHTML(&buf, Build(d, 1, t0)); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if strings.Contains(out, "<script>") {
		t.Error("experiment name is not escaped")
	}
	for _, want := range []string{"Incomplete", "seg-good", "seg-failed", "collision", "min_distance_m"} {
		if !strings.Contains(out, want) {
			t.Errorf("html lacks %q", want)
		}
	}
}

func TestRateMeterUsesRecentWindow(t *testing.T) {
	m := rateMeter{window: 10 * time.Second}
	m.observe(t0, 0)
	m.observe(t0.Add(5*time.Second), 500) // fast start
	m.observe(t0.Add(20*time.Second), 600)
	got := m.observe(t0.Add(30*time.Second), 700) // then 10 jobs/s
	if math.Abs(got-10) > 1e-9 {
		t.Errorf("rate = %v, want 10 over the last window", got)
	}
}
