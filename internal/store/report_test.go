package store_test

import (
	"context"
	"errors"
	"testing"
	"time"
	"uuid"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/dbtest"
	"github.com/eugene-pi/simfleet/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

type seedJob struct {
	speed, appears float64
	state          string
	// result; nil score means no result row at all
	score       *float64
	zone        string
	gatedBy     string
	staleHash   bool // scored with an older spec
	neverScored bool // result saved, scorer has not reached it (hash NULL)
	scoringErr  string
	durationMS  int
}

func f(v float64) *float64 { return &v }

func seedReportJobs(t *testing.T, pool *pgxpool.Pool, expID core.ExperimentID, jobs []seedJob) {
	t.Helper()
	ctx := context.Background()
	var hash string
	if err := pool.QueryRow(ctx, `SELECT scoring_hash FROM experiments WHERE id = $1`,
		uuid.UUID(expID)).Scan(&hash); err != nil {
		t.Fatal(err)
	}
	for i, j := range jobs {
		id := uuid.NewV7()
		params := map[string]any{"speed": j.speed, "appears": j.appears, "decel": 6.0}
		if _, err := pool.Exec(ctx, `
			INSERT INTO jobs (id, experiment_id, idx, params, seed, state, attempt)
			VALUES ($1, $2, $3, $4, 0, $5, 1)`,
			id, uuid.UUID(expID), i, params, j.state); err != nil {
			t.Fatalf("job %d: %v", i, err)
		}
		if j.score == nil && j.scoringErr == "" && !j.neverScored {
			continue
		}
		h := hash
		if j.staleHash {
			h = "old-spec"
		}
		var scoringErr, gatedBy, zone any
		if j.scoringErr != "" {
			scoringErr, h = j.scoringErr, ""
		}
		if j.gatedBy != "" {
			gatedBy = j.gatedBy
		}
		if j.zone != "" {
			zone = j.zone
		}
		var hashArg any
		if h != "" && !j.neverScored {
			hashArg = h
		}
		flags := map[string]bool{"collision": j.gatedBy == "collision"}
		if _, err := pool.Exec(ctx, `
			INSERT INTO job_results (job_id, attempt, metrics, flags, labels, duration_ms,
			                         score, zone, gated_by, scoring_hash, scoring_error)
			VALUES ($1, 1, $2, $3, '{}', $4, $5, $6, $7, $8, $9)`,
			id, map[string]float64{"min_distance_m": float64(i)}, flags, j.durationMS,
			j.score, zone, gatedBy, hashArg, scoringErr); err != nil {
			t.Fatalf("result %d: %v", i, err)
		}
	}
}

func TestReportData(t *testing.T) {
	st, pool := dbtest.NewPostgresAndPool(t)
	ctx := context.Background()

	expID := dbtest.SeedTestExperiment(t, pool)
	seedReportJobs(t, pool, expID, []seedJob{
		{speed: 10, appears: 20, state: "completed", score: f(100), zone: "green", durationMS: 10},
		{speed: 10, appears: 30, state: "completed", score: f(100), zone: "green", durationMS: 20},
		{speed: 14, appears: 20, state: "completed", score: f(0), zone: "red", gatedBy: "collision", durationMS: 30},
		{speed: 14, appears: 30, state: "completed", score: f(60), zone: "yellow", durationMS: 40},
		{speed: 14, appears: 20, state: "failed"},
		{speed: 10, appears: 20, state: "completed", score: f(90), zone: "green", staleHash: true, durationMS: 50},
		{speed: 10, appears: 30, state: "completed", scoringErr: "boom", durationMS: 60},
		{speed: 14, appears: 30, state: "queued"},
		{speed: 10, appears: 30, state: "completed", neverScored: true, durationMS: 0},
	})
	// An unrelated experiment must not leak into the report.
	other := dbtest.SeedTestExperiment(t, pool)
	seedReportJobs(t, pool, other, []seedJob{
		{speed: 99, appears: 99, state: "completed", score: f(0), zone: "red", durationMS: 1000},
	})

	d, err := st.ReportData(ctx, expID, 2)
	if err != nil {
		t.Fatal(err)
	}

	if want := (store.Summary{Total: 9, Completed: 7, Failed: 1, Queued: 1}); d.Jobs != want {
		t.Errorf("jobs = %+v, want %+v", d.Jobs, want)
	}
	res := d.Results
	// Unscored: the stale one and the never-scored one (NULL hash).
	if res.Count != 7 || res.Scored != 4 || res.Unscored != 2 || res.ScoringErrors != 1 {
		t.Errorf("results count/scored/unscored/errors = %d/%d/%d/%d, want 7/4/2/1",
			res.Count, res.Scored, res.Unscored, res.ScoringErrors)
	}
	if res.TotalDuration != 210*time.Millisecond || res.MeanDuration != 30*time.Millisecond ||
		res.MinDuration != 0 || res.MaxDuration != 60*time.Millisecond {
		t.Errorf("durations total/mean/min/max = %v/%v/%v/%v",
			res.TotalDuration, res.MeanDuration, res.MinDuration, res.MaxDuration)
	}
	if res.MinScore != 0 || res.MinScoreTies != 1 || res.MaxScore != 100 || res.MaxScoreTies != 2 {
		t.Errorf("extremes = min %v×%d, max %v×%d", res.MinScore, res.MinScoreTies, res.MaxScore, res.MaxScoreTies)
	}
	// The stale green (score from an older spec) is not counted.
	if d.Zones["green"] != 2 || d.Zones["yellow"] != 1 || d.Zones["red"] != 1 || len(d.Zones) != 3 {
		t.Errorf("zones = %v", d.Zones)
	}

	// Worst: lowest first, with params and gate. Best: ties broken by idx.
	if len(d.Worst) != 2 || d.Worst[0].Idx != 2 || d.Worst[1].Idx != 3 {
		t.Fatalf("worst = %+v", d.Worst)
	}
	w := d.Worst[0]
	if w.GatedBy != "collision" || w.Zone != "red" || !w.Flags["collision"] || w.Metrics["min_distance_m"] != 2 {
		t.Errorf("worst case = %+v", w)
	}
	if v, _ := w.Params["speed"].AsFloat(); v != 14 {
		t.Errorf("worst case speed = %v, want 14", w.Params["speed"])
	}
	if len(d.Best) != 2 || d.Best[0].Idx != 0 || d.Best[1].Idx != 1 {
		t.Errorf("best = %+v", d.Best)
	}

	// Axes: speed 14 has red, yellow, one failed and one queued job.
	type key struct {
		param string
		value float64
		zone  string
		state string
	}
	cells := map[key]store.AxisCell{}
	for _, c := range d.Axes {
		v, _ := c.Value.AsFloat()
		cells[key{c.Param, v, c.Zone, c.State}] = c
	}
	for k, n := range map[key]int{
		{"speed", 14, "red", "completed"}:    1,
		{"speed", 14, "yellow", "completed"}: 1,
		{"speed", 14, "", "failed"}:          1,
		{"speed", 14, "", "queued"}:          1,
		{"speed", 10, "green", "completed"}:  2,
		{"speed", 10, "", "completed"}:       3, // stale, scoring error, never scored
		{"decel", 6, "green", "completed"}:   2,
	} {
		if got := cells[k].Count; got != n {
			t.Errorf("axis cell %+v = %d, want %d", k, got, n)
		}
	}
	if s := cells[key{"speed", 10, "green", "completed"}].ScoreSum; s != 200 {
		t.Errorf("speed 10 score sum = %v, want 200 (stale score excluded)", s)
	}
	if _, ok := cells[key{"speed", 99, "red", "completed"}]; ok {
		t.Error("another experiment's jobs leaked into the axes")
	}

	p, err := st.Progress(ctx, expID)
	if err != nil {
		t.Fatal(err)
	}
	// LastResultAt is a pointer: compare it by value, the rest as a whole.
	if p.Results.LastResultAt == nil || !p.Results.LastResultAt.Equal(*d.Results.LastResultAt) {
		t.Errorf("progress last result at = %v, want %v", p.Results.LastResultAt, d.Results.LastResultAt)
	}
	p.Results.LastResultAt, d.Results.LastResultAt = nil, nil
	if p.Jobs != d.Jobs || p.Results != d.Results {
		t.Errorf("progress differs from report data: %+v", p)
	}
}

func TestReportDataUnknownExperiment(t *testing.T) {
	st, pool := dbtest.NewPostgresAndPool(t)
	ctx := context.Background()

	if _, err := st.LatestExperimentID(ctx); !errors.Is(err, store.ErrExperimentNotFound) {
		t.Errorf("latest on empty db: %v", err)
	}
	_, err := st.ReportData(ctx, core.ExperimentID(uuid.NewV7()), 5)
	if !errors.Is(err, store.ErrExperimentNotFound) {
		t.Errorf("err = %v, want ErrExperimentNotFound", err)
	}

	dbtest.SeedTestExperiment(t, pool)
	newest := dbtest.SeedTestExperiment(t, pool)
	if got, err := st.LatestExperimentID(ctx); err != nil || got != newest {
		t.Errorf("latest = %v, %v; want %v", got, err, newest)
	}

	// An experiment with no jobs at all still reports, with zeros.
	d, err := st.ReportData(ctx, newest, 5)
	if err != nil {
		t.Fatal(err)
	}
	if d.Jobs.Total != 0 || d.Results.Count != 0 || len(d.Worst) != 0 || len(d.Axes) != 0 {
		t.Errorf("empty experiment = %+v", d)
	}
}
