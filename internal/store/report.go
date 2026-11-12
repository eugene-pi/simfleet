package store

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"time"
	"uuid"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/jackc/pgx/v5"
)

var ErrExperimentNotFound = errors.New("experiment not found")

// ReportData is everything the experiment report is built from, read in one
// pass. It holds raw counts; shares, ordering and wording are the report
// package's business.
//
// "Scored" everywhere below means scored against the experiment's *current*
// scoring spec: a score left over from an older spec (hash mismatch) is stale
// and counted as unscored, so a threshold change never mixes two rule sets
// into one report.
type ReportData struct {
	ProgressData
	Axes  []AxisCell
	Worst []CaseRow // lowest scores first
	Best  []CaseRow // highest scores first
}

// ProgressData is the cheap subset of ReportData, read on every tick of a
// live view: no per-parameter breakdown, no case lists.
type ProgressData struct {
	Experiment ExperimentInfo
	Jobs       Summary // job counts by state
	Results    ResultStats
	Zones      map[string]int // scored results per zone
}

type ExperimentInfo struct {
	ID          core.ExperimentID
	Name        string
	Runner      string
	State       string
	Spec        core.Spec
	CreatedAt   time.Time
	CompletedAt *time.Time
}

type ResultStats struct {
	Count         int // result rows (failed jobs have none)
	Scored        int // scored with the current spec
	Unscored      int // waiting for (re)scoring
	ScoringErrors int // scoring gave up on these

	TotalDuration time.Duration // sum of run durations (compute time)
	MeanDuration  time.Duration
	MinDuration   time.Duration
	MaxDuration   time.Duration
	LastResultAt  *time.Time

	// How many scored results share the extreme scores. With many jobs at 0
	// (collision gate) or 100 the "N worst" list is an arbitrary slice of a
	// tie, and the report has to say so.
	MinScore, MaxScore         float64
	MinScoreTies, MaxScoreTies int
}

// AxisCell is one cell of the (parameter, value) × outcome table. Zone is
// empty when the job has no current score; State then tells failed apart
// from not-yet-finished or not-yet-scored.
type AxisCell struct {
	Param    string
	Value    core.ParamValue
	Zone     string
	State    string
	Count    int
	ScoreSum float64 // sum of current scores in this cell, for mean score per value
}

// CaseRow is one job in the worst/best lists, with the parameters that
// produced it — an id alone answers nothing.
type CaseRow struct {
	JobID   core.JobID
	Idx     int
	Params  map[string]core.ParamValue
	Score   float64
	Zone    string
	GatedBy string
	Metrics map[string]float64
	Flags   map[string]bool
}

// LatestExperimentID returns the most recently created experiment.
func (p *Postgres) LatestExperimentID(ctx context.Context) (core.ExperimentID, error) {
	var id uuid.UUID
	err := p.pool.QueryRow(ctx,
		`SELECT id FROM experiments ORDER BY created_at DESC, id DESC LIMIT 1`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return core.ExperimentID{}, ErrExperimentNotFound
	}
	if err != nil {
		return core.ExperimentID{}, fmt.Errorf("latest experiment: %w", err)
	}
	return core.ExperimentID(id), nil
}

// ReportData reads the data for an experiment report. n is the length of the
// worst and best lists.
//
// The queries run in one REPEATABLE READ read-only transaction so that the
// counts, the axis table and the case lists describe the same moment — the
// report is often run while workers and the scorer are still writing.
func (p *Postgres) ReportData(ctx context.Context, expID core.ExperimentID, n int) (ReportData, error) {
	tx, err := p.snapshot(ctx)
	if err != nil {
		return ReportData{}, err
	}
	defer tx.Rollback(ctx)

	id := uuid.UUID(expID)
	var d ReportData

	if d.ProgressData, err = readProgress(ctx, tx, id); err != nil {
		return ReportData{}, err
	}
	if d.Axes, err = reportAxes(ctx, tx, id); err != nil {
		return ReportData{}, err
	}
	if d.Worst, err = reportCases(ctx, tx, id, n, "ASC"); err != nil {
		return ReportData{}, err
	}
	if d.Best, err = reportCases(ctx, tx, id, n, "DESC"); err != nil {
		return ReportData{}, err
	}
	return d, tx.Commit(ctx)
}

// Progress reads the live-view subset of the report.
func (p *Postgres) Progress(ctx context.Context, expID core.ExperimentID) (ProgressData, error) {
	tx, err := p.snapshot(ctx)
	if err != nil {
		return ProgressData{}, err
	}
	defer tx.Rollback(ctx)

	d, err := readProgress(ctx, tx, uuid.UUID(expID))
	if err != nil {
		return ProgressData{}, err
	}
	return d, tx.Commit(ctx)
}

func (p *Postgres) snapshot(ctx context.Context) (pgx.Tx, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	return tx, nil
}

func readProgress(ctx context.Context, tx pgx.Tx, id uuid.UUID) (ProgressData, error) {
	var (
		d   ProgressData
		err error
	)
	if d.Experiment, err = reportExperiment(ctx, tx, id); err != nil {
		return ProgressData{}, err
	}
	if d.Jobs, err = reportJobs(ctx, tx, id); err != nil {
		return ProgressData{}, err
	}
	if d.Results, d.Zones, err = reportResults(ctx, tx, id); err != nil {
		return ProgressData{}, err
	}
	return d, nil
}

func reportExperiment(ctx context.Context, tx pgx.Tx, id uuid.UUID) (ExperimentInfo, error) {
	const q = `
		SELECT name, runner, state::text, spec, created_at, completed_at
		  FROM experiments WHERE id = $1`

	e := ExperimentInfo{ID: core.ExperimentID(id)}
	var specJSON []byte
	err := tx.QueryRow(ctx, q, id).Scan(&e.Name, &e.Runner, &e.State,
		&specJSON, &e.CreatedAt, &e.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ExperimentInfo{}, fmt.Errorf("%w: %s", ErrExperimentNotFound, id)
	}
	if err != nil {
		return ExperimentInfo{}, fmt.Errorf("load experiment: %w", err)
	}
	if err := json.Unmarshal(specJSON, &e.Spec); err != nil {
		return ExperimentInfo{}, fmt.Errorf("spec of %s: %w", id, err)
	}
	return e, nil
}

func reportJobs(ctx context.Context, tx pgx.Tx, id uuid.UUID) (Summary, error) {
	const q = `
		SELECT
		    count(*),
		    count(*) FILTER (WHERE state = 'queued'),
		    count(*) FILTER (WHERE state = 'running'),
		    count(*) FILTER (WHERE state = 'completed'),
		    count(*) FILTER (WHERE state = 'failed'),
		    count(*) FILTER (WHERE state = 'cancelled')
		  FROM jobs WHERE experiment_id = $1`

	var s Summary
	err := tx.QueryRow(ctx, q, id).Scan(&s.Total, &s.Queued, &s.Running,
		&s.Completed, &s.Failed, &s.Cancelled)
	if err != nil {
		return Summary{}, fmt.Errorf("job summary: %w", err)
	}
	return s, nil
}

// scoredCond selects results scored with the experiment's current spec.
// Requires the aliases r (job_results) and e (experiments).
//
// Never NULL: a never-scored row has scoring_hash NULL, and a bare
// "r.scoring_hash = e.scoring_hash" would make the whole condition NULL —
// so NOT scored would be NULL too, and such rows would silently drop out of
// both the scored and the unscored counts (same trap as in D-1).
const scoredCond = `(r.scoring_hash IS NOT NULL AND r.scoring_hash = e.scoring_hash
	AND r.scoring_error IS NULL)`

func reportResults(ctx context.Context, tx pgx.Tx, id uuid.UUID) (ResultStats, map[string]int, error) {
	const q = `
		WITH res AS (
			SELECT r.*, (` + scoredCond + `) AS scored
			  FROM job_results r
			  JOIN jobs j        ON j.id = r.job_id
			  JOIN experiments e ON e.id = j.experiment_id
			 WHERE j.experiment_id = $1
		), ext AS (
			SELECT min(score) AS lo, max(score) AS hi FROM res WHERE scored
		)
		SELECT
		    count(*),
		    count(*) FILTER (WHERE scored),
		    count(*) FILTER (WHERE NOT scored AND scoring_error IS NULL),
		    count(*) FILTER (WHERE scoring_error IS NOT NULL),
		    coalesce(sum(duration_ms), 0)::bigint,
		    coalesce(avg(duration_ms), 0)::float8,
		    coalesce(min(duration_ms), 0),
		    coalesce(max(duration_ms), 0),
		    max(created_at),
		    coalesce((SELECT lo FROM ext), 0)::float8,
		    coalesce((SELECT hi FROM ext), 0)::float8,
		    count(*) FILTER (WHERE scored AND score = (SELECT lo FROM ext)),
		    count(*) FILTER (WHERE scored AND score = (SELECT hi FROM ext))
		  FROM res`

	var (
		s            ResultStats
		sumMS        int64
		meanMS       float64
		minMS, maxMS int
	)
	err := tx.QueryRow(ctx, q, id).Scan(&s.Count, &s.Scored, &s.Unscored,
		&s.ScoringErrors, &sumMS, &meanMS, &minMS, &maxMS, &s.LastResultAt,
		&s.MinScore, &s.MaxScore, &s.MinScoreTies, &s.MaxScoreTies)
	if err != nil {
		return ResultStats{}, nil, fmt.Errorf("result stats: %w", err)
	}
	s.TotalDuration = time.Duration(sumMS) * time.Millisecond
	s.MeanDuration = time.Duration(meanMS * float64(time.Millisecond))
	s.MinDuration = time.Duration(minMS) * time.Millisecond
	s.MaxDuration = time.Duration(maxMS) * time.Millisecond

	const qz = `
		SELECT r.zone, count(*)
		  FROM job_results r
		  JOIN jobs j        ON j.id = r.job_id
		  JOIN experiments e ON e.id = j.experiment_id
		 WHERE j.experiment_id = $1 AND ` + scoredCond + `
		 GROUP BY r.zone`

	rows, err := tx.Query(ctx, qz, id)
	if err != nil {
		return ResultStats{}, nil, fmt.Errorf("zone counts: %w", err)
	}
	defer rows.Close()

	zones := map[string]int{}
	for rows.Next() {
		var (
			zone *string
			n    int
		)
		if err := rows.Scan(&zone, &n); err != nil {
			return ResultStats{}, nil, err
		}
		if zone != nil {
			zones[*zone] += n
		}
	}
	return s, zones, rows.Err()
}

// reportAxes counts outcomes per value of every swept parameter: one row per
// (parameter, value, zone, job state). jsonb_each unpacks the params object,
// so every parameter is covered in a single scan of the experiment's jobs.
// Jobs without a result are kept (LEFT JOIN): a value that makes jobs fail
// is as much an answer as a value that makes them red.
func reportAxes(ctx context.Context, tx pgx.Tx, id uuid.UUID) ([]AxisCell, error) {
	const q = `
		SELECT p.key, p.value,
		       CASE WHEN ` + scoredCond + ` THEN r.zone END AS zone,
		       j.state::text,
		       count(*),
		       coalesce(sum(r.score) FILTER (WHERE ` + scoredCond + `), 0)::float8
		  FROM jobs j
		  JOIN experiments e     ON e.id = j.experiment_id
		  LEFT JOIN job_results r ON r.job_id = j.id
		 CROSS JOIN LATERAL jsonb_each(j.params) AS p
		 WHERE j.experiment_id = $1
		 GROUP BY 1, 2, 3, 4`

	rows, err := tx.Query(ctx, q, id)
	if err != nil {
		return nil, fmt.Errorf("axis breakdown: %w", err)
	}
	defer rows.Close()

	var out []AxisCell
	for rows.Next() {
		var (
			c         AxisCell
			valueJSON []byte
			zone      *string
		)
		if err := rows.Scan(&c.Param, &valueJSON, &zone, &c.State, &c.Count, &c.ScoreSum); err != nil {
			return nil, fmt.Errorf("scan axis cell: %w", err)
		}
		if err := json.Unmarshal(valueJSON, &c.Value); err != nil {
			return nil, fmt.Errorf("value of %s: %w", c.Param, err)
		}
		if zone != nil {
			c.Zone = *zone
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// reportCases returns the n lowest (dir = "ASC") or highest ("DESC") scored
// jobs. Ties are broken by idx so the report is stable between runs.
func reportCases(ctx context.Context, tx pgx.Tx, id uuid.UUID, n int, dir string) ([]CaseRow, error) {
	if n <= 0 {
		return nil, nil
	}
	if dir != "ASC" && dir != "DESC" {
		return nil, fmt.Errorf("bad sort direction %q", dir)
	}
	q := `
		SELECT j.id, j.idx, j.params, r.score, coalesce(r.zone, ''),
		       coalesce(r.gated_by, ''), r.metrics, r.flags
		  FROM job_results r
		  JOIN jobs j        ON j.id = r.job_id
		  JOIN experiments e ON e.id = j.experiment_id
		 WHERE j.experiment_id = $1 AND ` + scoredCond + `
		 ORDER BY r.score ` + dir + `, j.idx
		 LIMIT $2`

	rows, err := tx.Query(ctx, q, id, n)
	if err != nil {
		return nil, fmt.Errorf("cases: %w", err)
	}
	defer rows.Close()

	var out []CaseRow
	for rows.Next() {
		var (
			c                                  CaseRow
			jobID                              uuid.UUID
			paramsJSON, metricsJSON, flagsJSON []byte
		)
		if err := rows.Scan(&jobID, &c.Idx, &paramsJSON, &c.Score, &c.Zone,
			&c.GatedBy, &metricsJSON, &flagsJSON); err != nil {
			return nil, fmt.Errorf("scan case: %w", err)
		}
		c.JobID = core.JobID(jobID)
		if err := json.Unmarshal(paramsJSON, &c.Params); err != nil {
			return nil, fmt.Errorf("params of job %d: %w", c.Idx, err)
		}
		if err := json.Unmarshal(metricsJSON, &c.Metrics); err != nil {
			return nil, fmt.Errorf("metrics of job %d: %w", c.Idx, err)
		}
		if err := json.Unmarshal(flagsJSON, &c.Flags); err != nil {
			return nil, fmt.Errorf("flags of job %d: %w", c.Idx, err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
