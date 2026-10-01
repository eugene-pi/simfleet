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

const leaseDuration = 60 * time.Second

var ErrNotClaimable = errors.New("job is not claimable")
var ErrJobNotFound = errors.New("job is not found")

// Claim переводит задание в running и увеличивает номер попытки.
// Возвращает номер попытки — маркер ограждения для последующей записи результата.
func (s *Postgres) Claim(ctx context.Context, jobID core.JobID, workerID string) (int, error) {
	const q = `
		UPDATE jobs
		   SET state       = 'running',
		       attempt     = attempt + 1,
		       lease_until = now() + $3::interval,
		       worker_id   = $2,
		       updated_at  = now()
		 WHERE id = $1 AND state = 'queued'
		RETURNING attempt`

	var attempt int
	err := s.pool.QueryRow(ctx, q, jobID, workerID, leaseDuration.String()).Scan(&attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotClaimable
	}
	if err != nil {
		return 0, fmt.Errorf("claim job %s: %w", jobID, err)
	}
	return attempt, nil
}

var ErrFenced = errors.New("result rejected: stale attempt")

func (s *Postgres) SaveResult(ctx context.Context, r ResultRecord) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	const upd = `
		UPDATE jobs
		   SET state = 'completed', lease_until = NULL,
		       worker_id = NULL, updated_at = now()
		 WHERE id = $1 AND attempt = $2 AND state = 'running'`

	tag, err := tx.Exec(ctx, upd, r.JobID, r.Attempt)
	if err != nil {
		return fmt.Errorf("update job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrFenced
	}

	const ins = `
		INSERT INTO job_results
		    (job_id, attempt, metrics, flags, labels, artifact_key, duration_ms)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (job_id) DO NOTHING`

	if _, err := tx.Exec(ctx, ins, r.JobID, r.Attempt, r.Metrics,
		r.Flags, r.Labels, r.ArtifactKey, r.DurationMS); err != nil {
		return fmt.Errorf("insert result: %w", err)
	}
	return tx.Commit(ctx)
}

// internal/store/jobs.go
func (p *Postgres) LoadTask(ctx context.Context, jobID core.JobID) (core.Task, error) {
	const q = `
		SELECT j.id, j.experiment_id, j.idx, j.params, j.seed, e.runner
		  FROM jobs j
		  JOIN experiments e ON e.id = j.experiment_id
		 WHERE j.id = $1`

	var (
		t          core.Task
		id, expID  uuid.UUID
		paramsJSON []byte
	)
	err := p.pool.QueryRow(ctx, q, uuid.UUID(jobID)).
		Scan(&id, &expID, &t.Idx, &paramsJSON, &t.Seed, &t.Runner)
	if errors.Is(err, pgx.ErrNoRows) {
		return core.Task{}, ErrJobNotFound
	}
	if err != nil {
		return core.Task{}, fmt.Errorf("load task %s: %w", jobID, err)
	}

	if err := json.Unmarshal(paramsJSON, &t.Params); err != nil {
		return core.Task{}, fmt.Errorf("unmarshal params: %w", err)
	}
	t.JobID = core.JobID(id)
	t.ExperimentID = core.ExperimentID(expID)
	return t, nil
}

func (p *Postgres) MarkFailed(ctx context.Context, jobID core.JobID, attempt int, cause string) error {
	return p.finishWith(ctx, jobID, attempt, "failed", cause)
}

func (p *Postgres) ReleaseForRetry(ctx context.Context, jobID core.JobID, attempt int, cause string) error {
	return p.finishWith(ctx, jobID, attempt, "queued", cause)
}

func (p *Postgres) finishWith(ctx context.Context, jobID core.JobID,
	attempt int, state string, cause string) error {

	const q = `
		UPDATE jobs
		   SET state = $3::job_state, lease_until = NULL, worker_id = NULL,
		       last_error = $4, updated_at = now()
		 WHERE id = $1 AND attempt = $2 AND state = 'running'`

	tag, err := p.pool.Exec(ctx, q, uuid.UUID(jobID), attempt, state, cause)
	if err != nil {
		return fmt.Errorf("finish job %s: %w", jobID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrFenced
	}
	return nil
}

func (p *Postgres) NextQueued(ctx context.Context, expID core.ExperimentID, limit int) ([]core.JobID, error) {
	const q = `
		SELECT id FROM jobs
		 WHERE experiment_id = $1 AND state = 'queued'
		 ORDER BY idx
		 LIMIT $2`

	rows, err := p.pool.Query(ctx, q, uuid.UUID(expID), limit)
	if err != nil {
		return nil, fmt.Errorf("next queued: %w", err)
	}
	defer rows.Close()

	var out []core.JobID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, core.JobID(id))
	}
	return out, rows.Err()
}

type Summary struct {
	Total     int
	Queued    int
	Running   int
	Completed int
	Failed    int
	Cancelled int
}

func (p *Postgres) Summary(ctx context.Context, expID core.ExperimentID) (Summary, error) {
	const q = `
		SELECT state, count(*) FROM jobs
		 WHERE experiment_id = $1
		 GROUP BY state`

	rows, err := p.pool.Query(ctx, q, uuid.UUID(expID))
	if err != nil {
		return Summary{}, fmt.Errorf("summary: %w", err)
	}
	defer rows.Close()

	var s Summary
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return Summary{}, err
		}
		s.Total += n
		switch state {
		case "queued":
			s.Queued = n
		case "running":
			s.Running = n
		case "completed":
			s.Completed = n
		case "failed":
			s.Failed = n
		case "cancelled":
			s.Cancelled = n
		}
	}
	return s, rows.Err()
}
