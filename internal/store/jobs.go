package store

import (
	"context"
	"errors"
	"fmt"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
)

const leaseDuration = 60 * time.Second

var ErrNotClaimable = errors.New("job is not claimable")

// Claim переводит задание в running и увеличивает номер попытки.
// Возвращает номер попытки — маркер ограждения для последующей записи результата.
func (s *Store) Claim(ctx context.Context, jobID uuid.UUID, workerID string) (int, error) {
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

func (s *Store) SaveResult(ctx context.Context, r ResultRecord) error {
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
