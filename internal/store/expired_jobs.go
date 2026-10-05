package store

import (
	"context"
	"fmt"
	"uuid"

	"github.com/eugene-pi/simfleet/internal/job"
	"github.com/jackc/pgx/v5"
)

type ExpiredJob struct {
	ID           uuid.UUID
	ExperimentID uuid.UUID
	Idx          int
	Attempt      int
}

func (s *Postgres) ReclaimExpired(ctx context.Context, limit int) ([]ExpiredJob, error) {
	const q = `
		UPDATE jobs
		   SET state = 'queued', lease_until = NULL,
		       worker_id = NULL, updated_at = now()
		 WHERE id IN (
		       SELECT id FROM jobs
		        WHERE state = 'running'
		          AND lease_until < now()
		          AND attempt < $1
		        ORDER BY lease_until
		        LIMIT $2
		        FOR UPDATE SKIP LOCKED
		 )
		RETURNING id, experiment_id, idx, attempt`

	rows, err := s.pool.Query(ctx, q, job.MaxAttempts, limit)
	if err != nil {
		return nil, fmt.Errorf("reclaim: %w", err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByPos[ExpiredJob])
}

func (p *Postgres) FailExhausted(ctx context.Context, limit int) (int, error) {
	const q = `
		UPDATE jobs
		   SET state = 'failed', lease_until = NULL, worker_id = NULL,
		       last_error = 'attempts exhausted', updated_at = now()
		 WHERE state = 'running' AND lease_until < now() AND attempt >= $1`
	tag, err := p.pool.Exec(ctx, q, job.MaxAttempts)
	return int(tag.RowsAffected()), err
}
