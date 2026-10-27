package store

import (
	"context"
	"fmt"
	"uuid"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/job"
	"github.com/jackc/pgx/v5"
)

type ExpiredJob struct {
	ID           uuid.UUID
	ExperimentID uuid.UUID
	Idx          int
	Attempt      int
	Runner       string
}

// ReclaimExpired requeues jobs whose lease ran out and writes a fresh outbox
// row for each, in one transaction. The job's original message has already
// been consumed, so without a new one a requeued job would never be delivered
// again and its experiment would never finish.
func (s *Postgres) ReclaimExpired(ctx context.Context, limit int) ([]ExpiredJob, error) {
	const q = `
		WITH reclaimed AS (
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
			RETURNING id, experiment_id, idx, attempt
		)
		SELECT r.id, r.experiment_id, r.idx, r.attempt, e.runner
		  FROM reclaimed r
		  JOIN experiments e ON e.id = r.experiment_id`

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, q, job.MaxAttempts, limit)
	if err != nil {
		return nil, fmt.Errorf("reclaim: %w", err)
	}
	expired, err := pgx.CollectRows(rows, pgx.RowToStructByPos[ExpiredJob])
	if err != nil {
		return nil, fmt.Errorf("reclaim: %w", err)
	}
	if len(expired) == 0 {
		return nil, nil
	}

	obRows := make([][]any, len(expired))
	for i, j := range expired {
		if obRows[i], err = outboxValues(core.JobID(j.ID),
			core.ExperimentID(j.ExperimentID), j.Runner, j.Idx); err != nil {
			return nil, err
		}
	}
	if err := enqueue(ctx, tx, obRows); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return expired, nil
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
