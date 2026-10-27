package store

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"time"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/jackc/pgx/v5"
)

// outboxValues builds the outbox row for one job. Every path that makes a job
// runnable — submit and every requeue — goes through here, so the message
// format has a single definition.
func outboxValues(jobID core.JobID, expID core.ExperimentID, runner string, idx int) ([]any, error) {
	payload, err := json.Marshal(core.JobMessage{
		JobID:        jobID,
		ExperimentID: expID,
		Runner:       runner,
		Idx:          idx,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal outbox payload: %w", err)
	}
	return []any{runner, jobID.String(), payload}, nil
}

// enqueue writes outbox rows inside the caller's transaction, so the message
// commits or rolls back together with the job state change that needs it.
func enqueue(ctx context.Context, tx pgx.Tx, rows [][]any) error {
	if _, err := tx.CopyFrom(ctx,
		pgx.Identifier{"outbox"},
		[]string{"runner", "msg_key", "payload"},
		pgx.CopyFromRows(rows),
	); err != nil {
		return fmt.Errorf("copy outbox: %w", err)
	}
	return nil
}

// OutboxRow is a message waiting to be sent. Runner says what kind of job it
// is; mapping that to a broker destination is the bus's job, not the store's.
type OutboxRow struct {
	ID      int64
	Runner  string
	Key     string
	Payload []byte
}

// FetchUnpublished locks a batch of unsent rows. SKIP LOCKED lets several
// controller instances publish in parallel without stepping on each other.
// returns data and active transaction. This keeps locked db records while we sending them.
// when the sending is complete, we shall commit the transaction
func (p *Postgres) FetchUnpublished(ctx context.Context, limit int) ([]OutboxRow, pgx.Tx, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}

	const q = `
		SELECT id, runner, msg_key, payload
		  FROM outbox
		 WHERE published_at IS NULL
		 ORDER BY id
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`

	rows, err := tx.Query(ctx, q, limit)
	if err != nil {
		tx.Rollback(ctx)
		return nil, nil, err
	}
	defer rows.Close()
	out := make([]OutboxRow, 0, limit)
	for rows.Next() {
		var id int64
		var runner, msg_key string
		var payload []byte
		_ = rows.Scan(&id, &runner, &msg_key, &payload)
		out = append(out, OutboxRow{
			ID:      id,
			Runner:  runner,
			Key:     msg_key,
			Payload: payload,
		})
	}
	return out, tx, nil
}

// MarkPublished stamps the rows as sent. It runs inside the caller's
// transaction — the same one holding FOR UPDATE on these rows — so the lock,
// the Kafka send and the stamp form one unit of work.
func (p *Postgres) MarkPublished(ctx context.Context, tx pgx.Tx, ids []int64) error {
	const q = `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`
	_, err := tx.Exec(ctx, q, ids)
	if err != nil {
		return fmt.Errorf("mark published: %w", err)
	}
	return nil
}

// PurgePublished deletes outbox rows published before the cutoff. Published
// rows are kept for a while because they show what was sent and when, which
// helps when investigating a lost job.
func (p *Postgres) PurgePublished(ctx context.Context, olderThan time.Duration, limit int) (int, error) {

	const q = `
		DELETE FROM outbox
		 WHERE id IN (
		       SELECT id FROM outbox
		        WHERE published_at IS NOT NULL
		          AND published_at < now() - $1::interval
		        ORDER BY id
		        LIMIT $2
		 )`

	tag, err := p.pool.Exec(ctx, q, olderThan.String(), limit)
	if err != nil {
		return 0, fmt.Errorf("purge outbox: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
