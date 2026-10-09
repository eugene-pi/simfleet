package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"uuid"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/jackc/pgx/v5"
)

type NewExperiment struct {
	ID     core.ExperimentID
	Spec   core.Spec
	Runner string
	Jobs   []NewJob
}

type NewJob struct {
	ID     core.JobID
	Idx    int
	Params map[string]core.ParamValue
	Seed   int64
}

// internal/store/experiments.go
func (p *Postgres) CreateExperiment(ctx context.Context, exp NewExperiment) error {
	specJSON, err := json.Marshal(exp.Spec)
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}
	scoringJSON, err := json.Marshal(exp.Spec.Scoring)
	if err != nil {
		return fmt.Errorf("marshal scoring: %w", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	const insExp = `
		INSERT INTO experiments
		    (id, name, runner, spec, scoring, scoring_hash, state, total_jobs, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, 'running', $7, $8)`

	_, err = tx.Exec(ctx, insExp,
		uuid.UUID(exp.ID), exp.Spec.Name, exp.Spec.Runner,
		specJSON, scoringJSON, scoringHash(exp.Spec.Scoring),
		len(exp.Jobs), "local")
	if err != nil {
		return fmt.Errorf("insert experiment: %w", err)
	}

	rows := make([][]any, len(exp.Jobs))
	for i, j := range exp.Jobs {
		paramsJSON, err := json.Marshal(j.Params)
		if err != nil {
			return fmt.Errorf("marshal params of job %d: %w", j.Idx, err)
		}
		rows[i] = []any{
			uuid.UUID(j.ID), uuid.UUID(exp.ID), j.Idx, paramsJSON, j.Seed,
		}
	}

	_, err = tx.CopyFrom(ctx,
		pgx.Identifier{"jobs"},
		[]string{"id", "experiment_id", "idx", "params", "seed"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copy jobs: %w", err)
	}

	return tx.Commit(ctx)
}

func scoringHash(s core.Scoring) string {
	b, _ := json.Marshal(s)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// internal/store/experiments.go
func (p *Postgres) FinalizeCompleted(ctx context.Context) (int, error) {
	const q = `
		UPDATE experiments e
		   SET state = 'completed', completed_at = now()
		 WHERE e.state = 'running'
		   AND NOT EXISTS (
		       SELECT 1 FROM jobs j
		        WHERE j.experiment_id = e.id
		          AND j.state IN ('queued', 'running')
		   )`
	tag, err := p.pool.Exec(ctx, q)
	return int(tag.RowsAffected()), err
}
