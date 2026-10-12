package store

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"uuid"

	"github.com/eugene-pi/simfleet/internal/core"
)

func (p *Postgres) UnscoredResults(ctx context.Context, limit int) ([]core.PendingEvaluation, error) {
	const q = `
		SELECT r.job_id, r.metrics, r.flags,
		       e.scoring, e.spec -> 'zones', e.scoring_hash
		  FROM job_results r
		  JOIN jobs j        ON j.id = r.job_id
		  JOIN experiments e ON e.id = j.experiment_id
		 WHERE r.scoring_hash IS DISTINCT FROM e.scoring_hash
		   AND r.scoring_error IS NULL
		 ORDER BY r.created_at
		 LIMIT $1`

	rows, err := p.pool.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("unscored results: %w", err)
	}
	defer rows.Close()

	var out []core.PendingEvaluation
	for rows.Next() {
		var (
			id                     uuid.UUID
			metricsJSON, flagsJSON []byte
			scoringJSON, zonesJSON []byte
			hash                   *string
		)
		if err := rows.Scan(&id, &metricsJSON, &flagsJSON,
			&scoringJSON, &zonesJSON, &hash); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		pe := core.PendingEvaluation{JobID: core.JobID(id)}
		if hash != nil {
			pe.ScoringHash = *hash
		}
		if err := json.Unmarshal(metricsJSON, &pe.Metrics); err != nil {
			return nil, fmt.Errorf("metrics of %s: %w", id, err)
		}
		if err := json.Unmarshal(flagsJSON, &pe.Flags); err != nil {
			return nil, fmt.Errorf("flags of %s: %w", id, err)
		}
		if err := json.Unmarshal(scoringJSON, &pe.Scoring); err != nil {
			return nil, fmt.Errorf("scoring of %s: %w", id, err)
		}
		if len(zonesJSON) > 0 {
			if err := json.Unmarshal(zonesJSON, &pe.Zones); err != nil {
				return nil, fmt.Errorf("zones of %s: %w", id, err)
			}
		}
		out = append(out, pe)
	}
	return out, rows.Err()
}

func (p *Postgres) SaveEvaluation(ctx context.Context, jobID core.JobID,
	ev core.Evaluation, scoringHash string) error {

	const q = `
		UPDATE job_results
		   SET score = $2, zone = $3, components = $4,
		       gated_by = $5, scoring_hash = $6
		 WHERE job_id = $1`

	comps, err := json.Marshal(ev.Components)
	if err != nil {
		return err
	}
	var gatedBy any
	if ev.GatedBy != "" {
		gatedBy = ev.GatedBy
	}

	_, err = p.pool.Exec(ctx, q, uuid.UUID(jobID), ev.Score, ev.Zone,
		comps, gatedBy, scoringHash)
	return err
}
