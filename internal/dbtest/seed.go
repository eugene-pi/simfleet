package dbtest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"testing"
	"uuid"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/job"
	"github.com/jackc/pgx/v5/pgxpool"
)

// seedQueuedJob inserts an experiment with a single job in the queued state.
func SeedQueuedJob(t *testing.T, pool *pgxpool.Pool, expID core.ExperimentID, idx int) core.JobID {
	t.Helper()
	ctx := context.Background()
	jobID := core.JobID(uuid.NewV7())

	const insJob = `
		INSERT INTO jobs (id, experiment_id, idx, params, seed, state, attempt)
		VALUES ($1, $2, $3, '{}'::jsonb, 0, $4, 0)`
	if _, err := pool.Exec(ctx, insJob, uuid.UUID(jobID), uuid.UUID(expID), idx, job.StateQueued); err != nil {
		t.Fatalf("insert job %d: %v", idx, err)
	}

	return jobID
}

// seedExperiment inserts an experiment directly, bypassing the expander: the
// test needs a known scoring spec, not a real parameter sweep.
func SeedExperiment(t *testing.T, pool *pgxpool.Pool, spec core.Spec) core.ExperimentID {
	t.Helper()
	ctx := context.Background()

	specJSON, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}
	scoringJSON, err := json.Marshal(spec.Scoring)
	if err != nil {
		t.Fatalf("marshal scoring: %v", err)
	}

	id := core.ExperimentID(uuid.NewV7())
	const q = `
		INSERT INTO experiments
		    (id, name, runner, spec, scoring, scoring_hash, state, total_jobs, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, 'running', 0, 'test')`

	if _, err := pool.Exec(ctx, q, uuid.UUID(id), spec.Name, spec.Runner,
		specJSON, scoringJSON, hashOf(scoringJSON)); err != nil {
		t.Fatalf("insert experiment: %v", err)
	}
	return id
}

func SeedTestExperiment(t *testing.T, pool *pgxpool.Pool) core.ExperimentID {
	return SeedExperiment(t, pool, core.Spec{
		Name:   "scoring-test",
		Runner: "fake",
		Scoring: core.Scoring{
			Gates:      []core.Gate{{When: "collision", Score: 0}},
			Components: []core.ScoreComponent{{Metric: "dist", Good: 3.0, Bad: 0.5}},
			Combine:    "min",
		},
		Zones: []core.Zone{{Name: "green", Min: 80}, {Name: "yellow", Min: 50}, {Name: "red", Min: 0}},
	})
}

// seedResult inserts a completed job together with its result. The job row is
// required first: job_results references it.
func SeedResult(t *testing.T, pool *pgxpool.Pool, expID core.ExperimentID,
	idx int, metrics map[string]float64, flags map[string]bool) core.JobID {
	t.Helper()
	return SeedJob(t, pool, expID, idx, metrics, flags, job.StateCompleted)
}

func SeedJob(t *testing.T, pool *pgxpool.Pool, expID core.ExperimentID,
	idx int, metrics map[string]float64, flags map[string]bool, state job.State) core.JobID {
	t.Helper()
	ctx := context.Background()

	if flags == nil {
		flags = map[string]bool{}
	}
	metricsJSON, err := json.Marshal(metrics)
	if err != nil {
		t.Fatalf("marshal metrics: %v", err)
	}
	flagsJSON, err := json.Marshal(flags)
	if err != nil {
		t.Fatalf("marshal flags: %v", err)
	}

	jobID := core.JobID(uuid.NewV7())

	const insJob = `
		INSERT INTO jobs (id, experiment_id, idx, params, seed, state, attempt)
		VALUES ($1, $2, $3, '{}'::jsonb, 0, $4, 1)`
	if _, err := pool.Exec(ctx, insJob, uuid.UUID(jobID), uuid.UUID(expID), idx, state); err != nil {
		t.Fatalf("insert job %d: %v", idx, err)
	}

	const insRes = `
		INSERT INTO job_results (job_id, attempt, metrics, flags, labels, duration_ms)
		VALUES ($1, 1, $2, $3, '{}'::jsonb, 1)`
	if _, err := pool.Exec(ctx, insRes, uuid.UUID(jobID), metricsJSON, flagsJSON); err != nil {
		t.Fatalf("insert result %d: %v", idx, err)
	}
	return jobID
}

// SeedExperimentWithJobs creates an experiment with n queued jobs and the
// matching outbox rows — the same state CreateExperiment leaves behind.
// The outbox part is what the publisher test actually operates on.
func SeedExperimentWithJobs(t *testing.T, pool *pgxpool.Pool, n int) core.ExperimentID {
	t.Helper()
	ctx := context.Background()

	expID := SeedExperiment(t, pool, core.Spec{Name: "pub-test", Runner: "fake"})

	for i := range n {
		jobID := SeedQueuedJob(t, pool, expID, i)

		payload, err := json.Marshal(core.JobMessage{
			JobID:        jobID,
			ExperimentID: expID,
			Runner:       "fake",
			Idx:          i,
		})
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}

		const q = `INSERT INTO outbox (topic, msg_key, payload) VALUES ($1, $2, $3)`
		if _, err := pool.Exec(ctx, q, "sim.jobs", jobID.String(), payload); err != nil {
			t.Fatalf("insert outbox %d: %v", i, err)
		}
	}
	return expID
}

// expireLease pushes the lease into the past so the reaper treats the job as
// orphaned. Faster and more deterministic than waiting out a real lease.
func ExpireLease(t *testing.T, pool *pgxpool.Pool, jobID core.JobID) {
	t.Helper()
	const q = `UPDATE jobs SET lease_until = now() - interval '1 second' WHERE id = $1`
	tag, err := pool.Exec(context.Background(), q, uuid.UUID(jobID))
	if err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("expire lease: affected %d rows, want 1", tag.RowsAffected())
	}
}

// JobState returns the current state of a job.
func JobState(t *testing.T, pool *pgxpool.Pool, jobID core.JobID) string {
	t.Helper()
	var s string
	err := pool.QueryRow(context.Background(),
		`SELECT state FROM jobs WHERE id = $1`, uuid.UUID(jobID)).Scan(&s)
	if err != nil {
		t.Fatalf("job state: %v", err)
	}
	return s
}

// hashOf gives a stable fingerprint of the scoring spec. Any change to the
// spec changes the hash, which is what triggers rescoring.
func hashOf(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
