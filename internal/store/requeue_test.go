package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/dbtest"
	"github.com/eugene-pi/simfleet/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

// A reclaimed job's original message is already consumed. Unless the requeue
// writes a new outbox row, nothing will ever deliver the job again.
func TestReclaimRepublishesJob(t *testing.T) {
	st, pool := dbtest.NewPostgresAndPool(t)
	ctx := context.Background()
	expID := dbtest.SeedTestExperiment(t, pool)
	jobID := dbtest.SeedQueuedJob(t, pool, expID, 0)

	if _, err := st.Claim(ctx, jobID, "worker-a", time.Second); err != nil {
		t.Fatalf("claim: %v", err)
	}
	dbtest.ExpireLease(t, pool, jobID)

	expired, err := st.ReclaimExpired(ctx, 10)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(expired) != 1 {
		t.Fatalf("reclaimed %d jobs, want 1", len(expired))
	}

	assertOutbox(t, pool, jobID, "fake")
}

// Same for a worker that gives a job back after a failed attempt.
func TestReleaseForRetryRepublishesJob(t *testing.T) {
	st, pool := dbtest.NewPostgresAndPool(t)
	ctx := context.Background()
	expID := dbtest.SeedTestExperiment(t, pool)
	jobID := dbtest.SeedQueuedJob(t, pool, expID, 0)

	attempt, err := st.Claim(ctx, jobID, "worker-a", time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := st.ReleaseForRetry(ctx, jobID, attempt, "boom"); err != nil {
		t.Fatalf("release: %v", err)
	}

	if state := dbtest.JobState(t, pool, jobID); state != "queued" {
		t.Fatalf("state = %q, want queued", state)
	}
	assertOutbox(t, pool, jobID, "fake")
}

// A stale worker releasing a job that was already reassigned is fenced off,
// and must not enqueue a message either: the requeue and the outbox row
// commit together or not at all.
func TestFencedReleaseDoesNotRepublish(t *testing.T) {
	st, pool := dbtest.NewPostgresAndPool(t)
	ctx := context.Background()
	expID := dbtest.SeedTestExperiment(t, pool)
	jobID := dbtest.SeedQueuedJob(t, pool, expID, 0)

	attemptA, err := st.Claim(ctx, jobID, "worker-a", time.Second)
	if err != nil {
		t.Fatalf("claim by A: %v", err)
	}
	dbtest.ExpireLease(t, pool, jobID)
	if _, err := st.ReclaimExpired(ctx, 10); err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if _, err := st.Claim(ctx, jobID, "worker-b", time.Second); err != nil {
		t.Fatalf("claim by B: %v", err)
	}

	err = st.ReleaseForRetry(ctx, jobID, attemptA, "stale")
	if !errors.Is(err, store.ErrFenced) {
		t.Fatalf("stale release: err = %v, want ErrFenced", err)
	}

	// Only the reclaim's row; the fenced release added nothing.
	assertOutbox(t, pool, jobID, "fake")
	if state := dbtest.JobState(t, pool, jobID); state != "running" {
		t.Errorf("state = %q, want running (B still holds it)", state)
	}
}

// assertOutbox checks that exactly one unpublished message exists and that it
// points at the expected job, routed by runner name.
func assertOutbox(t *testing.T, pool *pgxpool.Pool, jobID core.JobID, wantRunner string) {
	t.Helper()

	const q = `
		SELECT runner, msg_key, payload->>'job_id'
		  FROM outbox
		 WHERE published_at IS NULL`

	rows, err := pool.Query(context.Background(), q)
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	defer rows.Close()

	n := 0
	for rows.Next() {
		var runner, key, payloadJob string
		if err := rows.Scan(&runner, &key, &payloadJob); err != nil {
			t.Fatalf("scan outbox: %v", err)
		}
		n++
		if runner != wantRunner {
			t.Errorf("outbox runner = %q, want %q", runner, wantRunner)
		}
		if key != jobID.String() || payloadJob != jobID.String() {
			t.Errorf("outbox key/payload = %s/%s, want job %s", key, payloadJob, jobID)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if n != 1 {
		t.Fatalf("unpublished outbox rows = %d, want 1", n)
	}
}
