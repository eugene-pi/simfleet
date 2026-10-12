package store_test

import (
	"context"
	"errors"
	"testing"
	"time"
	"uuid"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/dbtest"
	"github.com/eugene-pi/simfleet/internal/job"
	"github.com/eugene-pi/simfleet/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The fencing token is the attempt number. A worker whose lease expired and
// whose job was reassigned must not be able to write its stale result.
func TestFencingRejectsStaleResult(t *testing.T) {
	st, pool := dbtest.NewPostgresAndPool(t)
	expID := dbtest.SeedTestExperiment(t, pool)
	ctx := context.Background()

	jobID := dbtest.SeedQueuedJob(t, pool, expID, 1)

	// Worker A claims the job.
	// Исполнитель A берёт задание.
	attemptA, err := st.Claim(ctx, jobID, "worker-a", time.Second)
	if err != nil {
		t.Fatalf("claim by A: %v", err)
	}

	// A stalls: its lease expires and the controller requeues the job.
	// A зависает: аренда истекает, контроллер возвращает задание в очередь.
	dbtest.ExpireLease(t, pool, jobID)

	expired, err := st.ReclaimExpired(ctx, 10)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(expired) != 1 {
		t.Fatalf("reclaimed %d jobs, want 1", len(expired))
	}

	// Worker B picks it up; the attempt number advances.
	// Исполнитель B берёт его снова; номер попытки увеличивается.
	attemptB, err := st.Claim(ctx, jobID, "worker-b", time.Second)
	if err != nil {
		t.Fatalf("claim by B: %v", err)
	}
	if attemptB != attemptA+1 {
		t.Fatalf("attempt B = %d, want %d", attemptB, attemptA+1)
	}

	resRecord := core.Result{
		Metrics:    map[string]float64{"from": 1},
		Flags:      map[string]bool{"collision": true},
		Labels:     map[string]string{"test": "true"},
		DurationMS: 1}
	// A wakes up and tries to save its result under the old attempt.
	// A просыпается и пытается сохранить результат со старым номером попытки.
	err = st.SaveResult(ctx, store.ResultRecord{
		JobID:   jobID,
		Attempt: attemptA,
		Result:  resRecord,
	})
	if !errors.Is(err, store.ErrFenced) {
		t.Fatalf("stale write: err = %v, want ErrFenced", err)
	}

	// B saves successfully.
	resRecord.Metrics["from"] = 2
	if err := st.SaveResult(ctx, store.ResultRecord{
		JobID:   jobID,
		Attempt: attemptB,
		Result:  resRecord,
	}); err != nil {
		t.Fatalf("save by B: %v", err)
	}

	/* uncomment to view job_results
	rows, _ := pool.Query(ctx, `SELECT job_id, attempt, metrics FROM job_results`)
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		var attempt int
		var m []byte
		_ = rows.Scan(&id, &attempt, &m)
		t.Logf("result: job=%s attempt=%d metrics=%s", id, attempt, m)
		tmp := string(m)
		slog.Warn(tmp)
	}*/

	// Exactly one result, and it is B's.
	// Ровно один результат, и он принадлежит B.
	assertSingleResult(t, pool, jobID, 2)
}

// assertSingleResult checks that exactly one result row exists for the job and
// that it came from the expected worker. The "from" marker matters: without it
// the test would pass even if the wrong worker's write had survived.

func assertSingleResult(t *testing.T, pool *pgxpool.Pool,
	jobID core.JobID, wantFrom float64) {
	t.Helper()

	const q = `
		SELECT count(*), max((metrics->>'from')::float)
		  FROM job_results
		 WHERE job_id = $1`

	var n int
	var from *float64 // NULL when there are no rows / NULL, если строк нет

	if err := pool.QueryRow(context.Background(), q, uuid.UUID(jobID)).
		Scan(&n, &from); err != nil {
		t.Fatalf("count results for %s: %v", jobID, err)
	}

	if n != 1 {
		t.Fatalf("results for %s = %d, want exactly 1", jobID, n)
	}
	if from == nil {
		t.Fatalf("result for %s has no 'from' marker", jobID)
	}
	if *from != wantFrom {
		t.Errorf("result for %s came from worker %v, want %v", jobID, *from, wantFrom)
	}
}

// A redelivered message must not create a second result row. The first write
// wins; the second finds the job already completed and is fenced off.
// Повторная доставка сообщения не должна создавать вторую строку результата.
// Первая запись побеждает; вторая застаёт задание уже завершённым и
// отсекается ограждением.
func TestDuplicateResultIsIdempotent(t *testing.T) {
	st, pool := dbtest.NewPostgresAndPool(t)
	ctx := context.Background()
	expID := dbtest.SeedTestExperiment(t, pool)
	jobID := dbtest.SeedQueuedJob(t, pool, expID, 4)

	attempt, err := st.Claim(ctx, jobID, "worker-a", time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	rec := store.ResultRecord{
		JobID:   jobID,
		Attempt: attempt,
		Result: core.Result{
			Metrics:    map[string]float64{"from": 1},
			Flags:      map[string]bool{},
			Labels:     map[string]string{},
			DurationMS: 1,
		},
	}

	if err := st.SaveResult(ctx, rec); err != nil {
		t.Fatalf("first save: %v", err)
	}

	// Same attempt, delivered twice: the job is no longer running, so the
	// guarded UPDATE matches nothing.
	err = st.SaveResult(ctx, rec)
	if !errors.Is(err, store.ErrFenced) {
		t.Fatalf("second save: err = %v, want ErrFenced", err)
	}

	assertSingleResult(t, pool, jobID, 1)
}

// A job that used up its attempts must not go back to the queue: the reaper
// skips it and FailExhausted moves it to failed. Without this split such a
// job would sit in running forever.
func TestReclaimSkipsExhausted(t *testing.T) {
	st, pool := dbtest.NewPostgresAndPool(t)
	ctx := context.Background()
	expID := dbtest.SeedTestExperiment(t, pool)
	jobID := dbtest.SeedQueuedJob(t, pool, expID, 5)

	// Burn through the attempts: claim, expire, requeue — MaxAttempts times.
	// Расходуем попытки: взять, просрочить, вернуть — MaxAttempts раз.
	for i := range job.MaxAttempts {
		if _, err := st.Claim(ctx, jobID, "worker-a", time.Second); err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		dbtest.ExpireLease(t, pool, jobID)

		reclaimed, err := st.ReclaimExpired(ctx, 10)
		if err != nil {
			t.Fatalf("reclaim %d: %v", i, err)
		}
		// The last attempt must NOT be reclaimed.
		// Последняя попытка возвращаться уже не должна.
		wantReclaimed := i < job.MaxAttempts-1
		if got := len(reclaimed) == 1; got != wantReclaimed {
			t.Fatalf("attempt %d: reclaimed = %v, want %v", i+1, got, wantReclaimed)
		}
	}

	// Still running, attempts exhausted.
	// По-прежнему running, попытки исчерпаны.
	if state := dbtest.JobState(t, pool, jobID); state != "running" {
		t.Fatalf("state = %q, want running", state)
	}

	n, err := st.FailExhausted(ctx, 10)
	if err != nil {
		t.Fatalf("fail exhausted: %v", err)
	}
	if n != 1 {
		t.Fatalf("failed %d jobs, want 1", n)
	}
	if state := dbtest.JobState(t, pool, jobID); state != "failed" {
		t.Errorf("state = %q, want failed", state)
	}
}
