package store_test

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/eugene-pi/simfleet/internal/dbtest"
)

// A state with no jobs is reported as 0, not left out: GROUP BY alone would
// drop it, and the series would vanish the moment a queue drains.
func TestActiveJobsByStateReportsEmptyStates(t *testing.T) {
	st, pool := dbtest.NewPostgresAndPool(t)
	ctx := context.Background()

	assertStates := func(want map[string]int) {
		t.Helper()
		got, err := st.ActiveJobsByState(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !maps.Equal(got, want) {
			t.Errorf("jobs by state = %v, want %v", got, want)
		}
	}

	assertStates(map[string]int{"queued": 0, "running": 0})

	expID := dbtest.SeedTestExperiment(t, pool)
	claimed := dbtest.SeedQueuedJob(t, pool, expID, 0)
	dbtest.SeedQueuedJob(t, pool, expID, 1)
	dbtest.SeedQueuedJob(t, pool, expID, 2)
	assertStates(map[string]int{"queued": 3, "running": 0})

	if _, err := st.Claim(ctx, claimed, "w", time.Minute); err != nil {
		t.Fatal(err)
	}
	assertStates(map[string]int{"queued": 2, "running": 1})

	if n, err := st.ActiveExperiments(ctx); err != nil || n != 1 {
		t.Errorf("active experiments = %d, %v, want 1", n, err)
	}
	if n, err := st.OutboxPending(ctx); err != nil || n != 0 {
		t.Errorf("outbox pending = %d, %v, want 0", n, err)
	}

	// Jobs of a finished experiment are no longer anyone's backlog.
	if _, err := pool.Exec(ctx, `UPDATE experiments SET state = 'completed'`); err != nil {
		t.Fatal(err)
	}
	assertStates(map[string]int{"queued": 0, "running": 0})
	if n, err := st.ActiveExperiments(ctx); err != nil || n != 0 {
		t.Errorf("active experiments = %d, %v, want 0", n, err)
	}
}
