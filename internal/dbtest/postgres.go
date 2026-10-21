package dbtest

import (
	"context"
	"testing"
	"time"
	"uuid"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/job"
	"github.com/eugene-pi/simfleet/internal/migrate"
	"github.com/eugene-pi/simfleet/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// NewPostgres starts a throwaway Postgres, applies migrations and returns a
// ready store. The container is removed when the test finishes.
func NewPostgresAndPool(t *testing.T) (*store.Postgres, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	c, err := postgres.Run(ctx, "postgres:17",
		postgres.WithDatabase("simfleet"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("dsn: %v", err)
	}
	if err := migrate.Migrate(ctx, dsn, false, false); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	st, err := store.NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(st.Close)
	pool, err := store.NewPostgresPool(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return st, pool
}

func NewPostgres(t *testing.T) *store.Postgres {
	t.Helper()
	ctx := context.Background()

	c, err := postgres.Run(ctx, "postgres:17",
		postgres.WithDatabase("simfleet"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("dsn: %v", err)
	}
	if err := migrate.Migrate(ctx, dsn, false, false); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	st, err := store.NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}

func CountByState(ctx context.Context, t *testing.T, pool *pgxpool.Pool, expID core.ExperimentID, state job.State) int {
	t.Helper()
	const q = `
		SELECT count(1) FROM jobs
		 WHERE experiment_id = $1
		 AND state = $2`
	var cnt int
	err := pool.QueryRow(ctx, q, uuid.UUID(expID), state).Scan(&cnt)
	if err != nil {
		t.Fatalf("count by state: %v", err)
	}
	return cnt
}

func CountUnpublished(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	if err != nil {
		t.Fatalf("count unpublished: %v", err)
	}
	return n
}
