package dbtest

import (
	"context"
	"testing"
	"time"

	"github.com/eugene-pi/simfleet/internal/store"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func NewPostgres(t *testing.T) string {
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
	if err := store.Migrate(ctx, dsn, false, false); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return dsn
}
