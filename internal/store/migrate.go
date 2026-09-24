package store

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/eugene-pi/simfleet/migrations"
)

func Migrate(ctx context.Context, dsn string, up1 bool, down1 bool) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer db.Close()

	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("dialect: %w", err)
	}
	if up1 {
		return goose.UpByOneContext(ctx, db, ".")
	} else if down1 {
		return goose.DownContext(ctx, db, ".")
	}
	return goose.UpContext(ctx, db, ".")
}
