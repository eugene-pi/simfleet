package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/eugene-pi/simfleet/internal/config"
	"github.com/eugene-pi/simfleet/internal/envfile"
	store "github.com/eugene-pi/simfleet/internal/migrate"
)

func main() {
	var up, down, top bool
	flag.BoolVar(&up, "up", false, "Upgrade 1 step up")
	flag.BoolVar(&down, "down", false, "Upgrade 1 step down")
	flag.BoolVar(&top, "top", true, "Upgrade to latest available version")
	flag.Parse()
	dsn := os.Getenv(config.ENV_DATABASE_URL)
	if dsn == "" {
		envfile.LoadEnv()
		dsn = os.Getenv(config.ENV_DATABASE_URL)
		if dsn == "" {
			log.Fatal("DATABASE_URL is not set")
		}
	}
	if !up && !down && !top {
		flag.PrintDefaults()
		log.Fatal("migration flags not set")
	}
	if err := store.Migrate(context.Background(), dsn, up, down); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	log.Println("migrations applied")
}
