package config

import (
	"os"
	"strconv"
	"time"
)

type WorkerConfig struct {
	LeaseDuration time.Duration
	RenewEvery    time.Duration
	BatchSize     int
}

func LoadConfig() WorkerConfig {
	ld := os.Getenv("SIMFLEET_LEASE_DURATION")
	btch := os.Getenv("SIMFLEET_BATCH_SIZE")
	lds := time.Second * 30
	batch := 20
	if ld != "" {
		if ld2, err := strconv.Atoi(ld); err == nil && ld2 > 0 {
			lds = time.Second * time.Duration(ld2)
		}
	}
	if btch != "" {
		if b2, err := strconv.Atoi(btch); err == nil && b2 > 0 {
			batch = b2
		}
	}
	return WorkerConfig{
		LeaseDuration: lds,
		RenewEvery:    lds / 3,
		BatchSize:     batch,
	}
}
