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

func LoadWorkerConfig() WorkerConfig {
	ld := os.Getenv(ENV_SIMFLEET_LEASE_DURATION)
	btch := os.Getenv(ENV_SIMFLEET_BATCH_SIZE)
	lds := lease_duration_sec_default
	batch := batch_size_default
	if ld != "" {
		if ld2, err := strconv.Atoi(ld); err == nil && ld2 > 0 {
			lds = ld2
		}
	}
	if btch != "" {
		if b2, err := strconv.Atoi(btch); err == nil && b2 > 0 {
			batch = b2
		}
	}
	return WorkerConfig{
		LeaseDuration: time.Second * time.Duration(lds),
		RenewEvery:    time.Second * time.Duration(lds/renew_lease_coef),
		BatchSize:     batch,
	}
}
