// internal/config/config.go
package config

import (
	"errors"
	"os"
	"strconv"
	"time"
)

type ControllerConfig struct {
	DatabaseURL   string
	Interval      time.Duration // период цикла согласования
	ReclaimBatch  int           // сколько аренд возвращать за проход
	LeaseDuration time.Duration // должен совпадать с тем, что ставит Claim
}

func LoadControllerConfig() (ControllerConfig, error) {
	c := ControllerConfig{
		Interval:      time.Duration(lease_duration_sec_default/renew_lease_coef) * time.Second,
		ReclaimBatch:  batch_size_default,
		LeaseDuration: time.Duration(lease_duration_sec_default) * time.Second,
	}
	c.DatabaseURL = os.Getenv(ENV_DATABASE_URL)
	if c.DatabaseURL == "" {
		return c, errors.New("DATABASE_URL is not set")
	}
	if ld, err := strconv.Atoi(os.Getenv(ENV_SIMFLEET_LEASE_DURATION)); err == nil && ld > 0 {
		c.LeaseDuration = time.Duration(ld) * time.Second
		c.Interval = time.Duration(ld/renew_lease_coef) * time.Second
	}
	if batch, err := strconv.Atoi(os.Getenv(ENV_SIMFLEET_BATCH_SIZE)); err == nil && batch > 0 {
		c.ReclaimBatch = batch
	}
	return c, nil
}
