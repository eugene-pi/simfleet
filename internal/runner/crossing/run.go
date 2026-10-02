package crossing

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/eugene-pi/simfleet/internal/core"
)

const (
	dt           = 0.02 // шаг интегрирования, с
	horizon      = 10.0 // максимальная длительность сценария, с
	sensorRangeM = 50.0
)

type params struct {
	speed           float64
	pedInitDistance float64
	reaction        float64
	maxDecel        float64
	noiseStd        float64
}

func (c *Crossing) Run(ctx context.Context, t core.Task) (core.Result, error) {
	collided := false
	start := time.Now()
	p, err := parseParams(t.Params)
	if err != nil {
		return core.Result{}, fmt.Errorf("params: %w", err)
	}

	rng := rand.New(rand.NewPCG(uint64(t.Seed), 0))

	bias := 0.0
	if p.noiseStd > 0 {
		bias = rng.NormFloat64() * p.noiseStd
	}

	effectiveRange := sensorRangeM + bias

	var (
		x          = -p.pedInitDistance
		v          = p.speed
		step       = 0
		maxSteps   = int(horizon / dt)
		minDist    = math.Inf(1)
		braking    = false
		detectedAt = -1.0
	)

	for ; step < maxSteps && x < 1.0 && v >= 0.01; step++ {
		tm := float64(step) * dt
		if err := ctx.Err(); err != nil {
			return core.Result{}, err
		}

		if detectedAt < 0 && math.Abs(x) <= effectiveRange {
			detectedAt = tm
		} else if detectedAt >= 0 && tm >= detectedAt+p.reaction {
			braking = true
		}

		if braking {
			v = math.Max(0, v-p.maxDecel*dt)
			if v < 0.01 {
				v = 0
			}
		}
		x += v * dt

	}
	if x >= 0 {
		collided = true
		minDist = 0
	} else {
		minDist = math.Abs(x)
	}
	metrics := map[string]float64{
		"min_distance_m":  minDist,
		"final_speed_mps": v,
	}
	if p.noiseStd > 0 {
		metrics["sensor_bias_m"] = bias
	}

	if collided {
		metrics["collision_time_s"] = float64(step) * dt
	}

	flags := map[string]bool{
		"collision": collided,
		"timeout":   step >= maxSteps,
	}
	return core.Result{
		Metrics:    metrics,
		Flags:      flags,
		Labels:     map[string]string{},
		DurationMS: int(time.Since(start).Milliseconds()),
	}, nil
}

func parseParams(m map[string]core.ParamValue) (params, error) {
	get := func(name string) (float64, error) {
		v, ok := m[name]
		if !ok {
			return 0, fmt.Errorf("missing %q", name)
		}
		f, ok := v.AsFloat()
		if !ok {
			return 0, fmt.Errorf("%q is not a number", name)
		}
		return f, nil
	}

	var p params
	var err error
	if p.speed, err = get("vehicle_speed_mps"); err != nil {
		return p, err
	}
	if p.pedInitDistance, err = get("pedestrian_appears_at_m"); err != nil {
		return p, err
	}
	if p.reaction, err = get("reaction_time_s"); err != nil {
		return p, err
	}
	if p.maxDecel, err = get("max_decel_mps2"); err != nil {
		return p, err
	}
	if p.noiseStd, err = get("sensor_noise_std"); err != nil {
		return p, err
	}
	return p, nil
}
