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
	dt           = 0.02  // шаг интегрирования, с
	horizon      = 10.0  // максимальная длительность сценария, с
	startX       = -60.0 // начальное положение машины, м (пешеход в нуле)
	sensorRangeM = 50.0
)

type params struct {
	speed    float64
	pedDelay float64
	reaction float64
	maxDecel float64
	noiseStd float64
}

func (c *Crossing) Run(ctx context.Context, t core.Task) (core.Result, error) {
	collided := false
	start := time.Now()

	p, err := parseParams(t.Params)
	if err != nil {
		return core.Result{}, fmt.Errorf("params: %w", err)
	}

	rng := rand.New(rand.NewPCG(uint64(t.Seed), 0))

	var (
		x          = startX
		v          = p.speed
		minDist    = math.Inf(1)
		minTTC     = math.Inf(1)
		maxDecel   = 0.0
		braking    = false
		detectedAt = -1.0
	)

	for tm := 0.0; tm < horizon; tm += dt {
		if err := ctx.Err(); err != nil {
			return core.Result{}, err
		}

		pedOnRoad := tm >= p.pedDelay

		if pedOnRoad && detectedAt < 0 && math.Abs(x) <= sensorRangeM {
			detectedAt = tm
		}
		if detectedAt >= 0 && tm >= detectedAt+p.reaction {
			braking = true
		}

		if braking {
			maxDecel = p.maxDecel
			v = math.Max(0, v-p.maxDecel*dt)
		}
		prevX := x
		x += v * dt

		if prevX < 0 && x >= 0 { // пересекли точку перехода
			if pedOnRoad && v > 0.01 {
				collided = true
				minDist = 0
			}
			break // сценарий закончен в любом случае
		}
		if pedOnRoad {
			dist := math.Min(math.Abs(prevX), math.Abs(x))
			if p.noiseStd > 0 {
				dist += rng.NormFloat64() * p.noiseStd
			}
			minDist = math.Min(minDist, dist)
			if x < 0 && v > 0.01 {
				minTTC = math.Min(minTTC, -x/v)
			}
		}

		if x > 5 { // перекрёсток пройден
			break
		}
	}

	metrics := map[string]float64{
		"min_distance_m":  minDist,
		"max_decel_mps2":  maxDecel,
		"final_speed_mps": v,
	}
	if !math.IsInf(minTTC, 1) {
		metrics["time_to_collision_s"] = minTTC
	}

	return core.Result{
		Metrics:    metrics,
		Flags:      map[string]bool{"collision": collided},
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
	if p.pedDelay, err = get("pedestrian_delay_s"); err != nil {
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
