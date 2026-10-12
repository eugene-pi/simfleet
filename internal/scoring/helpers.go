package scoring

import (
	"errors"
	"fmt"
	"math"

	"github.com/eugene-pi/simfleet/internal/core"
)

var (
	ErrMissingMetric  = errors.New("metric is missing from result")
	ErrUnknownCombine = errors.New("unknown combine mode")
	ErrZeroWeights    = errors.New("weights sum to zero")
	ErrNoComponents   = errors.New("scoring has no components")
	ErrSameGoodBad    = errors.New("good and bad must differ")
	ErrNoZones        = errors.New("no zones defined")
	ErrZoneGap        = errors.New("zones must cover 0..100 without gaps")
	ErrNoZoneForScore = errors.New("no zone matches score")
)

// zoneFor finds the zone for a score. zones must be sorted by descending
// Min — ValidateZones guarantees that.
func zoneFor(zones []core.Zone, score float64) (string, error) {
	for _, z := range zones {
		if score >= z.Min {
			return z.Name, nil
		}
	}
	return "", fmt.Errorf("%w: %.1f", ErrNoZoneForScore, score)
}

// normalize maps a metric value onto a 0..100 scale. Direction comes from
// the good/bad ordering: when good < bad the scale inverts automatically.
func normalize(v, good, bad float64) float64 {
	if good == bad {
		return 0
	}
	t := (v - bad) / (good - bad) // 0 при bad, 1 при good
	return 100 * math.Max(0, math.Min(1, t))
}

func combine(mode string, specs []core.ScoreComponent, comps map[string]float64) (float64, error) {
	switch mode {
	case "", "min":
		m := 100.0
		for _, v := range comps {
			m = math.Min(m, v)
		}
		return m, nil
	case "weighted_mean":
		var sum, wsum float64
		for _, c := range specs {
			w := c.Weight
			if w == 0 {
				w = 1
			}
			sum += comps[c.Metric] * w
			wsum += w
		}
		if wsum == 0 {
			return 0, ErrZeroWeights
		}
		return sum / wsum, nil
	default:
		return 0, fmt.Errorf("%w: %q", ErrUnknownCombine, mode)
	}
}
