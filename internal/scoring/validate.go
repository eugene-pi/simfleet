package scoring

import (
	"errors"
	"fmt"
	"sort"

	"github.com/eugene-pi/simfleet/internal/core"
)

func Validate(s core.Scoring, zones []core.Zone, available map[string]core.ParamKind) error {
	if len(s.Components) == 0 && len(s.Gates) == 0 {
		return ErrNoComponents
	}
	for _, c := range s.Components {
		if c.Good == c.Bad {
			return fmt.Errorf("%w: %q", ErrSameGoodBad, c.Metric)
		}
		if c.Weight < 0 {
			return fmt.Errorf("negative weight for %q", c.Metric)
		}
	}
	if _, err := combine(s.Combine, nil, map[string]float64{"x": 0}); err != nil &&
		!errors.Is(err, ErrNoComponents) {
		return err
	}
	return ValidateZones(zones)
}

// ValidateZones checks the zones and sorts them in place by descending Min.
// Note the in-place sort: the caller's slice is modified.
func ValidateZones(zones []core.Zone) error {
	if len(zones) == 0 {
		return ErrNoZones
	}
	sort.Slice(zones, func(i, j int) bool { return zones[i].Min > zones[j].Min })

	seen := map[string]bool{}
	for _, z := range zones {
		if z.Name == "" {
			return errors.New("zone name is empty")
		}
		if seen[z.Name] {
			return fmt.Errorf("duplicate zone %q", z.Name)
		}
		seen[z.Name] = true
		if z.Min < 0 || z.Min > 100 {
			return fmt.Errorf("zone %q: min out of range", z.Name)
		}
	}
	if zones[len(zones)-1].Min > 0 {
		return ErrZoneGap // нижняя зона обязана начинаться с нуля
	}
	return nil
}
