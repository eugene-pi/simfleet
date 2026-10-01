// internal/expander/cartesian.go
package expander

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/eugene-pi/simfleet/internal/core"
)

const MaxCombinations = 1_000_000

var (
	ErrEmptySweep          = errors.New("sweep is empty")
	ErrEmptyParam          = errors.New("parameter has no values")
	ErrBadStep             = errors.New("range step must be positive")
	ErrBadRange            = errors.New("range end is before start")
	ErrBothValuesAndRange  = errors.New("parameter has both values and range")
	ErrTooManyCombinations = errors.New("too many combinations")
	ErrUnknownParam        = errors.New("unknown parameter")
	ErrMissingParam        = errors.New("missing parameter")
	ErrWrongParamKind      = errors.New("wrong parameter kind")
)

// Cartesian разворачивает перебор в упорядоченный список сочетаний.
// Порядок устойчив: одна и та же спецификация всегда даёт один и тот же
// порядок, поэтому индекс сочетания можно использовать как идентификатор.
func Cartesian(sweep core.Sweep) ([]map[string]core.ParamValue, error) {
	if len(sweep) == 0 {
		return nil, ErrEmptySweep
	}

	names := make([]string, 0, len(sweep))
	for name := range sweep {
		names = append(names, name)
	}
	sort.Strings(names)

	axes := make([][]core.ParamValue, len(names))
	total := 1
	for i, name := range names {
		vals, err := expandParam(sweep[name])
		if err != nil {
			return nil, fmt.Errorf("параметр %q: %w", name, err)
		}
		axes[i] = vals

		if total > MaxCombinations/len(vals) {
			return nil, ErrTooManyCombinations
		}
		total *= len(vals)
	}

	out := make([]map[string]core.ParamValue, total)
	for idx := range total {
		combo := make(map[string]core.ParamValue, len(names))
		rest := idx
		for i := len(names) - 1; i >= 0; i-- {
			n := len(axes[i])
			combo[names[i]] = axes[i][rest%n]
			rest /= n
		}
		out[idx] = combo
	}
	return out, nil
}

func expandParam(pv core.ParamValues) ([]core.ParamValue, error) {
	switch {
	case len(pv.Values) > 0 && pv.Range != nil:
		return nil, ErrBothValuesAndRange
	case len(pv.Values) > 0:
		return pv.Values, nil
	case pv.Range != nil:
		return expandRange(*pv.Range)
	default:
		return nil, ErrEmptyParam
	}
}

func expandRange(r core.Range) ([]core.ParamValue, error) {
	if r.Step <= 0 {
		return nil, ErrBadStep
	}
	if r.To < r.From {
		return nil, ErrBadRange
	}
	n := int(math.Round((r.To-r.From)/r.Step)) + 1
	if n > MaxCombinations {
		return nil, ErrTooManyCombinations
	}

	out := make([]core.ParamValue, n)
	for i := range n {
		out[i] = core.NumFloat(r.From + float64(i)*r.Step)
	}
	return out, nil
}
