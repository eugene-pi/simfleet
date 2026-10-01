package expander

import (
	"testing"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/stretchr/testify/require"
)

func TestCartesianIsStable(t *testing.T) {
	sweep := core.Sweep{
		"speed": {Values: []core.ParamValue{core.NumFloat(8), core.NumFloat(10), core.NumFloat(12)}},
		"delay": {Range: &core.Range{From: 0, To: 0.4, Step: 0.1}},
		"react": {Values: []core.ParamValue{core.NumFloat(0.3), core.NumFloat(0.5)}},
	}
	first, err := Cartesian(sweep)
	require.NoError(t, err)

	for i := 0; i < 20; i++ { // обход map рандомизирован — повторяем
		got, err := Cartesian(sweep)
		require.NoError(t, err)
		require.Equal(t, first, got)
	}
}
