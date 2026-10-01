package core

import (
	"encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParamValueRoundTrip(t *testing.T) {
	for _, want := range []ParamValue{
		NumInt(10), NumFloat(0.1), Str("whisper"), Bool(true),
	} {
		b, err := json.Marshal(want)
		require.NoError(t, err)

		var got ParamValue
		require.NoError(t, json.Unmarshal(b, &got))
		require.Equal(t, want, got)
	}
}
