package core

type Spec struct {
	Name    string  `json:"name"`
	Runner  string  `json:"runner"`
	Sweep   Sweep   `json:"sweep"`
	Scoring Scoring `json:"scoring"`
	Zones   []Zone  `json:"zones"`
}

type Sweep map[string]ParamValues

type ParamValues struct {
	Values []ParamValue `json:"values,omitempty"`
	Range  *Range       `json:"range,omitempty"`
}

type Range struct {
	From float64 `json:"from"`
	To   float64 `json:"to"`
	Step float64 `json:"step"`
}

// Scoring и Zone пока объявлены, но не используются: оценивание
// добавится следующим слоем, после работающей вертикали.
type Scoring struct {
	Components []ScoreComponent `json:"components"`
	Combine    string           `json:"combine"` // "min" | "weighted_mean"
}

type ScoreComponent struct {
	Metric string  `json:"metric"`
	Good   float64 `json:"good"`
	Bad    float64 `json:"bad"`
	Weight float64 `json:"weight,omitempty"`
}

type Zone struct {
	Name string  `json:"name"`
	Min  float64 `json:"min"`
}
