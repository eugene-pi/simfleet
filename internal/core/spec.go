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
