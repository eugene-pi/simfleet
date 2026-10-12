package core

// Evaluation is the outcome of scoring one result. Derived from metrics,
// so it can be recomputed without re-running the job.
type Evaluation struct {
	Score      float64            // 0..100
	Zone       string             // zone name
	Components map[string]float64 // per-metric scores
	GatedBy    string             // gate that fired (if any)
}

// PendingEvaluation is one result awaiting (re)scoring, together with the
// experiment's scoring spec.
type PendingEvaluation struct {
	JobID       JobID
	Metrics     map[string]float64
	Flags       map[string]bool
	Scoring     Scoring
	Zones       []Zone
	ScoringHash string
}

type Scoring struct {
	Gates      []Gate           `json:"gates,omitempty"`
	Components []ScoreComponent `json:"components"`
	Combine    string           `json:"combine"`
}

type Gate struct {
	When  string  `json:"when"` // имя признака
	Score float64 `json:"score"`
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
