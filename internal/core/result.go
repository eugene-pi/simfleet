package core

type Result struct {
	Metrics    map[string]float64 // истина, из неё считается оценка
	Flags      map[string]bool    // признаки для жёстких отсечек
	Labels     map[string]string  // engine=whisper, model=base.en
	Artifact   []byte             // трасса; пусто, если нечего сохранять
	DurationMS int
}

type Evaluation struct {
	Score       float64 // 0..100
	Zone        string
	ScoringHash string
	Components  map[string]float64
}
