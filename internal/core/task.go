package core

import "time"

type Task struct {
	JobID        JobID
	ExperimentID ExperimentID
	Runner       string // "crossing" | "stt"
	Idx          int    // номер сочетания в переборе
	Seed         int64  // выводится из ExperimentID и Idx
	Params       map[string]any
	InputRef     string // ключ набора данных в S3; пусто для симуляции
	WorkDir      string // временный каталог, чистит платформа
}

type Profile struct {
	Timeout       time.Duration
	Deterministic bool // повторяемость прогоном или только записью
	MemoryMB      int
}
