package core

// JobMessage is what travels through Kafka. Keep it small: the payload is a
// pointer to work, not the work itself — params and seed stay in Postgres.
type JobMessage struct {
	JobID        JobID        `json:"job_id"`
	ExperimentID ExperimentID `json:"experiment_id"`
	Runner       string       `json:"runner"`
	Idx          int          `json:"idx"`
}
