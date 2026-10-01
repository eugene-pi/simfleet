package core

import "uuid" // стандартный пакет Go 1.27

type (
	ExperimentID uuid.UUID
	JobID        uuid.UUID
)

func (id ExperimentID) String() string { return uuid.UUID(id).String() }
func (id JobID) String() string        { return uuid.UUID(id).String() }

func (id ExperimentID) MarshalText() ([]byte, error) { return uuid.UUID(id).MarshalText() }
func (id JobID) MarshalText() ([]byte, error)        { return uuid.UUID(id).MarshalText() }

func (id *ExperimentID) UnmarshalText(b []byte) error {
	return (*uuid.UUID)(id).UnmarshalText(b)
}
func (id *JobID) UnmarshalText(b []byte) error {
	return (*uuid.UUID)(id).UnmarshalText(b)
}
