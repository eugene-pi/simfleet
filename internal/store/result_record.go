package store

import "github.com/eugene-pi/simfleet/internal/core"

type ResultRecord struct {
	JobID       core.JobID
	Attempt     int
	ArtifactKey string
	core.Result
}
