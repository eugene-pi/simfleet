package job

const MaxAttempts = 5

type FailureKind int

const (
	FailureInfra FailureKind = iota
	FailurePermanent
)

func OnFailure(attempt int, kind FailureKind) Event {
	if kind == FailurePermanent || attempt >= MaxAttempts {
		return EventExhaust
	}
	return EventRetry
}
