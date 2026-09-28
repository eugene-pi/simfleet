package job

type State string

const (
	StateQueued    State = "queued"
	StateRunning   State = "running"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
	StateCancelled State = "cancelled"
)

// Event is something that occurs during job lifecycle.
// Event causes state transition: state + event => new state.
type Event string

const (
	EventClaim    Event = "claim"    // Executor has took the job
	EventComplete Event = "complete" // Job has finished
	EventRetry    Event = "retry"    // failure or timeout, we still have retries
	EventExhaust  Event = "exhaust"  // failure or timeout, and no more retries
	EventCancel   Event = "cancel"   // the whole exp cancelled
)

var transitions = map[State]map[Event]State{
	StateQueued: {
		EventClaim:  StateRunning,
		EventCancel: StateCancelled,
	},
	StateRunning: {
		EventComplete: StateCompleted,
		EventRetry:    StateQueued,
		EventExhaust:  StateFailed,
		EventCancel:   StateCancelled,
	},
	// completed, failed, cancelled — no transition
}

func (s State) IsTerminal() bool {
	_, ok := transitions[s]
	return !ok
}

func (s State) Valid() bool {
	switch s {
	case StateQueued, StateRunning, StateCompleted, StateFailed, StateCancelled:
		return true
	}
	return false
}
