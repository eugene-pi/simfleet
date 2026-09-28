package job

import (
	"errors"
	"fmt"
)

var ErrInvalidTransition = errors.New("invalid transition")

type TransitionError struct {
	From  State
	Event Event
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("cannot apply %q in state %q", e.Event, e.From)
}

func (e *TransitionError) Is(target error) bool { return target == ErrInvalidTransition }

// Transit returns new state or error if transition is not allowed
func Transit(from State, ev Event) (State, error) {
	byEvent, ok := transitions[from]
	if !ok {
		return "", &TransitionError{From: from, Event: ev}
	}
	to, ok := byEvent[ev]
	if !ok {
		return "", &TransitionError{From: from, Event: ev}
	}
	return to, nil
}
