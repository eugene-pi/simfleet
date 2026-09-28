package job

import (
	"errors"
	"testing"
)

func TestNoTransitionsFromTerminalStates(t *testing.T) {
	terminal := []State{StateCompleted, StateFailed, StateCancelled}
	events := []Event{EventClaim, EventComplete, EventRetry, EventExhaust, EventCancel}

	for _, s := range terminal {
		for _, ev := range events {
			if _, err := Transit(s, ev); !errors.Is(err, ErrInvalidTransition) {
				t.Errorf("%s + %s: ожидалась ошибка", s, ev)
			}
		}
	}
}

func checkMapCompleted(mp map[State]bool) bool {
	for _, found := range mp {
		if found == false {
			return false
		}
	}
	return true
}

func doBFS(st State, events []Event) []State {
	rv := make([]State, 0)
	for _, ev := range events {
		if newState, err := Transit(st, ev); err == nil && newState != st {
			rv = append(rv, newState)
		}
	}
	return rv
}

func runBFSLevel(st []State, events []Event) []State {
	rv := make([]State, 0)
	for _, s := range st {
		newStates := doBFS(s, events)
		rv = append(rv, newStates...)
	}
	return rv
}

func TestEveryStateIsReachableFromQueued(t *testing.T) {
	mp := make(map[State]bool)
	events := []Event{EventClaim, EventComplete, EventRetry, EventExhaust, EventCancel}
	states := []State{StateQueued, StateRunning, StateCompleted, StateFailed, StateCancelled}
	for _, st := range states {
		mp[st] = false
	}

	cur := []State{StateQueued}
	for !checkMapCompleted(mp) {
		cur = runBFSLevel(cur, events)
		if len(cur) == 0 {
			break
		}
		for _, st := range cur {
			mp[st] = true
		}
	}

	if !checkMapCompleted(mp) {
		t.Errorf("unreachable state detected")
	}
}

func TestTransitionsProduceValidStates(t *testing.T) {
	for from, byEvent := range transitions {
		if !from.Valid() {
			t.Errorf("недопустимое исходное состояние %q", from)
		}
		for ev, to := range byEvent {
			if !to.Valid() {
				t.Errorf("%s + %s ведёт в недопустимое %q", from, ev, to)
			}
		}
	}
}
