package task

var stateTransitionMap = map[State][]State{
	Pending:   []State{Scheduled},
	Scheduled: []State{Scheduled, Running, Failed},
	Running:   []State{Running, Completed},
	Completed: []State{},
	Failed:    []State{},
}

func Contains(states []State, state State) bool {
	for _, s := range states {
		if s == state {
			return true
		}
	}
	return false
}

func IsValidStateTransition(src, dst State) bool {
	return Contains(stateTransitionMap[src], dst)
}
