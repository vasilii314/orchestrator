package scheduler

import (
	"github.com/vasilii314/orchestrator/node"
	"github.com/vasilii314/orchestrator/task"
)

type SchedulerType string

const (
	RoundRobinType SchedulerType = "roundrobin"
	EpvmType       SchedulerType = "epvm"
)

type Scheduler interface {
	SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node
	Score(t task.Task, nodes []*node.Node) map[string]float64
	Pick(scores map[string]float64, candidates []*node.Node) *node.Node
}
