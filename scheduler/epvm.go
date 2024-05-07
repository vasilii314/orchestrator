package scheduler

import (
	"github.com/vasilii314/orchestrator/node"
	"github.com/vasilii314/orchestrator/task"
	"math"
	"time"
)

type Epvm struct {
	Name string
}

// SelectCandidateNodes selects nodes that have
// enough of disk space to run task t.
func (e *Epvm) SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node {
	var candidates []*node.Node
	for i := range nodes {
		if checkDiskSpace(t, nodes[i].Disk-nodes[i].DiskAllocated) {
			candidates = append(candidates, nodes[i])
		}
	}
	return candidates
}

func checkDiskSpace(t task.Task, diskSpaceAvailable int64) bool {
	return t.Disk <= diskSpaceAvailable
}

// Score E-PVM implementation is based on https://mosix.cs.huji.ac.il/pub/ocja.pdf
func (e *Epvm) Score(t task.Task, nodes []*node.Node) map[string]float64 {
	nodeScores := make(map[string]float64)
	maxJobs := 4.0
	n := float64(len(nodes))
	for _, node := range nodes {
		cpuUsage, err := calculateCpuUsage(node)
		if err != nil {
			return nodeScores
		}
		cpuLoad := calculateLoad(cpuUsage, math.Pow(2, 0.8))
		memoryAllocated := float64(node.Stats.MemUsedKb()) + float64(node.MemoryAllocated)
		memoryPercentAllocated := memoryAllocated / float64(node.Memory)
		newMemPercent := calculateLoad(memoryAllocated+float64(t.Memory/1000), float64(node.Memory))
		memCost := math.Pow(n, newMemPercent) + math.Pow(n, float64(node.TaskCount+1)/maxJobs) -
			math.Pow(n, memoryPercentAllocated) - math.Pow(n, float64(node.TaskCount)/float64(maxJobs))
		cpuCost := math.Pow(n, cpuLoad) + math.Pow(n, float64(node.TaskCount+1)/maxJobs) -
			math.Pow(n, cpuLoad) - math.Pow(n, float64(node.TaskCount)/float64(maxJobs))
		nodeScores[node.Name] = memCost + cpuCost
	}
	return nodeScores
}

func calculateCpuUsage(node *node.Node) (float64, error) {
	stat1, err := node.GetStats()
	if err != nil {
		return 0, err
	}
	time.Sleep(3 * time.Second)
	stat2, err := node.GetStats()
	if err != nil {
		return 0, err
	}
	stat1Idle := stat1.CpuStats.Idle + stat1.CpuStats.IOWait
	stat2Idle := stat2.CpuStats.Idle + stat2.CpuStats.IOWait
	stat1NonIdle := stat1.CpuStats.User + stat1.CpuStats.Nice +
		stat1.CpuStats.System + stat1.CpuStats.IRQ +
		stat1.CpuStats.SoftIRQ + stat1.CpuStats.Steal
	stat2NonIdle := stat2.CpuStats.User + stat2.CpuStats.Nice +
		stat2.CpuStats.System + stat2.CpuStats.IRQ +
		stat2.CpuStats.SoftIRQ + stat2.CpuStats.Steal
	stat1Total := stat1Idle + stat1NonIdle
	stat2Total := stat2Idle + stat2NonIdle
	total := stat2Total - stat1Total
	idle := stat2Idle - stat1Idle
	var cpuPercentUsage float64
	if total == 0 && idle == 0 {
		cpuPercentUsage = 0.0
	} else {
		cpuPercentUsage = (float64(total) - float64(idle)) / float64(total)
	}
	return cpuPercentUsage, nil
}

func calculateLoad(usage, capacity float64) float64 {
	return usage / capacity
}

func (e *Epvm) Pick(scores map[string]float64, candidates []*node.Node) *node.Node {
	minCost := 0.0
	var bestNode *node.Node
	for i, n := range candidates {
		if i == 0 {
			minCost = scores[n.Name]
			bestNode = n
			continue
		}
		if scores[n.Name] < minCost {
			minCost = scores[n.Name]
			bestNode = n
		}
	}
	return bestNode
}
