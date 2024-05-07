package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/vasilii314/orchestrator/utils"
	"github.com/vasilii314/orchestrator/worker"
	"io"
	"log"
	"net/http"
)

// Node is a representation of a physical machine
// that Worker, or Manager is running on
type Node struct {
	Name            string
	Ip              string
	Api             string
	Cores           int
	Memory          int64
	MemoryAllocated int64
	Disk            int64
	DiskAllocated   int64
	Role            string
	TaskCount       int
	Stats           worker.Stats
}

func NewNode(name, api, role string) *Node {
	return &Node{
		Name: name,
		Api:  api,
		Role: role,
	}
}

func (n *Node) GetStats() (*worker.Stats, error) {
	var (
		resp *http.Response
		err  error
	)
	url := fmt.Sprintf("%s/stats", n.Api)
	resp, err = utils.HTTPWithRetry(http.Get, url)
	if err != nil {
		msg := fmt.Sprintf("Unable to connect to %v", n.Api)
		log.Printf("[node.Node] [GetStats] %s\n", msg)
		return nil, errors.New(msg)
	}
	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("Error retrieving stats form %v: %v", n.Api, err)
		log.Printf("[Epvm] [getNodeStats] %s\n", msg)
		return nil, errors.New(msg)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var stats worker.Stats
	json.Unmarshal(body, &stats)
	if err != nil {
		msg := fmt.Sprintf("Error decoding message while getting stats for node %s", n.Name)
		log.Printf("[Epvm] [getNodeStats] %s\n", msg)
		return nil, errors.New(msg)
	}
	n.Memory = int64(stats.MemTotalKb())
	n.Disk = int64(stats.DiskTotal())
	n.Stats = stats
	return &stats, nil
}
