package task

import (
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
)

type State int

const (
	Pending State = iota
	Scheduled
	Running
	Completed
	Failed
)

type Task struct {
	// Unique identifier
	ID          uuid.UUID
	ContainerID string
	// Human-readable name
	Name  string
	State State
	// Orchestrator will only work with Docker containers
	// Image is the name of a Docker container image
	Image string
	// Allocated memory
	Memory int
	// Allocated disk space
	Disk int
	// These fields will be used by Docker
	// to ensure the machine allocates the proper
	// network ports for the task, and it is
	// available on the network
	Cpu           float64
	ExposedPorts  nat.PortSet
	PortBindings  map[string]string
	RestartPolicy string
	StartTime     time.Time
	FinishTime    time.Time
}

func NewConfig(t *Task) *Config {
	return &Config{
		Name:          t.Name,
		Image:         t.Image,
		RestartPolicy: t.RestartPolicy,
	}
}
