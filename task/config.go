package task

import "github.com/docker/go-connections/nat"

// Config struct defines a configuration of a task
type Config struct {
	// Task name
	Name         string
	AttachStdin  bool
	AttachStdout bool
	AttachStderr bool
	ExposedPorts nat.PortSet
	Cmd          []string
	Image        string
	Cpu          float64
	// Cpu and Memory used by scheduler to find a node in
	// the cluster capable of running a task. They will also
	// be used to tell the Docker daemon the number of resources
	// a task requires
	Memory int64
	Disk   int64
	// Env is used to specify ENV variables
	Env []string
	// Restart policy specifies what Docker daemon has to do
	// if a container dies unexpectedly. Possible values are
	// always, unless-stopped, on-failure, or no.
	RestartPolicy string
}
