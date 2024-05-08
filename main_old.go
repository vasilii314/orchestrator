package main

import (
	"fmt"
	"github.com/docker/docker/client"
	"github.com/vasilii314/orchestrator/manager"
	"github.com/vasilii314/orchestrator/scheduler"
	"github.com/vasilii314/orchestrator/store"
	"github.com/vasilii314/orchestrator/task"
	"github.com/vasilii314/orchestrator/worker"
	"os"
	"strconv"
)

func createContainer() (*task.Docker, *task.DockerResult) {
	c := task.Config{
		Name:  "test-container-1",
		Image: "postgres:latest",
		Env: []string{
			"POSTGRES_USER=myuser",
			"POSTGRES_PASSWORD=myuser",
		},
	}
	dockerClient, _ := client.NewClientWithOpts(client.FromEnv)
	d := task.Docker{
		Client: dockerClient,
		Config: c,
	}

	result := d.Run()
	if result.Error != nil {
		fmt.Printf("%v\n", result.Error)
		return nil, nil
	}

	fmt.Printf("Container %s is running with config %v\n", result.ContainerId, c)
	return &d, &result
}

func stopContainer(d *task.Docker, id string) *task.DockerResult {
	result := d.Stop(id)
	if result.Error != nil {
		fmt.Printf("%v\n", result.Error)
		return nil
	}

	fmt.Printf("Container %s has been stopped and removed\n", result.ContainerId)
	return &result
}

func main() {
	workerHost := os.Getenv("O_WORKER_HOST")
	if len(workerHost) == 0 {
		workerHost = "localhost"
	}
	workerPort, err := strconv.Atoi(os.Getenv("O_WORKER_PORT"))
	if err != nil {
		workerPort = 5555
	}
	managerHost := os.Getenv("O_MANAGER_HOST")
	if len(workerHost) == 0 {
		managerHost = "localhost"
	}
	managerPort, err := strconv.Atoi(os.Getenv("O_MANAGER_PORT"))
	if err != nil {
		managerPort = 5554
	}
	fmt.Println("[] [main] Starting orchestration worker")
	w1 := worker.New("worker-1", store.PersistentStore)
	wapi1 := worker.Api{Address: workerHost, Port: workerPort, Worker: w1}
	w2 := worker.New("worker-2", store.PersistentStore)
	wapi2 := worker.Api{Address: workerHost, Port: workerPort + 1, Worker: w2}
	w3 := worker.New("worker-3", store.PersistentStore)
	wapi3 := worker.Api{Address: workerHost, Port: workerPort + 2, Worker: w3}
	go w1.RunTasks()
	go w1.CollectStats()
	go w1.UpdateTasks()
	go wapi1.Start()
	go w2.RunTasks()
	go w2.CollectStats()
	go w2.UpdateTasks()
	go wapi2.Start()
	go w3.RunTasks()
	go w3.CollectStats()
	go w3.UpdateTasks()
	go wapi3.Start()
	fmt.Println("[] [main] Starting orchestration manager")
	workers := []string{
		fmt.Sprintf("%s:%d", workerHost, workerPort),
		fmt.Sprintf("%s:%d", workerHost, workerPort+1),
		fmt.Sprintf("%s:%d", workerHost, workerPort+2),
	}
	m := manager.New(workers, scheduler.EpvmType, store.PersistentStore)
	mapi := manager.Api{Address: managerHost, Port: managerPort, Manager: m}
	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.DoHealthChecks()
	mapi.Start()
}
