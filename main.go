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
	w1 := worker.New("worker-1", store.InMemoryStore)
	wapi1 := worker.Api{Address: workerHost, Port: workerPort, Worker: w1}
	w2 := worker.New("worker-2", store.InMemoryStore)
	wapi2 := worker.Api{Address: workerHost, Port: workerPort + 1, Worker: w2}
	w3 := worker.New("worker-1=3", store.InMemoryStore)
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
	m := manager.New(workers, scheduler.EpvmType, store.InMemoryStore)
	mapi := manager.Api{Address: managerHost, Port: managerPort, Manager: m}
	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.DoHealthChecks()
	mapi.Start()
	//for i := 0; i < 3; i++ {
	//	t := task.Task{
	//		ID:    uuid.New(),
	//		Name:  fmt.Sprintf("test-container-%d", i),
	//		State: task.Scheduled,
	//		Image: "strm/helloworld-http",
	//	}
	//	te := task.TaskEvent{
	//		ID:    uuid.New(),
	//		State: task.Running,
	//		Task:  t,
	//	}
	//	m.AddTask(te)
	//}
	//go m.ProcessTasks()
	//go m.UpdateTasks()
	//go func() {
	//	for {
	//		for _, t := range m.TaskDb {
	//			fmt.Printf("[Manager] Task: id: %s, state: %d\n", t.ID, t.State)
	//			time.Sleep(15 * time.Second)
	//		}
	//	}
	//}()
	//go w.RunTasks()
	//go w.CollectStats()
	//api.Start()

	//t := task.Task{
	//	ID:    uuid.New(),
	//	Name:  "test-container-1",
	//	State: task.Scheduled,
	//	Image: "strm/helloworld-http",
	//}
	//fmt.Println("starting task")
	//w.AddTask(t)
	//result := w.RunTask()
	//if result.Error != nil {
	//	panic(result.Error)
	//}
	//t.ContainerID = result.ContainerId
	//fmt.Printf("task %s is running in container %s\n", t.ID, t.ContainerID)
	//fmt.Println("Sleeping...")
	//time.Sleep(10 * time.Second)
	//fmt.Printf("stopping task %s\n", t.ID)
	//t.State = task.Completed
	//w.AddTask(t)
	//result = w.RunTask()
	//if result.Error != nil {
	//	panic(result.Error)
	//}

	//t := task.Task{
	//	ID:     uuid.New(),
	//	Name:   "Task-1",
	//	State:  task.Pending,
	//	Image:  "Image-1",
	//	Memory: 1024,
	//	Disk:   1,
	//}
	//tEvent := task.TaskEvent{
	//	ID:        uuid.New(),
	//	State:     task.Pending,
	//	Timestamp: time.Now(),
	//	Task:      t,
	//}
	//fmt.Printf("task: %v\n", t)
	//fmt.Printf("task event: %v\n", tEvent)
	//
	//w := worker.Worker{
	//	Name:  "worker-1",
	//	Queue: *queue.New(),
	//	Db:    make(map[uuid.UUID]*task.Task),
	//}
	//fmt.Printf("worker: %v\n", w)
	//w.CollectStats()
	//w.RunTask()
	//w.StartTask()
	//w.StopTask(t)
	//
	//m := manager.Manager{
	//	Pending:     *queue.New(),
	//	TaskDb:      make(map[string]task.Task),
	//	TaskEventDb: make(map[string]task.TaskEvent),
	//	Workers:     []string{w.Name},
	//}
	//fmt.Printf("manager: %v\n", m)
	//m.SelectWorker()
	//m.UpdateTask()
	//m.SendWork()
	//
	//n := node.Node{
	//	Name:   "Node-1",
	//	Ip:     "192.168.1.1",
	//	Cores:  4,
	//	Memory: 1024,
	//	Disk:   25,
	//	Role:   "worker",
	//}
	//fmt.Printf("node: %v\n", n)
	//
	//fmt.Println("--------------------")
	//fmt.Println("create a test container")
	//dockerTask, createResult := createContainer()
	//if createResult.Error != nil {
	//	fmt.Printf("%v\n", createResult.Error)
	//	os.Exit(1)
	//}
	//
	//time.Sleep(time.Second * 5)
	//fmt.Printf("stopping container %s\n", createResult.ContainerId)
	//_ = stopContainer(dockerTask, createResult.ContainerId)
}
