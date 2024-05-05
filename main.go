package main

import (
	"fmt"
	"github.com/docker/docker/client"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/vasilii314/orchestrator/manager"
	"github.com/vasilii314/orchestrator/task"
	"github.com/vasilii314/orchestrator/worker"
	"log"
	"os"
	"strconv"
	"time"
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

func runTasks(w *worker.Worker) {
	for {
		if w.Queue.Len() != 0 {
			result := w.RunTask()
			if result.Error != nil {
				log.Printf("Error running task: %v\n", result.Error)
			}
		} else {
			log.Println("No tasks to process currently.")
		}
		log.Println("Sleeping for 10 seconds.")
		time.Sleep(10 * time.Second)
	}
}

func main() {
	host := os.Getenv("CUBE_HOST")
	if len(host) == 0 {
		host = "localhost"
	}
	port, err := strconv.Atoi(os.Getenv("CUBE_PORT"))
	if err != nil {
		port = 5555
	}
	fmt.Println("Starting cube worker")
	w := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}
	api := worker.Api{Address: host, Port: port, Worker: &w}
	workers := []string{fmt.Sprintf("%s:%d", host, port)}
	m := manager.New(workers)
	for i := 0; i < 3; i++ {
		t := task.Task{
			ID:    uuid.New(),
			Name:  fmt.Sprintf("test-container-%d", i),
			State: task.Scheduled,
			Image: "strm/helloworld-http",
		}
		te := task.TaskEvent{
			ID:    uuid.New(),
			State: task.Running,
			Task:  t,
		}
		m.AddTask(te)
	}
	go func() {
		for {
			fmt.Println("Trying to send task to worker")
			m.SendWork()
			time.Sleep(15 * time.Second)
		}
	}()
	go func() {
		for {
			fmt.Printf("[Manager] Updating task from %d workers\n", len(m.Workers))
			m.UpdateTasks()
			time.Sleep(15 * time.Second)
		}
	}()
	go func() {
		for {
			for _, t := range m.TaskDb {
				fmt.Printf("[Manager] Task: id: %s, state: %d\n", t.ID, t.State)
				time.Sleep(15 * time.Second)
			}
		}
	}()
	go runTasks(&w)
	go w.CollectStats()
	api.Start()

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