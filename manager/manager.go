package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/vasilii314/orchestrator/task"
	"github.com/vasilii314/orchestrator/worker"
	"log"
	"net/http"
	"strings"
	"time"
)

type Manager struct {
	// Pending queue stores all task events
	// before they are submitted to workers.
	Pending queue.Queue
	// TaskDb (in-memory db) used to track all tasks in the system
	TaskDb map[uuid.UUID]*task.Task
	// TaskEventDb (in-memory db) used to track
	// all task events in the system
	TaskEventDb map[uuid.UUID]*task.TaskEvent
	// Workers slice stores all workers in the system.
	// Its values are strings of the following pattern:
	// <hostname>:<port>
	Workers []string
	// WorkerTaskMap is a convenience field
	// used to track the tasks assigned to a worker
	WorkerTaskMap map[string][]uuid.UUID
	// TaskWorkerMap is a convenience field
	// used to track which worker a task is
	// assigned to
	TaskWorkerMap map[uuid.UUID]string
	// LastWorker stores the index of last
	// selected worker from Workers slice.
	// This field is required for round-robin
	// scheduling.
	LastWorker int
}

// This method is used to schedule tasks
// recieved from a user onto workers
func (m *Manager) SelectWorker() string {
	return m.Workers[m.selectWorkerRoundRobin()]
}

// updateTask polls workers
// to update task statuses.
func (m *Manager) updateTasks() {
	for _, worker := range m.Workers {
		log.Printf("[manager.Manager] [updateTasks] Checking %v for task updates", worker)
		url := fmt.Sprintf("http://%s/tasks", worker)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("[manager.Manager] [updateTasks] Error connecting to %v: %v\n", worker, err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			log.Printf("[manager.Manager] [updateTasks] Error sending request: %v\n", err)
			continue
		}
		d := json.NewDecoder(resp.Body)
		var tasks []*task.Task
		err = d.Decode(&tasks)
		if err != nil {
			log.Printf("[manager.Manager] [updateTasks] Error unmarshalling tasks: %s\n", err.Error())
			continue
		}
		for _, t := range tasks {
			log.Printf("[manager.Manager] [updateTasks] Attempting to update task %v\n", t.ID)
			_, ok := m.TaskDb[t.ID]
			if !ok {
				log.Printf("[manager.Manager] [updateTasks] Task with ID %s not found\n", t.ID)
				continue
			}
			if m.TaskDb[t.ID].State != t.State {
				m.TaskDb[t.ID].State = t.State
			}
			m.TaskDb[t.ID].StartTime = t.StartTime
			m.TaskDb[t.ID].FinishTime = t.FinishTime
			m.TaskDb[t.ID].ContainerID = t.ContainerID
		}
	}
}

func (m *Manager) UpdateTasks() {
	for {
		log.Println("[manager.Manager] [UpdateTasks] Checking for task updates from workers")
		m.updateTasks()
		log.Println("[manager.Manager] [UpdateTasks] Task updates completed")
		log.Println("[manager.Manager] [UpdateTasks] Sleeping for 15 seconds")
		time.Sleep(15 * time.Second)
	}
}

func (m *Manager) ProcessTasks() {
	for {
		log.Println("[manager.Manager] [ProcessTasks] Processing any tasks in the queue")
		m.SendWork()
		log.Println("[manager.Manager] [ProcessTasks] Sleeping for 10 seconds")
		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) SendWork() {
	if m.Pending.Len() > 0 {
		w := m.SelectWorker()
		e := m.Pending.Dequeue()
		taskEvent := e.(task.TaskEvent)
		t := taskEvent.Task
		log.Printf("[manager.Manager] [SendWork] Pulled %v off pending queue\n", t)
		m.TaskEventDb[taskEvent.ID] = &taskEvent
		m.WorkerTaskMap[w] = append(m.WorkerTaskMap[w], taskEvent.Task.ID)
		m.TaskWorkerMap[t.ID] = w
		t.State = task.Scheduled
		m.TaskDb[t.ID] = &t
		data, err := json.Marshal(taskEvent)
		if err != nil {
			log.Printf("[manager.Manager] [SendWork] Unable to marshal task object^ %v\n", t)
			return
		}
		url := fmt.Sprintf("http://%s/tasks", w)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			log.Printf("[manager.Manager] [SendWork] Error connecting to %v: %v\n", w, err)
			m.Pending.Enqueue(taskEvent)
			return
		}
		d := json.NewDecoder(resp.Body)
		if resp.StatusCode != http.StatusCreated {
			e := worker.ErrResponse{}
			err := d.Decode(&e)
			if err != nil {
				log.Printf("[manager.Manager] [SendWork] Error decoding response: %s\n", err.Error())
				return
			}
			log.Printf("[manager.Manager] [SendWork] Response error (%d): %s", e.HTTPStatusCode, e.Message)
			return
		}
		t = task.Task{}
		err = d.Decode(&t)
		if err != nil {
			log.Printf("[manager.Manager] [SendWork] Error decoding response: %s\n", err.Error())
			return
		}
		log.Printf("[manager.Manager] [SendWork] %#v\n", t)
	} else {
		log.Println("[manager.Manager] [SendWork] No work in the queue")
	}
}

func (m *Manager) selectWorkerRoundRobin() int {
	var newWorker int
	if m.LastWorker+1 < len(m.Workers) {
		newWorker = m.LastWorker + 1
		m.LastWorker++
	} else {
		newWorker = 0
		m.LastWorker = 0
	}
	return newWorker
}

func (m *Manager) AddTask(te task.TaskEvent) {
	m.Pending.Enqueue(te)
}

func New(workers []string) *Manager {
	taskDb := make(map[uuid.UUID]*task.Task)
	eventDb := make(map[uuid.UUID]*task.TaskEvent)
	workerTaskMap := make(map[string][]uuid.UUID)
	taskWorkerMap := make(map[uuid.UUID]string)
	for i := range workers {
		workerTaskMap[workers[i]] = []uuid.UUID{}
	}
	return &Manager{
		Pending:       *queue.New(),
		Workers:       workers,
		TaskDb:        taskDb,
		TaskEventDb:   eventDb,
		WorkerTaskMap: workerTaskMap,
		TaskWorkerMap: taskWorkerMap,
	}
}

func (m *Manager) GetTasks() []*task.Task {
	res := make([]*task.Task, 0, len(m.TaskDb))
	for _, t := range m.TaskDb {
		res = append(res, t)
	}
	return res
}

func (m *Manager) checkTaskHealth(t task.Task) error {
	log.Printf("[manager.Manager] [checkTaskHealth] Calling health check for task %s: %s\n", t.ID, t.HealthCheck)
	w := m.TaskWorkerMap[t.ID]
	hostPort := getHostPort(t.HostPorts)
	worker := strings.Split(w, ":")
	url := fmt.Sprintf("http://%s:%s%s", worker[0], *hostPort, t.HealthCheck)
	log.Printf("[manager.Manager] [checkTaskHealth] Calling health check for task %s: %s\n", t.ID, url)
	resp, err := http.Get(url)
	if err != nil {
		msg := fmt.Sprintf("Error connecting to health check endpoint %s", url)
		log.Println(msg)
	}
	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("Error health check for task %s did not return 200\n", t.ID)
		log.Println(msg)
		return errors.New(msg)
	}
	log.Printf("[manager.Manager] [checkTaskHealth] Task %s health check resonse: %v\n", t.ID, resp.StatusCode)
	return nil
}

// getHostPort is a helper function that returns
// the host port where the task is listening.
func getHostPort(ports nat.PortMap) *string {
	for k, _ := range ports {
		return &ports[k][0].HostPort
	}
	return nil
}

func (m *Manager) doHealthChecks() {
	for _, t := range m.GetTasks() {
		if t.State == task.Running && t.RestartCount < 3 {
			err := m.checkTaskHealth(*t)
			if err != nil {
				if t.RestartCount < 3 {
					m.restartTask(t)
				}
			}
		} else if t.State == task.Failed && t.RestartCount < 3 {
			m.restartTask(t)
		}
	}
}

func (m *Manager) restartTask(t *task.Task) {
	w := m.TaskWorkerMap[t.ID]
	t.State = task.Scheduled
	t.RestartCount++
	m.TaskDb[t.ID] = t
	taskEvent := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Running,
		Timestamp: time.Now(),
		Task:      *t,
	}
	data, err := json.Marshal(taskEvent)
	if err != nil {
		log.Printf("Unable to marshal task object: %v\n", t)
		return
	}
	url := fmt.Sprintf("http://%s/tasks", w)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("Error connecting to %v: %v\n", w, err)
		m.Pending.Enqueue(t)
		return
	}
	d := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		e := worker.ErrResponse{}
		err := d.Decode(&e)
		if err != nil {
			fmt.Println("Error decoding response: %s\n", err.Error())
			return
		}
		log.Printf("Response error (%d): %s", e.HTTPStatusCode, e.Message)
		return
	}
	newTask := task.Task{}
	err = d.Decode(&newTask)
	if err != nil {
		log.Printf("Error decoding response: %s\n", err.Error())
		return
	}
	log.Printf("%#v\n", t)
}

func (m *Manager) DoHealthChecks() {
	for {
		log.Println("[manager.Manager] [DoHealthChecks] Performing task health check")
		m.doHealthChecks()
		log.Println("[manager.Manager] [DoHealthChecks]  Task health checks completed")
		log.Println("[manager.Manager] [DoHealthChecks]  Sleeping for 60 seconds")
		time.Sleep(60 * time.Second)
	}
}
