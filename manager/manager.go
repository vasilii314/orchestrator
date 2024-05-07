package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/vasilii314/orchestrator/node"
	"github.com/vasilii314/orchestrator/scheduler"
	"github.com/vasilii314/orchestrator/store"
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
	TaskDb store.Store[string, *task.Task]
	// TaskEventDb (in-memory db) used to track
	// all task events in the system
	TaskEventDb store.Store[string, *task.TaskEvent]
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
	LastWorker  int
	WorkerNodes []*node.Node
	Scheduler   scheduler.Scheduler
}

// This method is used to schedule tasks
// recieved from a user onto workers
func (m *Manager) SelectWorker(t task.Task) (*node.Node, error) {
	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if candidates == nil {
		msg := fmt.Sprintf("No available candidates match resource request for task %v\n", t.ID)
		err := errors.New(msg)
		return nil, err
	}
	scores := m.Scheduler.Score(t, candidates)
	selectedNode := m.Scheduler.Pick(scores, candidates)
	return selectedNode, nil
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
			taskPersisited, err := m.TaskDb.Get(t.ID.String())
			if err != nil {
				log.Printf("[manager.Manager] [updateTasks] Task with ID %s not found\n", t.ID)
				continue
			}
			if taskPersisited.State != t.State {
				taskPersisited.State = t.State
			}
			taskPersisited.StartTime = t.StartTime
			taskPersisited.FinishTime = t.FinishTime
			taskPersisited.ContainerID = t.ContainerID
			taskPersisited.HostPorts = t.HostPorts
			m.TaskDb.Put(taskPersisited.ID.String(), taskPersisited)
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

func (m *Manager) stopTask(worker, taskID string) {
	client := &http.Client{}
	url := fmt.Sprintf("http://%s/tasks/%s", worker, taskID)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		log.Printf("[manager.Manager] [stopTask] Error creating request to delete task %s: %v\n", taskID, err)
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[manager.Manager] [stopTask] Error connecting to worker %s: %v\n", url, err)
		return
	}
	if resp.StatusCode != 204 {
		log.Printf("[manager.Manager] [stopTask] Error sending request: %v\n", err)
		return
	}
	log.Printf("[manager.Manager] [stopTask] Task %s has been scheduled to be stopped", taskID)
}

func (m *Manager) SendWork() {
	if m.Pending.Len() > 0 {
		e := m.Pending.Dequeue()
		taskEvent := e.(task.TaskEvent)
		err := m.TaskEventDb.Put(taskEvent.ID.String(), &taskEvent)
		if err != nil {
			log.Printf("[manager.Manager] [SendWork] Error attempting to store task event %s: %v\n", taskEvent.ID.String(), err)
			return
		}
		log.Printf("[manager.Manager] [SendWork] Pulled %v off pending queue\n", taskEvent)
		taskWorker, ok := m.TaskWorkerMap[taskEvent.Task.ID]
		if ok {
			persistedTask, err := m.TaskDb.Get(taskEvent.Task.ID.String())
			if err != nil {
				log.Printf("[manager.Manager] [SendWork] Unable to schedule task^ %s\n", err.Error())
				return
			}
			if taskEvent.State == task.Completed && task.IsValidStateTransition(persistedTask.State, taskEvent.State) {
				m.stopTask(taskWorker, taskEvent.Task.ID.String())
				return
			}
			log.Printf("[manager.Manager] [SendWork] Invalid request: existing task %s is in state %v and cannot transition to the completed state\n", persistedTask.ID.String(), persistedTask.State)
			return
		}
		t := taskEvent.Task
		w, err := m.SelectWorker(t)
		if err != nil {
			log.Printf("[manager.Manager] [SendWork] Error selecting worker for task %s: %v\n", t.ID, err)
			m.Pending.Enqueue(taskEvent)
			return
		}
		m.WorkerTaskMap[w.Name] = append(m.WorkerTaskMap[w.Name], taskEvent.Task.ID)
		m.TaskWorkerMap[t.ID] = w.Name
		t.State = task.Scheduled
		m.TaskDb.Put(t.ID.String(), &t)
		data, err := json.Marshal(taskEvent)
		if err != nil {
			log.Printf("[manager.Manager] [SendWork] Unable to marshal task object^ %v\n", t)
			m.Pending.Enqueue(taskEvent)
			return
		}
		url := fmt.Sprintf("http://%s/tasks", w.Name)
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

func New(workers []string, schedulerType scheduler.SchedulerType, storeType store.StoreType) *Manager {
	workerTaskMap := make(map[string][]uuid.UUID)
	taskWorkerMap := make(map[uuid.UUID]string)
	var nodes []*node.Node
	for i := range workers {
		workerTaskMap[workers[i]] = []uuid.UUID{}
		nodeApi := fmt.Sprintf("http://%v", workers[i])
		n := node.NewNode(workers[i], nodeApi, "worker")
		nodes = append(nodes, n)
	}
	var s scheduler.Scheduler
	switch schedulerType {
	case scheduler.RoundRobinType:
		s = &scheduler.RoundRobin{Name: "roundrobin"}
	case scheduler.EpvmType:
		s = &scheduler.Epvm{Name: "epvm"}
	default:
		s = &scheduler.RoundRobin{Name: "roundrobin"}
	}
	m := Manager{
		Pending:       *queue.New(),
		Workers:       workers,
		WorkerTaskMap: workerTaskMap,
		TaskWorkerMap: taskWorkerMap,
		WorkerNodes:   nodes,
		Scheduler:     s,
	}
	var ts store.Store[string, *task.Task]
	var es store.Store[string, *task.TaskEvent]
	switch storeType {
	case store.InMemoryStore:
		ts = store.NewInMemoryTaskStore()
		es = store.NewInMemoryTaskEventStore()
	default:
		ts = store.NewInMemoryTaskStore()
		es = store.NewInMemoryTaskEventStore()
	}
	m.TaskDb = ts
	m.TaskEventDb = es
	return &m
}

func (m *Manager) GetTasks() []*task.Task {
	taskList, err := m.TaskDb.List()
	if err != nil {
		log.Printf("[manager.Manager] [GetTasks] Error getting list of tasks: %v\n", err)
		return nil
	}
	return taskList
}

func (m *Manager) checkTaskHealth(t task.Task) error {
	log.Printf("[manager.Manager] [checkTaskHealth] Calling health check for task %s: %s\n", t.ID, t.HealthCheck)
	w := m.TaskWorkerMap[t.ID]
	hostPort := getHostPort(t.HostPorts)
	worker := strings.Split(w, ":")
	fmt.Println("FDSDSFDFS", hostPort)
	fmt.Println(t)
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
	m.TaskDb.Put(t.ID.String(), t)
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
