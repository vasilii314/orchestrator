package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/vasilii314/orchestrator/worker"
	"log"
	"net/http"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/vasilii314/orchestrator/task"
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

// UpdateTask will call CollectStats
// and update tasks respectively
func (m *Manager) UpdateTasks() {
	for _, worker := range m.Workers {
		log.Printf("Checking %v for task updates", worker)
		url := fmt.Sprintf("http://%s/tasks", worker)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("Error connecting to %v: %v\n", worker, err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			log.Printf("Error sending request: %v\n", err)
			continue
		}
		d := json.NewDecoder(resp.Body)
		var tasks []*task.Task
		err = d.Decode(&tasks)
		if err != nil {
			log.Printf("Error unmarshalling tasks: %s\n", err.Error())
			continue
		}
		for _, t := range tasks {
			log.Printf("Attempting to update task %v\n", t.ID)
			_, ok := m.TaskDb[t.ID]
			if !ok {
				log.Printf("Task with ID %s not found\n", t.ID)
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

func (m *Manager) SendWork() {
	if m.Pending.Len() > 0 {
		w := m.SelectWorker()
		e := m.Pending.Dequeue()
		taskEvent := e.(task.TaskEvent)
		t := taskEvent.Task
		log.Printf("Pulled %v off pending queue\n", t)
		m.TaskEventDb[taskEvent.ID] = &taskEvent
		m.WorkerTaskMap[w] = append(m.WorkerTaskMap[w], taskEvent.Task.ID)
		m.TaskWorkerMap[t.ID] = w
		t.State = task.Scheduled
		m.TaskDb[t.ID] = &t
		data, err := json.Marshal(taskEvent)
		if err != nil {
			log.Printf("Unable to marshal task object^ %v\n", t)
			return
		}
		url := fmt.Sprintf("http://%s/tasks", w)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			log.Printf("Error connecting to %v: %v\n", w, err)
			m.Pending.Enqueue(taskEvent)
			return
		}
		d := json.NewDecoder(resp.Body)
		if resp.StatusCode != http.StatusCreated {
			e := worker.ErrResponse{}
			err := d.Decode(&e)
			if err != nil {
				log.Printf("Error decoding response: %s\n", err.Error())
				return
			}
			log.Printf("Response error (%d): %s", e.HTTPStatusCode, e.Message)
			return
		}
		t = task.Task{}
		err = d.Decode(&t)
		if err != nil {
			log.Printf("Error decoding response: %s\n", err.Error())
			return
		}
		log.Printf("%#v\n", t)
	} else {
		log.Println("No work in the queue")
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
