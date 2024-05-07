package worker

import (
	"errors"
	"fmt"
	"github.com/vasilii314/orchestrator/store"
	"log"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/vasilii314/orchestrator/task"
)

type Worker struct {
	Name string
	// Queue is needed to accept tasks from manager
	// and run them in FIFO fashion
	Queue queue.Queue
	// Db is needed to track all the tasks
	// assigned to this Worker
	Db store.Store[string, *task.Task]
	// Convenience field
	TaskCount int
	Stats     *Stats
}

func New(name string, storeType store.StoreType) *Worker {
	w := Worker{
		Name:  name,
		Queue: *queue.New(),
	}
	var s store.Store[string, *task.Task]
	switch storeType {
	case store.InMemoryStore:
		s = store.NewInMemoryTaskStore()
	default:
		s = store.NewInMemoryTaskStore()
	}
	w.Db = s
	return &w
}

// CollectStats used to periodically
// collect statistics about the worker
func (w *Worker) CollectStats() {
	for {
		log.Println("[worker.Worker] [CollectStats] Collecting stats")
		w.Stats = GetStats()
		time.Sleep(15 * time.Second)
	}
}

// RunTask will handle running task on
// the machine where whe worker is running.
// This method is responsible for identifying
// the task's current state and then either
// starting or stopping a task based on the state.
func (w *Worker) RunTask() task.DockerResult {
	t := w.Queue.Dequeue()
	if t == nil {
		log.Println("[worker.Worker] [RunTask] No tasks in the queue")
		return task.DockerResult{Error: nil}
	}
	taskQueued := t.(task.Task)
	err := w.Db.Put(taskQueued.ID.String(), &taskQueued)
	if err != nil {
		msg := fmt.Errorf("error storing task %s: %v", taskQueued.ID.String(), err)
		log.Printf("[worker.Worker] [RunTask] %v\n", msg)
		return task.DockerResult{Error: msg}
	}
	queuedTask, err := w.Db.Get(taskQueued.ID.String())
	if err != nil {
		msg := fmt.Errorf("error getting task %s from database^ %v", taskQueued.ID.String(), err)
		log.Printf("[worker.Worker] [RunTask] %v\n", msg)
		return task.DockerResult{Error: msg}
	}
	taskPersisted := queuedTask
	var result task.DockerResult
	if task.IsValidStateTransition(taskPersisted.State, taskQueued.State) {
		switch taskQueued.State {
		case task.Scheduled:
			result = w.StartTask(taskQueued)
		case task.Completed:
			result = w.StopTask(taskQueued)
		default:
			result.Error = errors.New("invalid task state")
		}
	} else {
		err := fmt.Errorf("invalid transition from %v to %v", taskPersisted.State, taskQueued.State)
		result.Error = err
	}
	return result
}

func (w *Worker) StartTask(t task.Task) task.DockerResult {
	t.StartTime = time.Now().UTC()
	c := task.NewConfig(&t)
	d := task.NewDocker(c)
	result := d.Run()
	if result.Error != nil {
		log.Printf("[worker.Worker] [StartTask] Error running task %v: %v\n", t.ID, result.Error)
		t.State = task.Failed
		w.Db.Put(t.ID.String(), &t)
		return result
	}
	t.ContainerID = result.ContainerId
	t.State = task.Running
	w.Db.Put(t.ID.String(), &t)
	return result
}

func (w *Worker) StopTask(t task.Task) task.DockerResult {
	c := task.NewConfig(&t)
	d := task.NewDocker(c)
	result := d.Stop(t.ContainerID)
	if result.Error != nil {
		log.Printf("[worker.Worker] [StopTask] Error stopping container %v: %v\n", t.ContainerID, result.Error)
	}
	t.FinishTime = time.Now().UTC()
	t.State = task.Completed
	w.Db.Put(t.ID.String(), &t)
	log.Printf("[worker.Worker] [StopTask] Stopped and removed container %v for task %v\n", t.ContainerID, t.ID)
	return result
}

// AddTask adds a task to a temporary storage.
func (w *Worker) AddTask(t task.Task) {
	w.Queue.Enqueue(t)
}

func (w *Worker) GetTasks() []*task.Task {
	taskList, err := w.Db.List()
	if err != nil {
		log.Printf("[worker.Worker] [GetTasks] Error getting list of tasks: %v\n", err)
		return nil
	}
	return taskList
}

func (w *Worker) RunTasks() {
	for {
		if w.Queue.Len() != 0 {
			result := w.RunTask()
			if result.Error != nil {
				log.Printf("[worker.Worker] [RunTasks] Error running task: %v\n", result.Error)
			}
		} else {
			log.Println("[worker.Worker] [RunTasks] No tasks to process currently.")
		}
		log.Println("[worker.Worker] [RunTasks] Sleeping for 10 seconds.")
		time.Sleep(10 * time.Second)
	}
}

func (w *Worker) InspectTask(t task.Task) task.DockerInspectResponse {
	config := task.NewConfig(&t)
	d := task.NewDocker(config)
	return d.Inspect(t.ContainerID)
}

func (w *Worker) UpdateTasks() {
	for {
		log.Println("[worker.Worker] [UpdateTasks] Checking status of tasks")
		w.updateTasks()
		log.Println("[worker.Worker] [UpdateTasks] Task updates completed")
		log.Println("[worker.Worker] [UpdateTasks] Sleeping for 15 seconds")
		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) updateTasks() {
	tasks, err := w.Db.List()
	if err != nil {
		log.Printf("[worker.Worker] [updateTasks] Error getting list of tasks: %v\n", err)
		return
	}
	for _, t := range tasks {
		if t.State == task.Running {
			resp := w.InspectTask(*t)
			if resp.Error != nil {
				log.Printf("[worker.Worker] [updateTasks] ERROR: %v\n", resp.Error)
			}
			if resp.Container == nil {
				log.Printf("[worker.Worker] [updateTasks] No container for running task %s\n", t.ID.String())
				t.State = task.Failed
				w.Db.Put(t.ID.String(), t)
			}
			if resp.Container.State.Status == "exited" {
				log.Printf("[worker.Worker] [updateTasks] Container for task %s in non-running state %s", t.ID.String(), resp.Container.State.Status)
				t.State = task.Failed
				w.Db.Put(t.ID.String(), t)
			}
			t.HostPorts = resp.Container.NetworkSettings.NetworkSettingsBase.Ports
			w.Db.Put(t.ID.String(), t)
		}
	}
}
