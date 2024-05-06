package worker

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/vasilii314/orchestrator/task"
)

type Worker struct {
	Name string
	// Queue is needed to accept tasks from manager
	// and run them in FIFO fashion
	Queue queue.Queue
	// Db is needed to track all the tasks
	// assigned to this Worker
	Db map[uuid.UUID]*task.Task
	// Convenience field
	TaskCount int
	Stats     *Stats
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
	taskPersisted, ok := w.Db[taskQueued.ID]
	if !ok {
		taskPersisted = &taskQueued
		w.Db[taskQueued.ID] = &taskQueued
	}
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
		w.Db[t.ID] = &t
		return result
	}
	t.ContainerID = result.ContainerId
	t.State = task.Running
	w.Db[t.ID] = &t
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
	w.Db[t.ID] = &t
	log.Printf("[worker.Worker] [StopTask] Stopped and removed container %v for task %v\n", t.ContainerID, t.ID)
	return result
}

// AddTask adds a task to a temporary storage.
func (w *Worker) AddTask(t task.Task) {
	w.Queue.Enqueue(t)
}

func (w *Worker) GetTasks() []*task.Task {
	res := make([]*task.Task, 0, len(w.Db))
	for _, v := range w.Db {
		if v != nil {
			res = append(res, v)
		}
	}
	return res
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
	for id, t := range w.Db {
		if t.State == task.Running {
			resp := w.InspectTask(*t)
			if resp.Error != nil {
				log.Printf("[worker.Worker] [updateTasks] ERROR: %v\n", resp.Error)
			}
			if resp.Container == nil {
				log.Printf("[worker.Worker] [updateTasks] No container for running task %s\n", id)
				w.Db[id].State = task.Failed
			}
			if resp.Container.State.Status == "exited" {
				log.Printf("[worker.Worker] [updateTasks] Container for task %s in non-running state %s", id, resp.Container.State.Status)
				w.Db[id].State = task.Failed
			}
			w.Db[id].HostPorts = resp.Container.NetworkSettings.NetworkSettingsBase.Ports
		}
	}
}
