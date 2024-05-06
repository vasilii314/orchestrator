package manager

import (
	"encoding/json"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/vasilii314/orchestrator/task"
	"log"
	"net/http"
	"time"
)

type ErrResponse struct {
	HTTPStatusCode int
	Message        string
}

func (a *Api) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	taskEvent := task.TaskEvent{}
	err := d.Decode(&taskEvent)
	if err != nil {
		msg := fmt.Sprintf("Error unmarshalling body: %v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		e := ErrResponse{
			HTTPStatusCode: http.StatusBadRequest,
			Message:        msg,
		}
		json.NewEncoder(w).Encode(e)
		return
	}
	a.Manager.AddTask(taskEvent)
	log.Printf("[manager.Api] [StartTaskHandler] Added task %v\n", taskEvent.Task.ID)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(taskEvent.Task)
}

func (a *Api) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(a.Manager.GetTasks())
}

func (a *Api) StopTasksHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	if taskID == "" {
		log.Println("[manager.Api] [StopTasksHandler] No taskID passed in request")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	tID, _ := uuid.Parse(taskID)
	taskToStop, ok := a.Manager.TaskDb[tID]
	if !ok {
		log.Printf("[manager.Api] [StopTasksHandler] No task with ID %v found\n", tID)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	taskEvent := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Completed,
		Timestamp: time.Now(),
	}
	taskCopy := *taskToStop
	taskCopy.State = task.Completed
	taskEvent.Task = taskCopy
	a.Manager.AddTask(taskEvent)
	log.Printf("[manager.Api] [StopTasksHandler] Added task event %v to stop task %v\n", taskEvent.ID, taskToStop.ID)
	w.WriteHeader(http.StatusNoContent)
}
