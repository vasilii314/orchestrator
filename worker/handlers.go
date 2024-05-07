package worker

import (
	"encoding/json"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/vasilii314/orchestrator/task"
	"log"
	"net/http"
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
		msg := fmt.Sprintf("error unmarshalling body: %v\n", err)
		log.Printf("[worker.Api] [StartTaskHandler] %s\n", msg)
		w.WriteHeader(http.StatusBadRequest)
		e := ErrResponse{
			HTTPStatusCode: http.StatusBadRequest,
			Message:        msg,
		}
		json.NewEncoder(w).Encode(e)
		return
	}
	a.Worker.AddTask(taskEvent.Task)
	log.Printf("[worker.Api] [StartTaskHandler] task added: %v\n", taskEvent.Task.ID)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(taskEvent.Task)
}

func (a *Api) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(a.Worker.GetTasks())
}

func (a *Api) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskId := chi.URLParam(r, "taskID")
	if taskId == "" {
		log.Println("[worker.Api] [StopTaskHandler] No taskID passed in request.")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	tID, _ := uuid.Parse(taskId)
	t, err := a.Worker.Db.Get(tID.String())
	if err != nil {
		log.Printf("[worker.Api] [StopTaskHandler] No task with ID %v found\n", tID)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	taskCopy := *t
	taskCopy.State = task.Completed
	a.Worker.AddTask(taskCopy)
	log.Printf("[worker.Api] [StopTaskHandler] Added task %v to stop container %v\n", t.ID, t.ContainerID)
	w.WriteHeader(http.StatusNoContent)
}

func (a *Api) GetStatsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(a.Worker.Stats)
}
