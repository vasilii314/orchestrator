package store

import (
	"fmt"
	"github.com/vasilii314/orchestrator/task"
)

type InMemoryTaskStore struct {
	Db map[string]*task.Task
}

func NewInMemoryTaskStore() *InMemoryTaskStore {
	return &InMemoryTaskStore{
		Db: make(map[string]*task.Task),
	}
}

func (i *InMemoryTaskStore) Put(key string, t *task.Task) error {
	i.Db[key] = t
	return nil
}

func (i *InMemoryTaskStore) Get(key string) (*task.Task, error) {
	t, ok := i.Db[key]
	if !ok {
		return nil, fmt.Errorf("task with key %s does not exist", key)
	}
	return t, nil
}

func (i *InMemoryTaskStore) List() ([]*task.Task, error) {
	tasks := make([]*task.Task, 0, len(i.Db))
	for _, t := range i.Db {
		tasks = append(tasks, t)
	}
	return tasks, nil
}

func (i *InMemoryTaskStore) Count() (int, error) {
	return len(i.Db), nil
}

type InMemoryTaskEventStore struct {
	Db map[string]*task.TaskEvent
}

func NewInMemoryTaskEventStore() *InMemoryTaskEventStore {
	return &InMemoryTaskEventStore{
		Db: make(map[string]*task.TaskEvent),
	}
}

func (i *InMemoryTaskEventStore) Put(key string, t *task.TaskEvent) error {
	i.Db[key] = t
	return nil
}

func (i *InMemoryTaskEventStore) Get(key string) (*task.TaskEvent, error) {
	t, ok := i.Db[key]
	if !ok {
		return nil, fmt.Errorf("task event with key %s does not exist", key)
	}
	return t, nil
}

func (i *InMemoryTaskEventStore) List() ([]*task.TaskEvent, error) {
	tasks := make([]*task.TaskEvent, 0, len(i.Db))
	for _, t := range i.Db {
		tasks = append(tasks, t)
	}
	return tasks, nil
}

func (i *InMemoryTaskEventStore) Count() (int, error) {
	return len(i.Db), nil
}
