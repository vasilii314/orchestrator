package store

import (
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/vasilii314/orchestrator/task"
	"log"
	"os"
)

type PersistentTaskStore struct {
	Db       *bolt.DB
	DbFile   string
	FileMode os.FileMode
	Bucket   string
}

func NewPersistentTaskStore(file string, mode os.FileMode, bucket string) (*PersistentTaskStore, error) {
	db, err := bolt.Open(file, mode, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to upen %v", file)
	}
	t := PersistentTaskStore{
		DbFile:   file,
		FileMode: mode,
		Db:       db,
		Bucket:   bucket,
	}
	err = t.CreateBucket()
	if err != nil {
		log.Printf("[store.PersistentTaskStore] [NewPersistentTaskStore] bucket already exists, will use it instead of creating a new one")
	}
	return &t, nil
}

func (t *PersistentTaskStore) Close() {
	t.Db.Close()
}

func (t *PersistentTaskStore) Count() (int, error) {
	taskCount := 0
	err := t.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.Bucket))
		b.ForEach(func(k, v []byte) error {
			taskCount++
			return nil
		})
		return nil
	})
	if err != nil {
		return -1, err
	}
	return taskCount, err
}

func (t *PersistentTaskStore) CreateBucket() error {
	return t.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(t.Bucket))
		if err != nil {
			return fmt.Errorf("error creating bucket %s: %s", t.Bucket, err)
		}
		return nil
	})
}

func (t *PersistentTaskStore) Put(key string, task *task.Task) error {
	return t.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.Bucket))
		buf, err := json.Marshal(task)
		if err != nil {
			return err
		}
		err = b.Put([]byte(key), buf)
		if err != nil {
			return err
		}
		return nil
	})
}

func (t *PersistentTaskStore) Get(key string) (*task.Task, error) {
	var task task.Task
	err := t.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.Bucket))
		taskRaw := b.Get([]byte(key))
		if t == nil {
			return fmt.Errorf("task %v not found", key)
		}
		err := json.Unmarshal(taskRaw, &task)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func (t *PersistentTaskStore) List() ([]*task.Task, error) {
	var tasks []*task.Task
	err := t.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.Bucket))
		b.ForEach(func(k, v []byte) error {
			var task task.Task
			err := json.Unmarshal(v, &task)
			if err != nil {
				return err
			}
			tasks = append(tasks, &task)
			return nil
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

type PersistentTaskEventStore struct {
	Db       *bolt.DB
	DbFile   string
	FileMode os.FileMode
	Bucket   string
}

func NewPersistentTaskEventStore(file string, mode os.FileMode, bucket string) (*PersistentTaskEventStore, error) {
	db, err := bolt.Open(file, mode, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to upen %v", file)
	}
	e := PersistentTaskEventStore{
		DbFile:   file,
		FileMode: mode,
		Db:       db,
		Bucket:   bucket,
	}
	err = e.CreateBucket()
	if err != nil {
		log.Printf("[store.PersistentTaskEventStore] [NewPersistentTaskEventStore] bucket already exists, will use it instead of creating a new one")
	}
	return &e, nil
}

func (e *PersistentTaskEventStore) Close() {
	e.Db.Close()
}

func (e *PersistentTaskEventStore) CreateBucket() error {
	return e.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(e.Bucket))
		if err != nil {
			return fmt.Errorf("error creating bucket %s: %s", e.Bucket, err)
		}
		return nil
	})
}

func (e *PersistentTaskEventStore) Count() (int, error) {
	taskCount := 0
	err := e.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(e.Bucket))
		b.ForEach(func(k, v []byte) error {
			taskCount++
			return nil
		})
		return nil
	})
	if err != nil {
		return -1, err
	}
	return taskCount, err
}

func (e *PersistentTaskEventStore) Put(key string, event *task.TaskEvent) error {
	return e.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(e.Bucket))
		buf, err := json.Marshal(event)
		if err != nil {
			return err
		}
		err = b.Put([]byte(key), buf)
		if err != nil {
			return err
		}
		return nil
	})
}

func (e *PersistentTaskEventStore) Get(key string) (*task.TaskEvent, error) {
	var task task.TaskEvent
	err := e.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(e.Bucket))
		taskRaw := b.Get([]byte(key))
		if e == nil {
			return fmt.Errorf("task %v not found", key)
		}
		err := json.Unmarshal(taskRaw, &task)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func (e *PersistentTaskEventStore) List() ([]*task.TaskEvent, error) {
	var events []*task.TaskEvent
	err := e.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(e.Bucket))
		b.ForEach(func(k, v []byte) error {
			var event task.TaskEvent
			err := json.Unmarshal(v, &event)
			if err != nil {
				return err
			}
			events = append(events, &event)
			return nil
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}
