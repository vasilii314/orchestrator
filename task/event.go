package task

import (
	"time"

	"github.com/google/uuid"
)

// TaskEvent is an internal object used to
// transition tasks from one state to another
//
//	TaskEvent.State - stores state value to which a task
//	should transition to
type TaskEvent struct {
	ID        uuid.UUID
	State     State
	Timestamp time.Time
	Task      Task
}
