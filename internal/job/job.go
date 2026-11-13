package job

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"
)


type Status int

const (
	Pending   Status = iota 
	Running                 
	Completed              
	Failed                  
	Retrying              
)

func (s Status) String() string {
	switch s {
	case Pending:
		return "pending"
	case Running:
		return "running"
	case Completed:
		return "completed"
	case Failed:
		return "failed"
	case Retrying:
		return "retrying"
	default:
		return "unknown"
	}
}


func (s Status) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

type Job struct {
	ID         string            `json:"id"`
	Type       string            `json:"type"`
	Payload    map[string]string `json:"payload"`
	Status     Status            `json:"status"`
	Attempts   int               `json:"attempts"`
	MaxRetries int               `json:"max_retries"`
	Error      string            `json:"error,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

func New(jobType string, payload map[string]string, maxRetries int) Job {
	if maxRetries < 0 {
		maxRetries = 0
	}
	now := time.Now()
	return Job{
		ID:         newID(),
		Type:       jobType,
		Payload:    payload,
		Status:     Pending,
		MaxRetries: maxRetries,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}


func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}