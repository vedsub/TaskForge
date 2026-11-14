package metrics

import (
	"sync"
	"time"
)

type Metrics struct {
	mu        sync.Mutex
	submitted int
	completed int
	failed    int
	retried   int
	totalTime time.Duration
}

type Snapshot struct {
	Submitted     int     `json:"submitted"`
	Completed     int     `json:"completed"`
	Failed        int     `json:"failed"`
	Retried       int     `json:"retried"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
}

func (m *Metrics) IncSubmitted() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.submitted++
}

func (m *Metrics) IncFailed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failed++
}

func (m *Metrics) IncRetried() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retried++
}

func (m *Metrics) RecordCompleted(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completed++
	m.totalTime += d
}

func (m *Metrics) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := Snapshot{
		Submitted: m.submitted,
		Completed: m.completed,
		Failed:    m.failed,
		Retried:   m.retried,
	}
	if m.completed > 0 {
		s.AvgDurationMs = float64(m.totalTime.Milliseconds()) / float64(m.completed)
	}
	return s
}


















