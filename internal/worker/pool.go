package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"taskforge/internal/job"
	"taskforge/internal/metrics"
	"taskforge/internal/store"
)

var (
	ErrQueueFull   = errors.New("queue is full")
	ErrUnknownType = errors.New("unknown job type")
)

type Pool struct {
	store      *store.Store[string, job.Job]
	metrics    *metrics.Metrics
	handlers   map[string]Handler
	queue      chan string // holds job IDs
	workers    int
	jobTimeout time.Duration

	wg   sync.WaitGroup
	quit chan struct{}
	once sync.Once
}

func NewPool(workers, queueSize int, s *store.Store[string, job.Job], m *metrics.Metrics) *Pool {
	return &Pool{
		store:      s,
		metrics:    m,
		handlers:   make(map[string]Handler),
		queue:      make(chan string, queueSize),
		workers:    workers,
		jobTimeout: 5 * time.Second,
		quit:       make(chan struct{}),
	}
}

// Register must be called before Start (the map is read-only afterwards,
// which is why reading it from many goroutines is safe).
func (p *Pool) Register(jobType string, h Handler) {
	p.handlers[jobType] = h
}

func (p *Pool) Start() {
	for i := 1; i <= p.workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
	log.Printf("started %d workers", p.workers)
}

func (p *Pool) Stop() {
	p.once.Do(func() { close(p.quit) })
	p.wg.Wait()
}

func (p *Pool) Submit(j job.Job) error {
	if _, ok := p.handlers[j.Type]; !ok {
		return fmt.Errorf("%w: %q", ErrUnknownType, j.Type)
	}
	p.store.Set(j.ID, j)

	select {
	case p.queue <- j.ID:
		p.metrics.IncSubmitted()
		return nil
	default: // queue full: don't block the HTTP request
		p.store.Delete(j.ID)
		return ErrQueueFull
	}
}

func (p *Pool) worker(id int) {
	defer p.wg.Done()
	for {
		select {
		case <-p.quit:
			return
		case jobID := <-p.queue:
			p.process(id, jobID)
		}
	}
}

func (p *Pool) process(workerID int, id string) {
	j, ok := p.store.Update(id, func(cur job.Job) job.Job {
		cur.Status = job.Running
		cur.Attempts++
		cur.UpdatedAt = time.Now()
		return cur
	})
	if !ok {
		return
	}
	log.Printf("[worker %d] running job %s (%s), attempt %d", workerID, j.ID, j.Type, j.Attempts)

	ctx, cancel := context.WithTimeout(context.Background(), p.jobTimeout)
	start := time.Now()
	err := p.handlers[j.Type](ctx, j)
	cancel()
	elapsed := time.Since(start)

	if err == nil {
		p.store.Update(id, func(cur job.Job) job.Job {
			cur.Status = job.Completed
			cur.Error = ""
			cur.UpdatedAt = time.Now()
			return cur
		})
		p.metrics.RecordCompleted(elapsed)
		log.Printf("[worker %d] job %s completed in %v", workerID, j.ID, elapsed)
		return
	}

	if j.Attempts <= j.MaxRetries {
		p.store.Update(id, func(cur job.Job) job.Job {
			cur.Status = job.Retrying
			cur.Error = err.Error()
			cur.UpdatedAt = time.Now()
			return cur
		})
		p.metrics.IncRetried()
		delay := backoff(j.Attempts)
		log.Printf("[worker %d] job %s failed (%v), retrying in %v", workerID, j.ID, err, delay)

		p.wg.Add(1)
		go p.requeueAfter(id, delay)
		return
	}

	p.store.Update(id, func(cur job.Job) job.Job {
		cur.Status = job.Failed
		cur.Error = err.Error()
		cur.UpdatedAt = time.Now()
		return cur
	})
	p.metrics.IncFailed()
	log.Printf("[worker %d] job %s permanently failed: %v", workerID, j.ID, err)
}

// requeueAfter waits, then puts the job ID back on the queue.
func (p *Pool) requeueAfter(id string, delay time.Duration) {
	defer p.wg.Done()
	select {
	case <-time.After(delay):
		select {
		case p.queue <- id:
		case <-p.quit:
		}
	case <-p.quit:
	}
}

// backoff: attempt 1 -> 500ms, 2 -> 1s, 3 -> 2s, ...
func backoff(attempt int) time.Duration {
	base := 500 * time.Millisecond
	return base * time.Duration(1<<(attempt-1))
}