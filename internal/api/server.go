package api

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sort"
	"time"

	"taskforge/internal/job"
	"taskforge/internal/metrics"
	"taskforge/internal/store"
	"taskforge/internal/worker"
)

type Server struct {
	pool    *worker.Pool
	store   *store.Store[string, job.Job]
	metrics *metrics.Metrics
}

func NewServer(p *worker.Pool, s *store.Store[string, job.Job], m *metrics.Metrics) *Server {
	return &Server{pool: p, store: s, metrics: m}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.handleCreate)
	mux.HandleFunc("GET /jobs", s.handleList)
	mux.HandleFunc("GET /jobs/{id}", s.handleGet)
	mux.HandleFunc("GET /stats", s.handleStats)
	return logging(mux)
}

type createRequest struct {
	Type       string            `json:"type"`
	Payload    map[string]string `json:"payload"`
	MaxRetries int               `json:"max_retries"`
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	j := job.New(req.Type, req.Payload, req.MaxRetries)
	if err := s.pool.Submit(j); err != nil {
		switch {
		case errors.Is(err, worker.ErrUnknownType):
			writeError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, worker.ErrQueueFull):
			writeError(w, http.StatusServiceUnavailable, "queue is full, try again later")
		default:
			writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}
	writeJSON(w, http.StatusAccepted, j)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, ok := s.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	jobs := s.store.List()
	if status := r.URL.Query().Get("status"); status != "" {
		jobs = store.Filter(jobs, func(j job.Job) bool {
			return j.Status.String() == status
		})
	}
	sort.Slice(jobs, func(a, b int) bool {
		return jobs[a].CreatedAt.Before(jobs[b].CreatedAt)
	})
	writeJSON(w, http.StatusOK, jobs)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.metrics.Snapshot())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// logging is middleware: it wraps another handler.
func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s (%v)", r.Method, r.URL.Path, time.Since(start))
	})
}