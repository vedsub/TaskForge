package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"taskforge/internal/api"
	"taskforge/internal/job"
	"taskforge/internal/metrics"
	"taskforge/internal/store"
	"taskforge/internal/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	jobStore := store.New[string, job.Job]()
	m := &metrics.Metrics{}

	pool := worker.NewPool(4, 100, jobStore, m)
	pool.Register("email", worker.EmailHandler)
	pool.Register("report", worker.ReportHandler)
	pool.Start()

	srv := &http.Server{
		Addr:    ":8080",
		Handler: api.NewServer(pool, jobStore, m).Routes(),
	}

	go func() {
		log.Println("listening on :8080")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done() // block until Ctrl+C
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	pool.Stop()
	log.Println("bye")
}
