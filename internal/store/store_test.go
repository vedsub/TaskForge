
package store

import (
	"sync"
	"testing"
)

func TestStoreConcurrentUpdate(t *testing.T) {
	s := New[string, int]()
	s.Set("counter", 0)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Update("counter", func(v int) int { return v + 1 })
		}()
	}
	wg.Wait()

	got, _ := s.Get("counter")
	if got != 100 {
		t.Fatalf("expected 100, got %d", got)
	}
}