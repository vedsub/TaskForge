package store

import "sync"

type Store[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]V
}

func New[K comparable, V any]() *Store[K, V] {
	return &Store[K, V]{data: make(map[K]V)}
}

func (s *Store[K, V]) Set(key K, value V) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *Store[K, V]) Get(key K) (V, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Update atomically applies fn to the stored value.
func (s *Store[K, V]) Update(key K, fn func(V) V) (V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	if !ok {
		var zero V
		return zero, false
	}
	v = fn(v)
	s.data[key] = v
	return v, true
}

func (s *Store[K, V]) Delete(key K) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

func (s *Store[K, V]) List() []V {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]V, 0, len(s.data))
	for _, v := range s.data {
		out = append(out, v)
	}
	return out
}

// Filter is a generic *function* (not a method).
func Filter[T any](items []T, keep func(T) bool) []T {
	out := make([]T, 0)
	for _, item := range items {
		if keep(item) {
			out = append(out, item)
		}
	}
	return out
}