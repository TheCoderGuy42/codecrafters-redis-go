package main

import (
	"sync"
	"time"
)

type Entry struct {
	value string
	time  time.Time
}

type SafeMap struct {
	mu sync.RWMutex
	m  map[string]Entry
}

func NewSafeMap() *SafeMap {
	return &SafeMap{
		m: make(map[string]Entry),
	}
}

func (s *SafeMap) Set(key string, value string, expiry time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = Entry{value: value, time: expiry}
}

func (s *SafeMap) Get(key string) (string, bool) {
	s.mu.RLock()
	entry, ok := s.m[key]
	s.mu.RUnlock()
	if !ok {
		return "", false
	}

	if !time.Time.IsZero(entry.time) && entry.time.Before(time.Now()) {
		s.mu.Lock()
		delete(s.m, key)
		s.mu.Unlock()
		return "", false

	}
	return entry.value, true

}

func (s *SafeMap) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
}
