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
	var ret string
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.m[key]
	if !ok {
		ret = "$-1\r\n"
		ok = false
	} else if !time.Time.IsZero(entry.time) && entry.time.Before(time.Now()) {
		delete(s.m, key)
		ret = "$-1\r\n"
		ok = false
	} else {
		ret = entry.value
		ok = true
	}

	return ret, ok

}

func (s *SafeMap) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
}
