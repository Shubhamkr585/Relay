package store

import (
	"sync"
	"time"
)

// Clock allows injecting time into the engine for deterministic behavior in tests.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

// ControllableClock is a thread-safe deterministic clock for tests.
type ControllableClock struct {
	mu  sync.RWMutex
	now time.Time
}

// NewControllableClock returns a clock initialized at a given time.
func NewControllableClock(initial time.Time) *ControllableClock {
	if initial.IsZero() {
		initial = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	}
	return &ControllableClock{now: initial}
}

// Now returns the current simulated time.
func (c *ControllableClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

// Advance shifts the simulated time forward by d.
func (c *ControllableClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Set explicitly sets the simulated time.
func (c *ControllableClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}
