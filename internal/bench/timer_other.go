//go:build !windows

package bench

import "time"

// HighResTime represents a high-resolution monotonic timestamp on non-Windows OSes.
type HighResTime struct {
	wall time.Time
}

// NowHighRes returns the current high-resolution monotonic timestamp.
func NowHighRes() HighResTime {
	return HighResTime{wall: time.Now()}
}

// SinceHighRes returns the elapsed duration since the provided start timestamp.
func SinceHighRes(start HighResTime) time.Duration {
	return time.Since(start.wall)
}

// GetTimerImplementationName returns the active timer name.
func GetTimerImplementationName() string {
	return "POSIX monotonic clock (time.Now)"
}
