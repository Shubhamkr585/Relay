//go:build windows

package bench

import (
	"fmt"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

var (
	kernel32                      = syscall.NewLazyDLL("kernel32.dll")
	procQueryPerformanceCounter   = kernel32.NewProc("QueryPerformanceCounter")
	procQueryPerformanceFrequency = kernel32.NewProc("QueryPerformanceFrequency")

	qpcInitOnce sync.Once
	qpcFreq     int64
	qpcEnabled  bool
)

func initQPC() {
	qpcInitOnce.Do(func() {
		var freq int64
		r, _, _ := procQueryPerformanceFrequency.Call(uintptr(unsafe.Pointer(&freq)))
		if r != 0 && freq > 0 {
			qpcFreq = freq
			qpcEnabled = true
		}
	})
}

// HighResTime represents a high-resolution monotonic timestamp.
type HighResTime struct {
	qpcTicks int64
	wall     time.Time
}

// NowHighRes returns the current high-resolution monotonic timestamp.
func NowHighRes() HighResTime {
	initQPC()
	if qpcEnabled {
		var ticks int64
		_, _, _ = procQueryPerformanceCounter.Call(uintptr(unsafe.Pointer(&ticks)))
		return HighResTime{qpcTicks: ticks}
	}
	return HighResTime{wall: time.Now()}
}

// SinceHighRes returns the elapsed duration since the provided start timestamp.
func SinceHighRes(start HighResTime) time.Duration {
	if qpcEnabled && start.qpcTicks > 0 {
		var endTicks int64
		_, _, _ = procQueryPerformanceCounter.Call(uintptr(unsafe.Pointer(&endTicks)))
		diff := endTicks - start.qpcTicks
		if diff <= 0 {
			return 0
		}
		secs := diff / qpcFreq
		remTicks := diff % qpcFreq
		ns := (remTicks * 1000000000) / qpcFreq
		return time.Duration(secs)*time.Second + time.Duration(ns)*time.Nanosecond
	}
	return time.Since(start.wall)
}

// GetTimerImplementationName returns the active timer name and measured frequency.
func GetTimerImplementationName() string {
	initQPC()
	if qpcEnabled {
		return fmt.Sprintf("Windows QueryPerformanceCounter (QPC freq: %.2f MHz)", float64(qpcFreq)/1e6)
	}
	return "Go runtime monotonic clock"
}
