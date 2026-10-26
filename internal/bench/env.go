package bench

import (
	"fmt"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// RelayBenchConfig holds the active Relay operational configuration parameters.
type RelayBenchConfig struct {
	BatchWindow     time.Duration
	BatchMaxSize    int
	RaftHeartbeat   time.Duration
	RaftElection    time.Duration
	RaftLeaderLease time.Duration
	FsyncMode       string
}

// MeasureClockResolution measures the smallest positive duration delta between back-to-back high-resolution timer calls.
func MeasureClockResolution() time.Duration {
	minDiff := time.Hour
	for i := 0; i < 1000; i++ {
		t1 := NowHighRes()
		for {
			diff := SinceHighRes(t1)
			if diff > 0 {
				if diff < minDiff {
					minDiff = diff
				}
				break
			}
		}
	}
	if minDiff == time.Hour || minDiff <= 0 {
		minDiff = 100 * time.Nanosecond
	}
	return minDiff
}

// DefaultRelayBenchConfig provides the production default Relay configuration.
func DefaultRelayBenchConfig() RelayBenchConfig {
	return RelayBenchConfig{
		BatchWindow:     2 * time.Millisecond,
		BatchMaxSize:    64,
		RaftHeartbeat:   1000 * time.Millisecond,
		RaftElection:    1000 * time.Millisecond,
		RaftLeaderLease: 500 * time.Millisecond,
		FsyncMode:       "Raft WAL fsync ON, FSM bbolt NoSync; B1 engine fsync mode stated separately",
	}
}

// CaptureEnv captures all machine and system environment details.
func CaptureEnv(cfg RelayBenchConfig, mode string) *EnvInfo {
	if mode == "" {
		mode = "full"
	}

	clockRes := MeasureClockResolution()
	SetMeasuredClockResolution(clockRes)

	info := &EnvInfo{
		CapturedAt:          time.Now().Format(time.RFC3339),
		Mode:                mode,
		GoVersion:           runtime.Version(),
		OS:                  runtime.GOOS,
		Arch:                runtime.GOARCH,
		LogicalCores:        runtime.NumCPU(),
		GOMAXPROCS:          runtime.GOMAXPROCS(0),
		TimerImplementation: GetTimerImplementationName(),
		ClockResolution:     FormatDuration(clockRes),
		ClockResolutionNs:   clockRes.Nanoseconds(),
		ConfigLine:          "Raft WAL fsync ON, FSM bbolt NoSync; B1 engine fsync mode stated separately",
		BatchWindow:         cfg.BatchWindow.String(),
		BatchMaxSize:        cfg.BatchMaxSize,
		RaftHeartbeat:       cfg.RaftHeartbeat.String(),
		RaftElection:        cfg.RaftElection.String(),
		RaftLeaderLease:     cfg.RaftLeaderLease.String(),
		RaftTimeouts: map[string]string{
			"b2_cluster":             "heartbeat=1s, election=1s, leader_lease=500ms",
			"b3_openloop":            "heartbeat=1s, election=1s, leader_lease=500ms",
			"b4_e2e":                 "heartbeat=1s, election=1s, leader_lease=500ms",
			"b5_failover_default":    "heartbeat=1s, election=1s, leader_lease=500ms",
			"b5_failover_aggressive": "heartbeat=100ms, election=300ms, leader_lease=150ms",
		},
		FsyncMode:     cfg.FsyncMode,
		PreflightNote: "PRE-FLIGHT REMINDER: Ensure laptop is plugged in, power mode is set to Best Performance, and other heavy applications are closed.",
	}

	// 1. Git Commit Hash & Dirty Check
	if out, err := exec.Command("git", "rev-parse", "HEAD").Output(); err == nil {
		info.GitCommit = strings.TrimSpace(string(out))
	} else {
		info.GitCommit = "unknown"
	}
	if out, err := exec.Command("git", "status", "--porcelain").Output(); err == nil {
		info.GitDirty = len(strings.TrimSpace(string(out))) > 0
	}

	// 2. Platform-specific environment detection
	if runtime.GOOS == "windows" {
		detectWindowsEnv(info)
	} else {
		detectUnixEnv(info)
	}

	return info
}

func detectWindowsEnv(info *EnvInfo) {
	// CPU Model
	cmd := exec.Command("powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_Processor).Name")
	if out, err := cmd.Output(); err == nil {
		info.CPUModel = strings.TrimSpace(string(out))
	}

	// Total RAM
	cmd = exec.Command("powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory")
	if out, err := cmd.Output(); err == nil {
		str := strings.TrimSpace(string(out))
		if bytesVal, err := strconv.ParseUint(str, 10, 64); err == nil {
			info.TotalRAMBytes = bytesVal
			info.TotalRAMGB = fmt.Sprintf("%.2f GB", float64(bytesVal)/(1024*1024*1024))
		}
	}

	// OS Caption
	cmd = exec.Command("powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_OperatingSystem).Caption")
	if out, err := cmd.Output(); err == nil {
		info.OSCaption = strings.TrimSpace(string(out))
	}

	// Disk Type
	cmd = exec.Command("powershell", "-NoProfile", "-Command", "(Get-PhysicalDisk | Select-Object -First 1).MediaType")
	if out, err := cmd.Output(); err == nil {
		info.DiskType = strings.TrimSpace(string(out))
	}
	if info.DiskType == "" {
		info.DiskType = "SSD/NVMe"
	}

	// Power Plan (fix truncation: use LastIndex to preserve nested parentheses)
	cmd = exec.Command("powercfg", "/getactivescheme")
	if out, err := cmd.Output(); err == nil {
		str := strings.TrimSpace(string(out))
		idx := strings.Index(str, "(")
		endIdx := strings.LastIndex(str, ")")
		if idx != -1 && endIdx > idx {
			info.PowerPlan = strings.TrimSpace(str[idx+1 : endIdx])
		} else {
			info.PowerPlan = str
		}
	}

	// Battery / AC Status
	cmd = exec.Command("powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_Battery).BatteryStatus")
	if out, err := cmd.Output(); err == nil {
		statusStr := strings.TrimSpace(string(out))
		if statusStr == "1" {
			info.ACStatus = "Battery (Discharging)"
			info.IsOnBattery = true
		} else if statusStr != "" {
			info.ACStatus = "AC Power (Plugged in)"
			info.IsOnBattery = false
		} else {
			info.ACStatus = "AC Power (Desktop / No Battery)"
			info.IsOnBattery = false
		}
	} else {
		info.ACStatus = "AC Power (Desktop / No Battery)"
		info.IsOnBattery = false
	}
}

func detectUnixEnv(info *EnvInfo) {
	// Fallbacks for linux/darwin
	info.CPUModel = fmt.Sprintf("%d cores (%s)", runtime.NumCPU(), runtime.GOARCH)
	info.OSCaption = runtime.GOOS
	info.DiskType = "SSD"
	info.PowerPlan = "Default"
	info.ACStatus = "AC Power (Unix Default)"
	info.IsOnBattery = false
	info.TotalRAMGB = "N/A"
}
