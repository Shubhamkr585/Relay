package bench

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// GenerateReportFiles writes env.json, per-benchmark JSON files, and bench-report.md into outDir.
func GenerateReportFiles(outDir string, report *SuiteReport) (string, error) {
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create report directory: %w", err)
	}

	// 1. env.json
	envBytes, err := json.MarshalIndent(report.Env, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal env: %w", err)
	}
	if err := os.WriteFile(filepath.Join(outDir, "env.json"), envBytes, 0644); err != nil {
		return "", fmt.Errorf("failed to write env.json: %w", err)
	}

	// 2. Individual benchmark JSON files
	writeJSON := func(filename string, v interface{}) {
		if v == nil {
			return
		}
		data, err := json.MarshalIndent(v, "", "  ")
		if err == nil {
			_ = os.WriteFile(filepath.Join(outDir, filename), data, 0644)
		}
	}

	writeJSON("b0b_fsync.json", report.B0b_Fsync)
	writeJSON("b1_storage.json", report.B1_Storage)
	writeJSON("b2_cluster.json", report.B2_Cluster)
	writeJSON("b3_openloop.json", report.B3_OpenLoop)
	writeJSON("b4_e2e.json", report.B4_E2E)
	writeJSON("b5_failover.json", report.B5_Failover)

	// 3. bench-report.md
	reportMd := FormatMarkdownReport(report)
	reportPath := filepath.Join(outDir, "bench-report.md")
	if err := os.WriteFile(reportPath, []byte(reportMd), 0644); err != nil {
		return "", fmt.Errorf("failed to write bench-report.md: %w", err)
	}

	return reportPath, nil
}

// FormatMarkdownReport builds the complete markdown report containing tables and configs.
func FormatMarkdownReport(report *SuiteReport) string {
	var sb strings.Builder

	sb.WriteString("# Relay Benchmark Verification Report (Phase 5)\n\n")
	sb.WriteString(fmt.Sprintf("**Execution Date:** %s  \n", report.Env.CapturedAt))
	sb.WriteString(fmt.Sprintf("**Execution Mode:** `%s`  \n", report.Env.Mode))
	sb.WriteString(fmt.Sprintf("**Git Commit:** `%s` (Dirty: `%t`)  \n", report.Env.GitCommit, report.Env.GitDirty))
	sb.WriteString(fmt.Sprintf("**Configuration:** %s  \n", report.Env.ConfigLine))
	sb.WriteString(fmt.Sprintf("**Environment:** %s, %d Logical Cores, %s RAM, %s (%s), Go %s  \n",
		report.Env.CPUModel, report.Env.LogicalCores, report.Env.TotalRAMGB, report.Env.OSCaption, report.Env.Arch, report.Env.GoVersion))
	sb.WriteString(fmt.Sprintf("**Storage Media:** %s | **Power Plan:** %s | **Power Source:** %s  \n",
		report.Env.DiskType, report.Env.PowerPlan, report.Env.ACStatus))
	sb.WriteString(fmt.Sprintf("**Timer Implementation:** %s  \n", report.Env.TimerImplementation))
	sb.WriteString(fmt.Sprintf("**Timer Clock Resolution:** %s (%d ns) | **GOMAXPROCS:** %d  \n\n",
		report.Env.ClockResolution, report.Env.ClockResolutionNs, report.Env.GOMAXPROCS))

	// Loud Warnings
	if report.Env.GitDirty {
		sb.WriteString("> [!WARNING]\n")
		sb.WriteString("> **DIRTY WORKING TREE WARNING:** The repository contains uncommitted working tree changes (`git_dirty=true`). Results must not be used for official releases until committed!\n\n")
	}

	isHighPerf := strings.Contains(strings.ToLower(report.Env.PowerPlan), "high performance") ||
		strings.Contains(strings.ToLower(report.Env.PowerPlan), "ultimate performance") ||
		strings.Contains(strings.ToLower(report.Env.PowerPlan), "best performance")
	if !isHighPerf {
		sb.WriteString("> [!WARNING]\n")
		sb.WriteString(fmt.Sprintf("> **SUB-OPTIMAL POWER PLAN WARNING:** The host system is running under power scheme `%s`. For reproducible full benchmark runs, set the OS power plan to High/Best Performance to avoid CPU throttling.\n\n", report.Env.PowerPlan))
	}

	if report.Env.IsOnBattery {
		sb.WriteString("> [!WARNING]\n")
		sb.WriteString(fmt.Sprintf("> **BATTERY POWER WARNING:** Host is running on battery (%s). Dynamic frequency scaling on battery power can inflate tail latencies.\n\n", report.Env.ACStatus))
	}

	sb.WriteString("> [!NOTE]\n")
	sb.WriteString("> **Hardware & Setup Disclaimer:** All cluster nodes and benchmarks were executed on a single physical host sharing one physical SSD. Absolute throughput and latency are hardware-dependent. Durability OFF (`NoSync`) numbers are provided strictly for isolating fsync cost and are labeled **NOT DURABLE, comparison only**.\n\n")

	// Table B0b
	if report.B0b_Fsync != nil {
		sb.WriteString("## B0b. Raw Disk fsync Baseline (Hardware Ceiling)\n\n")
		sb.WriteString("Direct micro-benchmark issuing small 256-byte sequential appends with immediate `fsync()` per operation.\n\n")
		sb.WriteString("| Metric | Measured Value (Median [Min - Max]) |\n")
		sb.WriteString("| :--- | :--- |\n")
		sb.WriteString(fmt.Sprintf("| **Throughput (fsyncs/sec)** | %s |\n", FormatOpsRange(report.B0b_Fsync.Throughput)))
		sb.WriteString(fmt.Sprintf("| **p50 Latency** | %s |\n", FormatDurationRange(report.B0b_Fsync.LatencyP50)))
		sb.WriteString(fmt.Sprintf("| **p95 Latency** | %s |\n", FormatDurationRange(report.B0b_Fsync.LatencyP95)))
		sb.WriteString(fmt.Sprintf("| **p99 Latency** | %s |\n", FormatDurationRange(report.B0b_Fsync.LatencyP99)))
		sb.WriteString(fmt.Sprintf("| **Max Latency** | %s |\n\n", FormatDurationRange(report.B0b_Fsync.LatencyMax)))
	}

	// Table B1
	if len(report.B1_Storage) > 0 {
		sb.WriteString("## B1. Single-Node Storage Engine (No Raft)\n\n")
		sb.WriteString("Direct `bbolt` storage engine throughput and latency across concurrency levels, testing unbatched vs. group-commit micro-batcher, and engine fsync ON vs. engine fsync OFF.\n\n")
		sb.WriteString("| Configuration | Concurrency | Engine Durability | Throughput (ops/sec) | p50 Latency | p95 Latency | p99 Latency | Max Latency | Little's Law | Sanity Check |\n")
		sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
		for _, b := range report.B1_Storage {
			sanityStatus := "PASS"
			if !b.OverallPass {
				if len(b.Repetitions) > 0 && !b.Repetitions[0].SanityPass {
					sanityStatus = fmt.Sprintf("FAIL (%s)", b.Repetitions[0].SanityNote)
				} else if b.TotalErrors > 0 {
					sanityStatus = fmt.Sprintf("FAIL (%d errors)", b.TotalErrors)
				} else {
					sanityStatus = "FAIL"
				}
			}

			p50Display := FormatDurationRange(b.LatencyP50)
			if b.Unreliable {
				p50Display = fmt.Sprintf("%s *(unreliable, batch mean: %s)*", p50Display, FormatDurationRange(b.BatchMean))
			}

			mode := b.Config["mode"]
			durabilityStr := "fsync ON (Durable)"
			if b.Config["nosync"] == "true" {
				durabilityStr = "NoSync *(NOT DURABLE, comparison only)*"
			}

			littlesLawDisplay := "PASS"
			if !b.LittlesLawConsistent && b.LittlesLawNote != "" {
				littlesLawDisplay = b.LittlesLawNote
			}

			sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
				mode,
				b.Config["concurrency"],
				durabilityStr,
				FormatOpsRange(b.Throughput),
				p50Display,
				FormatDurationRange(b.LatencyP95),
				FormatDurationRange(b.LatencyP99),
				FormatDurationRange(b.LatencyMax),
				littlesLawDisplay,
				sanityStatus,
			))
		}
		sb.WriteString("\n")
	}

	// Table B2
	if len(report.B2_Cluster) > 0 {
		sb.WriteString("## B2. 3-Node Raft Cluster Closed-Loop Enqueue (Strict Durability ON)\n\n")
		sb.WriteString("End-to-end gRPC clients submitting `Enqueue` operations across 3 Raft replicas with fsync ON. Compares leader micro-batcher (2ms/64-op) against unbatched (0ms/1-op).\n\n")
		sb.WriteString("| Mode | Concurrency | Throughput (ops/sec) | Stability (CV) | p50 Latency | p95 Latency | p99 Latency | Max Latency | Errors / Timeouts (Breakdown) | Little's Law | Sanity Check |\n")
		sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
		var unbatchedC1, unbatchedC128 *BenchmarkResult
		for _, b := range report.B2_Cluster {
			sanityStatus := "PASS"
			if !b.OverallPass {
				if len(b.Repetitions) > 0 && !b.Repetitions[0].SanityPass {
					sanityStatus = fmt.Sprintf("FAIL (%s)", b.Repetitions[0].SanityNote)
				} else if b.TotalErrors > 0 || b.TotalTimeouts > 0 {
					sanityStatus = fmt.Sprintf("FAIL (%d err, %d timeout)", b.TotalErrors, b.TotalTimeouts)
				} else {
					sanityStatus = "FAIL"
				}
			}

			p50Display := FormatDurationRange(b.LatencyP50)
			if b.Unreliable {
				p50Display = fmt.Sprintf("%s *(unreliable, batch mean: %s)*", p50Display, FormatDurationRange(b.BatchMean))
			}

			littlesLawDisplay := "PASS"
			if !b.LittlesLawConsistent && b.LittlesLawNote != "" {
				littlesLawDisplay = b.LittlesLawNote
			}

			errTimeoutStr := fmt.Sprintf("%d / %d", b.TotalErrors, b.TotalTimeouts)
			if len(b.ErrorBreakdown) > 0 {
				var bdParts []string
				for code, cnt := range b.ErrorBreakdown {
					bdParts = append(bdParts, fmt.Sprintf("%s: %d", code, cnt))
				}
				sort.Strings(bdParts)
				errTimeoutStr = fmt.Sprintf("%s (%s)", errTimeoutStr, strings.Join(bdParts, ", "))
			}

			stabilityDisplay := b.StabilityStatus
			if b.Unstable {
				stabilityDisplay = fmt.Sprintf("**%s**", b.StabilityStatus)
			}

			sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
				b.Config["mode"],
				b.Config["concurrency"],
				FormatOpsRange(b.Throughput),
				stabilityDisplay,
				p50Display,
				FormatDurationRange(b.LatencyP95),
				FormatDurationRange(b.LatencyP99),
				FormatDurationRange(b.LatencyMax),
				errTimeoutStr,
				littlesLawDisplay,
				sanityStatus,
			))

			if b.Config["mode"] == "Unbatched (0ms/1-op)" {
				if b.Config["concurrency"] == "1" {
					unbatchedC1 = b
				} else if b.Config["concurrency"] == "128" {
					unbatchedC128 = b
				}
			}
		}
		sb.WriteString("\n")

		// Dynamic analysis derived strictly from B2 data
		if unbatchedC1 != nil && unbatchedC128 != nil {
			sb.WriteString(fmt.Sprintf("> **Unbatched Scaling Observation:** Under unbatched operations (0ms/1-op), concurrency scaling from c=1 to c=128 shifted throughput from %.0f to %.0f ops/sec (p50: %s to %s) with %d errors and %d timeouts, reflecting Raft log lock serialization and synchronous disk sync per transaction.\n\n",
				unbatchedC1.Throughput.Median, unbatchedC128.Throughput.Median,
				FormatDurationRange(unbatchedC1.LatencyP50), FormatDurationRange(unbatchedC128.LatencyP50),
				unbatchedC128.TotalErrors, unbatchedC128.TotalTimeouts))
		}
	}

	// Table B3
	if len(report.B3_OpenLoop) > 0 {
		sb.WriteString("## B3. Open-Loop Latency (Eliminating Coordinated Omission)\n\n")
		sb.WriteString("Requests dispatched on a strict fixed timeline at 25%, 50%, and 80% of max measured B2 throughput. Latency is measured from scheduled send time to eliminate coordinated omission.\n\n")
		sb.WriteString("| Offered Target Rate | Achieved Rate (ops/sec) | p50 Latency | p95 Latency | p99 Latency | p99.9 Latency | Max Latency | Saturation Status |\n")
		sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
		var p95Values []time.Duration
		var offeredRates []float64
		var achievedRates []float64
		for _, b := range report.B3_OpenLoop {
			var offered float64
			fmt.Sscanf(b.Config["target_rate"], "%f", &offered)
			offeredRates = append(offeredRates, offered)
			achieved := b.Throughput.Median
			achievedRates = append(achievedRates, achieved)
			p95Values = append(p95Values, b.LatencyP95.Median)

			statusStr := "Nominal"
			if offered > 0 && achieved < 0.95*offered {
				statusStr = fmt.Sprintf("**SATURATED** (achieved %.0f of offered %.0f)", achieved, offered)
			}

			sb.WriteString(fmt.Sprintf("| %s req/s (%s) | %s | %s | %s | %s | %s | %s | %s |\n",
				b.Config["target_rate"],
				b.Config["fraction"],
				FormatOpsRange(b.Throughput),
				FormatDurationRange(b.LatencyP50),
				FormatDurationRange(b.LatencyP95),
				FormatDurationRange(b.LatencyP99),
				FormatDurationRange(b.LatencyP999),
				FormatDurationRange(b.LatencyMax),
				statusStr,
			))
		}
		sb.WriteString("\n")

		// Dynamic statement computed from results only
		monotonicP95 := true
		for i := 1; i < len(p95Values); i++ {
			if p95Values[i] < p95Values[i-1] {
				monotonicP95 = false
				break
			}
		}
		if !monotonicP95 && len(p95Values) >= 3 {
			sb.WriteString(fmt.Sprintf("> **Measured Open-Loop Tail Metrics:** In this run, p95 latencies were %s (25%%), %s (50%%), and %s (80%%); p95 was not monotonic across offered rates.\n\n",
				FormatDuration(p95Values[0]), FormatDuration(p95Values[1]), FormatDuration(p95Values[2])))
		} else if len(p95Values) >= 3 {
			sb.WriteString(fmt.Sprintf("> **Measured Open-Loop Tail Metrics:** p95 latencies scaled monotonically across offered rates (%s -> %s -> %s).\n\n",
				FormatDuration(p95Values[0]), FormatDuration(p95Values[1]), FormatDuration(p95Values[2])))
		}
	}

	// Table B4
	if len(report.B4_E2E) > 0 {
		sb.WriteString("## B4. End-to-End Pipeline Flow (Enqueue -> Reserve -> Ack)\n\n")
		sb.WriteString("Concurrent producers enqueuing jobs paired with concurrent workers reserving and acknowledging leases across worker pool sizes and producer pacing modes.\n\n")
		sb.WriteString("| Workload Mode | Producers | Workers | Offered Target | Achieved Completion | p50 Latency | p95 Latency | p99 Latency | Max Latency | End Backlog | Validity / Status |\n")
		sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
		for _, b := range report.B4_E2E {
			mode := b.Config["mode"]
			offeredStr := "Unthrottled"
			if o, ok := b.Config["offered_rate"]; ok && o != "" {
				offeredStr = fmt.Sprintf("%s jobs/s", o)
			}
			achievedStr := FormatOpsRange(b.Throughput)
			p50Str := FormatDurationRange(b.LatencyP50)
			p95Str := FormatDurationRange(b.LatencyP95)
			p99Str := FormatDurationRange(b.LatencyP99)
			maxStr := FormatDurationRange(b.LatencyMax)
			backlogStr := fmt.Sprintf("%d", b.Backlog)
			statusStr := "PASS"

			if !b.Valid {
				if b.StabilityStatus == "SKIPPED" {
					achievedStr = "-"
					p50Str = "-"
					p95Str = "-"
					p99Str = "-"
					maxStr = "-"
					statusStr = fmt.Sprintf("SKIPPED (%s)", b.InvalidReason)
				} else {
					p50Str = "INVALID"
					p95Str = "INVALID"
					p99Str = "INVALID"
					maxStr = "INVALID"
					statusStr = fmt.Sprintf("INVALID (%s)", b.InvalidReason)
				}
			}

			sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
				mode,
				b.Config["producers"],
				b.Config["workers"],
				offeredStr,
				achievedStr,
				p50Str,
				p95Str,
				p99Str,
				maxStr,
				backlogStr,
				statusStr,
			))
		}
		sb.WriteString("\n")

		// Dynamic statement computed from results only
		for _, b := range report.B4_E2E {
			if b.Config["mode"] == "Rate-Limited (50% Capacity)" {
				if !b.Valid {
					sb.WriteString(fmt.Sprintf("> **Rate-Limited Pipeline Check (Workers=%s):** %s\n\n",
						b.Config["workers"], b.InvalidReason))
					continue
				}
				var offered float64
				fmt.Sscanf(b.Config["offered_rate"], "%f", &offered)
				achieved := b.Throughput.Median
				flagStr := ""
				if offered > 0 && achieved < 0.80*offered {
					flagStr = fmt.Sprintf(" **[FLAGGED: achieved %.0f jobs/sec far below offered %.0f jobs/sec]**", achieved, offered)
				}
				sb.WriteString(fmt.Sprintf("> **Rate-Limited Pipeline Check (Workers=%s):** Offered: %.0f jobs/sec, Achieved: %s jobs/sec, Backlog: %d jobs.%s\n\n",
					b.Config["workers"],
					offered,
					FormatOpsRange(b.Throughput),
					b.Backlog,
					flagStr,
				))
			}
		}
	}

	// Table B5
	if len(report.B5_Failover) > 0 {
		sb.WriteString("## B5. Leader Failover Latency (Real relayd Subprocesses)\n\n")
		sb.WriteString("3 independent `relayd` child processes under steady write traffic. Old leader is killed via `Process.Kill`; failover duration is measured from kill time until the first successful write through a surviving node.\n\n")
		for _, b5 := range report.B5_Failover {
			p95Str := FormatDuration(b5.P95Duration)
			if !b5.P95Valid {
				p95Str = "n too small"
			}
			headlineNote := ""
			if b5.IsPrimaryHeadline {
				headlineNote = " **(Primary Headline)**"
			} else {
				headlineNote = " *(Secondary Sweep, Non-Default)*"
			}
			sb.WriteString(fmt.Sprintf("### %s%s\n\n", b5.Name, headlineNote))
			sb.WriteString(fmt.Sprintf("**Raft Flags:** `%s` | **Total Trials:** %d | **Valid Trials:** %d | **Verified Data Loss:** %d jobs  \n",
				b5.Flags, b5.TrialsCount, b5.ValidTrialsCount, b5.TotalLostJobs))
			if b5.LowerBoundDerivation != "" {
				sb.WriteString(fmt.Sprintf("**Expected Lower Bound Derivation:** %s  \n\n", b5.LowerBoundDerivation))
			} else {
				sb.WriteString("\n")
			}
			sb.WriteString("| Metric | Measured Duration | Expected Lower Bound | Lower Bound Status |\n")
			sb.WriteString("| :--- | :--- | :--- | :--- |\n")

			boundStatus := "PASS (>= lower bound)"
			if b5.Suspicious {
				boundStatus = "**SUSPICIOUS** (< expected lower bound)"
			}

			sb.WriteString(fmt.Sprintf("| **Min Failover Time** | %s | %s | %s |\n",
				FormatDuration(b5.MinDuration), FormatDuration(b5.ExpectedLowerBound), boundStatus))
			sb.WriteString(fmt.Sprintf("| **Median Failover Time** | %s | %s | %s |\n",
				FormatDuration(b5.MedianDuration), FormatDuration(b5.ExpectedLowerBound), boundStatus))
			sb.WriteString(fmt.Sprintf("| **p95 Failover Time** | %s | %s | %s |\n",
				p95Str, FormatDuration(b5.ExpectedLowerBound), boundStatus))
			sb.WriteString(fmt.Sprintf("| **Max Failover Time** | %s | %s | %s |\n\n",
				FormatDuration(b5.MaxDuration), FormatDuration(b5.ExpectedLowerBound), boundStatus))

			if len(b5.Trials) > 0 {
				sb.WriteString("#### Valid Trials (Included in Percentiles)\n\n")
				sb.WriteString("| Trial | Killed Leader | New Leader | Failover Time | Pre-Ack Jobs | Lost Jobs | Status |\n")
				sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
				for _, t := range b5.Trials {
					sb.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %d | %d | PASS |\n",
						t.Trial, t.KilledLeader, t.NewLeader, FormatDuration(t.FailoverDuration), t.PreAckCount, t.VerifiedLostJobs))
				}
				sb.WriteString("\n")
			}

			if len(b5.InvalidTrials) > 0 {
				sb.WriteString("#### Invalid Trials (Excluded from Percentiles)\n\n")
				sb.WriteString("| Trial | Killed Leader | New Leader | Duration | Invalidation Reason | Status |\n")
				sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- |\n")
				for _, t := range b5.InvalidTrials {
					sb.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %s | INVALID |\n",
						t.Trial, t.KilledLeader, t.NewLeader, FormatDuration(t.FailoverDuration), t.InvalidReason))
				}
				sb.WriteString("\n")
			}
		}
	}

	return sb.String()
}
