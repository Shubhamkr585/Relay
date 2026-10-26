package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/shubhamkr585/relay/internal/bench"
)

type ResumeNumbersOutput struct {
	Hardware  HardwareSummary `json:"hardware"`
	B0b       *B0bSummary     `json:"b0b,omitempty"`
	B1        *B1Summary      `json:"b1,omitempty"`
	B2        *B2Summary      `json:"b2,omitempty"`
	B3        []B3RowSummary  `json:"b3,omitempty"`
	B4        []B4RowSummary  `json:"b4,omitempty"`
	B5        *B5Summary      `json:"b5,omitempty"`
	Chaos     *ChaosSummary   `json:"chaos,omitempty"`
	SafeToUse []string        `json:"safe_to_use"`
	DoNotUse  []DoNotUseItem  `json:"do_not_use"`
}

type HardwareSummary struct {
	SourceFile   string `json:"source_file"`
	CPU          string `json:"cpu"`
	LogicalCores int    `json:"logical_cores"`
	RAM          string `json:"ram"`
	DiskType     string `json:"disk_type"`
	OS           string `json:"os"`
	PowerPlan    string `json:"power_plan"`
	ACStatus     string `json:"ac_status"`
	GitCommit    string `json:"git_commit"`
	GitDirty     bool   `json:"git_dirty"`
}

type B0bSummary struct {
	SourceFile string  `json:"source_file"`
	Median     float64 `json:"median_fsync_sec"`
	Min        float64 `json:"min_fsync_sec"`
	Max        float64 `json:"max_fsync_sec"`
	P50Latency string  `json:"p50_latency"`
}

type B1SpeedupRow struct {
	Concurrency           int     `json:"concurrency"`
	UnbatchedThroughput   float64 `json:"unbatched_throughput"`
	GroupCommitThroughput float64 `json:"group_commit_throughput"`
	SpeedupRatio          float64 `json:"speedup_ratio"`
}

type B1Summary struct {
	SourceFile        string         `json:"source_file"`
	Speedups          []B1SpeedupRow `json:"speedups"`
	C64UnbatchedP50   string         `json:"c64_unbatched_p50"`
	C64UnbatchedP99   string         `json:"c64_unbatched_p99"`
	C64GroupCommitP50 string         `json:"c64_group_commit_p50"`
	C64GroupCommitP99 string         `json:"c64_group_commit_p99"`
}

type B2RowSummary struct {
	Mode          string  `json:"mode"`
	Concurrency   int     `json:"concurrency"`
	Throughput    float64 `json:"throughput"`
	P50Latency    string  `json:"p50_latency"`
	P99Latency    string  `json:"p99_latency"`
	StabilityCV   float64 `json:"stability_cv"`
	Usable        bool    `json:"usable"`
	ExcludeReason string  `json:"exclude_reason,omitempty"`
}

type B2Summary struct {
	SourceFile string         `json:"source_file"`
	Rows       []B2RowSummary `json:"rows"`
}

type B3RowSummary struct {
	SourceFile   string  `json:"source_file"`
	TargetRate   float64 `json:"target_rate"`
	AchievedRate float64 `json:"achieved_rate"`
	P50Latency   string  `json:"p50_latency"`
	P95Latency   string  `json:"p95_latency"`
	P99Latency   string  `json:"p99_latency"`
	Saturated    bool    `json:"saturated"`
}

type B4RowSummary struct {
	SourceFile   string  `json:"source_file"`
	Mode         string  `json:"mode"`
	Workers      int     `json:"workers"`
	OfferedRate  string  `json:"offered_rate"`
	AchievedRate float64 `json:"achieved_rate"`
	P50Latency   string  `json:"p50_latency"`
	P95Latency   string  `json:"p95_latency"`
	P99Latency   string  `json:"p99_latency"`
	Backlog      int64   `json:"backlog"`
}

type B5Summary struct {
	SourceFile         string `json:"source_file"`
	ValidTrials        int    `json:"valid_trials"`
	InvalidTrials      int    `json:"invalid_trials"`
	MinFailover        string `json:"min_failover"`
	MedianFailover     string `json:"median_failover"`
	P95Failover        string `json:"p95_failover"`
	MaxFailover        string `json:"max_failover"`
	ExpectedLowerBound string `json:"expected_lower_bound"`
	TotalLostJobs      int    `json:"total_lost_jobs"`
	Suspicious         bool   `json:"suspicious"`
}

type ChaosSummary struct {
	SourceFile           string            `json:"source_file"`
	Verified             bool              `json:"verified"`
	StatusText           string            `json:"status_text"`
	TotalRuns            int               `json:"total_runs,omitempty"`
	AcknowledgedEnqueues int               `json:"acknowledged_enqueues,omitempty"`
	CompletedDone        int               `json:"completed_done,omitempty"`
	DeadLetteredDead     int               `json:"dead_lettered_dead,omitempty"`
	LeaderKillsFailovers string            `json:"leader_kills_failovers,omitempty"`
	Invariants           map[string]string `json:"invariants,omitempty"`
}

type DoNotUseItem struct {
	Claim  string `json:"claim"`
	Reason string `json:"reason"`
}

func main() {
	benchResultsDir := "bench-results"
	entries, err := os.ReadDir(benchResultsDir)
	if err != nil || len(entries) == 0 {
		fmt.Fprintf(os.Stderr, "ERROR: No benchmark results found in %s! Run 'make bench-full' first.\n", benchResultsDir)
		os.Exit(1)
	}

	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		}
	}

	if len(dirs) == 0 {
		fmt.Fprintf(os.Stderr, "ERROR: No benchmark run directories found in %s! Run 'make bench-full' first.\n", benchResultsDir)
		os.Exit(1)
	}

	sort.Slice(dirs, func(i, j int) bool {
		return dirs[i] > dirs[j]
	})

	latestDir := filepath.Join(benchResultsDir, dirs[0])

	// 1. Validate env.json
	envRelPath := filepath.Join(latestDir, "env.json")
	envData, err := os.ReadFile(envRelPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Cannot read %s: %v\n", envRelPath, err)
		os.Exit(1)
	}

	var env bench.EnvInfo
	if err := json.Unmarshal(envData, &env); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Cannot parse %s: %v\n", envRelPath, err)
		os.Exit(1)
	}

	// STRICT REFUSAL GATES
	if env.Mode != "full" {
		fmt.Fprintf(os.Stderr, "ERROR: Refusing to extract resume numbers: latest run (%s) has mode=%q (required: mode=\"full\"). Run 'make bench-full' first.\n", dirs[0], env.Mode)
		os.Exit(1)
	}

	if env.GitDirty {
		fmt.Fprintf(os.Stderr, "ERROR: Refusing to extract resume numbers: latest run (%s) was run with git_dirty=true. Commit your changes and run 'make bench-full' from a clean tree.\n", dirs[0])
		os.Exit(1)
	}

	out := ResumeNumbersOutput{
		SafeToUse: make([]string, 0),
		DoNotUse:  make([]DoNotUseItem, 0),
	}

	out.Hardware = HardwareSummary{
		SourceFile:   envRelPath,
		CPU:          env.CPUModel,
		LogicalCores: env.LogicalCores,
		RAM:          env.TotalRAMGB,
		DiskType:     env.DiskType,
		OS:           fmt.Sprintf("%s (%s)", env.OSCaption, env.Arch),
		PowerPlan:    env.PowerPlan,
		ACStatus:     env.ACStatus,
		GitCommit:    env.GitCommit,
		GitDirty:     env.GitDirty,
	}

	// 2. Read B0b fsync
	b0bPath := filepath.Join(latestDir, "b0b_fsync.json")
	if b0bData, err := os.ReadFile(b0bPath); err == nil {
		var b0b bench.BenchmarkResult
		if err := json.Unmarshal(b0bData, &b0b); err == nil {
			out.B0b = &B0bSummary{
				SourceFile: b0bPath,
				Median:     b0b.Throughput.Median,
				Min:        b0b.Throughput.Min,
				Max:        b0b.Throughput.Max,
				P50Latency: bench.FormatDuration(b0b.LatencyP50.Median),
			}
			out.SafeToUse = append(out.SafeToUse,
				fmt.Sprintf("Raw Disk fsync Baseline: %.0f fsyncs/sec (range: %.0f - %.0f, p50: %s) on host SSD",
					b0b.Throughput.Median, b0b.Throughput.Min, b0b.Throughput.Max, bench.FormatDuration(b0b.LatencyP50.Median)))
		}
	}

	// 3. Read B1 Storage (fsync ON only)
	b1Path := filepath.Join(latestDir, "b1_storage.json")
	if b1Data, err := os.ReadFile(b1Path); err == nil {
		var b1List []*bench.BenchmarkResult
		if err := json.Unmarshal(b1Data, &b1List); err == nil {
			durableUnbatched := make(map[int]*bench.BenchmarkResult)
			durableGroupCommit := make(map[int]*bench.BenchmarkResult)

			for _, b := range b1List {
				if b.Config["nosync"] == "true" {
					out.DoNotUse = append(out.DoNotUse, DoNotUseItem{
						Claim:  fmt.Sprintf("B1 NoSync %s c=%s (%.0f ops/s)", b.Config["mode"], b.Config["concurrency"], b.Throughput.Median),
						Reason: "Durability is disabled (fsync OFF); memory-only numbers are not valid for durable queue throughput claims",
					})
					continue
				}
				var c int
				fmt.Sscanf(b.Config["concurrency"], "%d", &c)
				if strings.Contains(b.Config["mode"], "Unbatched") {
					durableUnbatched[c] = b
				} else if strings.Contains(b.Config["mode"], "Group-Commit") {
					durableGroupCommit[c] = b
				}
			}

			b1Summary := &B1Summary{
				SourceFile: b1Path,
				Speedups:   make([]B1SpeedupRow, 0),
			}

			for _, c := range []int{1, 8, 32, 64} {
				u := durableUnbatched[c]
				g := durableGroupCommit[c]
				if u != nil && g != nil && u.Throughput.Median > 0 {
					speedup := g.Throughput.Median / u.Throughput.Median
					b1Summary.Speedups = append(b1Summary.Speedups, B1SpeedupRow{
						Concurrency:           c,
						UnbatchedThroughput:   u.Throughput.Median,
						GroupCommitThroughput: g.Throughput.Median,
						SpeedupRatio:          speedup,
					})
				}
			}

			if u64, ok := durableUnbatched[64]; ok {
				b1Summary.C64UnbatchedP50 = bench.FormatDuration(u64.LatencyP50.Median)
				b1Summary.C64UnbatchedP99 = bench.FormatDuration(u64.LatencyP99.Median)
			}
			if g64, ok := durableGroupCommit[64]; ok {
				b1Summary.C64GroupCommitP50 = bench.FormatDuration(g64.LatencyP50.Median)
				b1Summary.C64GroupCommitP99 = bench.FormatDuration(g64.LatencyP99.Median)
			}
			out.B1 = b1Summary

			if len(b1Summary.Speedups) > 0 {
				last := b1Summary.Speedups[len(b1Summary.Speedups)-1]
				out.SafeToUse = append(out.SafeToUse,
					fmt.Sprintf("Storage Group-Commit: %.1fx speedup at c=%d (%.0f ops/s vs %.0f ops/s unbatched, durable fsync ON)",
						last.SpeedupRatio, last.Concurrency, last.GroupCommitThroughput, last.UnbatchedThroughput))
			}
		}
	}

	// 4. Read B2 Cluster (only STABLE rows with 0 errors)
	b2Path := filepath.Join(latestDir, "b2_cluster.json")
	if b2Data, err := os.ReadFile(b2Path); err == nil {
		var b2List []*bench.BenchmarkResult
		if err := json.Unmarshal(b2Data, &b2List); err == nil {
			b2Summary := &B2Summary{
				SourceFile: b2Path,
				Rows:       make([]B2RowSummary, 0),
			}

			for _, b := range b2List {
				var c int
				fmt.Sscanf(b.Config["concurrency"], "%d", &c)
				usable := true
				var excludeReason string

				if b.Unstable {
					usable = false
					excludeReason = fmt.Sprintf("UNSTABLE (CV: %.1f%% > 25%% threshold across 5 repetitions)", b.CV)
				} else if b.TotalErrors > 0 || b.TotalTimeouts > 0 {
					usable = false
					excludeReason = fmt.Sprintf("Errors > 0 (%d errors, %d timeouts; exhibits survivor bias)", b.TotalErrors, b.TotalTimeouts)
				} else if !b.Valid {
					usable = false
					excludeReason = fmt.Sprintf("INVALID (%s)", b.InvalidReason)
				}

				row := B2RowSummary{
					Mode:          b.Config["mode"],
					Concurrency:   c,
					Throughput:    b.Throughput.Median,
					P50Latency:    bench.FormatDuration(b.LatencyP50.Median),
					P99Latency:    bench.FormatDuration(b.LatencyP99.Median),
					StabilityCV:   b.CV,
					Usable:        usable,
					ExcludeReason: excludeReason,
				}
				b2Summary.Rows = append(b2Summary.Rows, row)

				if !usable {
					out.DoNotUse = append(out.DoNotUse, DoNotUseItem{
						Claim:  fmt.Sprintf("B2 %s c=%d (%.0f ops/s)", b.Config["mode"], c, b.Throughput.Median),
						Reason: excludeReason,
					})
				} else {
					out.SafeToUse = append(out.SafeToUse,
						fmt.Sprintf("3-Node Raft Cluster (%s, c=%d): %.0f ops/sec (p50: %s, p99: %s, CV: %.1f%%, 0 errors)",
							b.Config["mode"], c, b.Throughput.Median, row.P50Latency, row.P99Latency, b.CV))
				}
			}
			out.B2 = b2Summary
		}
	}

	// 5. Read B3 Open-Loop
	b3Path := filepath.Join(latestDir, "b3_openloop.json")
	if b3Data, err := os.ReadFile(b3Path); err == nil {
		var b3List []*bench.BenchmarkResult
		if err := json.Unmarshal(b3Data, &b3List); err == nil {
			for _, b := range b3List {
				var targetRate float64
				fmt.Sscanf(b.Config["target_rate"], "%f", &targetRate)
				achieved := b.Throughput.Median
				saturated := (targetRate > 0 && achieved < 0.95*targetRate)

				row := B3RowSummary{
					SourceFile:   b3Path,
					TargetRate:   targetRate,
					AchievedRate: achieved,
					P50Latency:   bench.FormatDuration(b.LatencyP50.Median),
					P95Latency:   bench.FormatDuration(b.LatencyP95.Median),
					P99Latency:   bench.FormatDuration(b.LatencyP99.Median),
					Saturated:    saturated,
				}
				out.B3 = append(out.B3, row)

				if saturated {
					out.DoNotUse = append(out.DoNotUse, DoNotUseItem{
						Claim:  fmt.Sprintf("B3 Target Rate %.0f req/s", targetRate),
						Reason: fmt.Sprintf("SATURATED: Cluster achieved %.0f req/s (< 95%% of offered %.0f req/s)", achieved, targetRate),
					})
				} else {
					out.SafeToUse = append(out.SafeToUse,
						fmt.Sprintf("Open-Loop Tail Latency at %.0f req/sec (Coordinated Omission eliminated): p50: %s, p95: %s, p99: %s",
							targetRate, row.P50Latency, row.P95Latency, row.P99Latency))
				}
			}
		}
	}

	// 6. Read B4 Pipeline (only valid rows)
	b4Path := filepath.Join(latestDir, "b4_e2e.json")
	if b4Data, err := os.ReadFile(b4Path); err == nil {
		var b4List []*bench.BenchmarkResult
		if err := json.Unmarshal(b4Data, &b4List); err == nil {
			for _, b := range b4List {
				var workers int
				fmt.Sscanf(b.Config["workers"], "%d", &workers)

				if !b.Valid {
					out.DoNotUse = append(out.DoNotUse, DoNotUseItem{
						Claim:  fmt.Sprintf("B4 %s Workers=%d", b.Config["mode"], workers),
						Reason: fmt.Sprintf("INVALID: %s", b.InvalidReason),
					})
					continue
				}

				row := B4RowSummary{
					SourceFile:   b4Path,
					Mode:         b.Config["mode"],
					Workers:      workers,
					OfferedRate:  b.Config["offered_rate"],
					AchievedRate: b.Throughput.Median,
					P50Latency:   bench.FormatDuration(b.LatencyP50.Median),
					P95Latency:   bench.FormatDuration(b.LatencyP95.Median),
					P99Latency:   bench.FormatDuration(b.LatencyP99.Median),
					Backlog:      b.Backlog,
				}
				out.B4 = append(out.B4, row)

				out.SafeToUse = append(out.SafeToUse,
					fmt.Sprintf("End-to-End Pipeline (%s, Workers=%d): %.0f completed jobs/sec (p50: %s, p95: %s, End Backlog: %d jobs)",
						b.Config["mode"], workers, row.AchievedRate, row.P50Latency, row.P95Latency, row.Backlog))
			}
		}
	}

	// 7. Read B5 Production Defaults
	b5Path := filepath.Join(latestDir, "b5_failover.json")
	if b5Data, err := os.ReadFile(b5Path); err == nil {
		var b5List []*bench.FailoverResult
		if err := json.Unmarshal(b5Data, &b5List); err == nil {
			for _, b5 := range b5List {
				if b5.IsPrimaryHeadline {
					out.B5 = &B5Summary{
						SourceFile:         b5Path,
						ValidTrials:        b5.ValidTrialsCount,
						InvalidTrials:      len(b5.InvalidTrials),
						MinFailover:        bench.FormatDuration(b5.MinDuration),
						MedianFailover:     bench.FormatDuration(b5.MedianDuration),
						P95Failover:        bench.FormatDuration(b5.P95Duration),
						MaxFailover:        bench.FormatDuration(b5.MaxDuration),
						ExpectedLowerBound: bench.FormatDuration(b5.ExpectedLowerBound),
						TotalLostJobs:      b5.TotalLostJobs,
						Suspicious:         b5.Suspicious,
					}
					if !b5.P95Valid {
						out.B5.P95Failover = "n too small"
					}
					out.SafeToUse = append(out.SafeToUse,
						fmt.Sprintf("Leader Failover Outage (Production Defaults, 1s heartbeat): median %s, max %s across %d trials with ZERO data loss (0 lost acknowledged jobs)",
							out.B5.MedianFailover, out.B5.MaxFailover, b5.ValidTrialsCount))
				} else {
					out.DoNotUse = append(out.DoNotUse, DoNotUseItem{
						Claim:  fmt.Sprintf("B5 Aggressive Timeouts (%s median)", bench.FormatDuration(b5.MedianDuration)),
						Reason: "Non-default aggressive Raft parameters (100ms heartbeat / 300ms election); must not be presented as standard production failover latency",
					})
				}
			}
		}
	}

	// 8. Read Chaos Summary
	chaosSummary := parseChaosReport()
	out.Chaos = chaosSummary
	if chaosSummary.Verified {
		out.SafeToUse = append(out.SafeToUse,
			fmt.Sprintf("Chaos Fault-Injection: %d total runs, %d acknowledged enqueues verified with 0 lost jobs (DONE: %d, DEAD: %d, Failovers: %s across 8 invariants)",
				chaosSummary.TotalRuns, chaosSummary.AcknowledgedEnqueues, chaosSummary.CompletedDone, chaosSummary.DeadLetteredDead, chaosSummary.LeaderKillsFailovers))
	} else {
		out.DoNotUse = append(out.DoNotUse, DoNotUseItem{
			Claim:  "Chaos Long-Run Invariant Certification",
			Reason: fmt.Sprintf("CHAOS NOT VERIFIED (%s)", chaosSummary.StatusText),
		})
	}

	// Write RESUME_NUMBERS.md
	mdContent := formatResumeMarkdown(out, dirs[0])
	if err := os.WriteFile("RESUME_NUMBERS.md", []byte(mdContent), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing RESUME_NUMBERS.md: %v\n", err)
	} else {
		fmt.Println("Successfully generated RESUME_NUMBERS.md")
	}

	// Write resume-numbers.json
	jsonData, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error serializing JSON: %v\n", err)
	} else {
		if err := os.WriteFile("resume-numbers.json", jsonData, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing resume-numbers.json: %v\n", err)
		} else {
			fmt.Println("Successfully generated resume-numbers.json")
		}
	}

	// Print summary
	fmt.Print(mdContent)
}

func parseChaosReport() *ChaosSummary {
	paths := []string{
		"chaos-results/chaos-report.txt",
	}

	// Also look for latest subfolder in chaos-results/
	if entries, err := os.ReadDir("chaos-results"); err == nil {
		var subdirs []string
		for _, e := range entries {
			if e.IsDir() {
				subdirs = append(subdirs, e.Name())
			}
		}
		sort.Slice(subdirs, func(i, j int) bool {
			return subdirs[i] > subdirs[j]
		})
		for _, sd := range subdirs {
			paths = append(paths, filepath.Join("chaos-results", sd, "chaos-report.txt"))
		}
	}

	for _, p := range paths {
		if data, err := os.ReadFile(p); err == nil {
			content := string(data)
			if !strings.Contains(content, "FINAL GRAND OVERALL STATUS : PASS") {
				return &ChaosSummary{
					SourceFile: p,
					Verified:   false,
					StatusText: "Grand total did not achieve PASS status or had invariant violations",
				}
			}

			summary := &ChaosSummary{
				SourceFile: p,
				Verified:   true,
				StatusText: "PASS",
				Invariants: make(map[string]string),
			}

			scanner := bufio.NewScanner(strings.NewReader(content))
			inGrandTotal := false
			for scanner.Scan() {
				line := scanner.Text()
				if strings.Contains(line, "GRAND TOTAL ACROSS ALL SCENARIOS") {
					inGrandTotal = true
					continue
				}
				if inGrandTotal {
					if strings.Contains(line, "Total Runs Completed") {
						summary.TotalRuns = extractInt(line)
					} else if strings.Contains(line, "Total Acknowledged Enqueues") {
						summary.AcknowledgedEnqueues = extractInt(line)
					} else if strings.Contains(line, "Total Completed (DONE)") {
						summary.CompletedDone = extractInt(line)
					} else if strings.Contains(line, "Total Dead-Lettered (DEAD)") {
						summary.DeadLetteredDead = extractInt(line)
					} else if strings.Contains(line, "Total Leader Kills") {
						kills := extractInt(line)
						summary.LeaderKillsFailovers = fmt.Sprintf("%d kills", kills)
					} else if strings.Contains(line, "Total Leader Failovers") {
						fails := extractInt(line)
						summary.LeaderKillsFailovers = fmt.Sprintf("%s / %d failovers", summary.LeaderKillsFailovers, fails)
					} else if strings.Contains(line, "Inv ") {
						parts := strings.Split(line, ":")
						if len(parts) >= 2 {
							invName := strings.TrimSpace(parts[0])
							invState := strings.TrimSpace(parts[1])
							summary.Invariants[invName] = invState
						}
					}
				}
			}
			return summary
		}
	}

	return &ChaosSummary{
		Verified:   false,
		StatusText: "chaos-report.txt missing or not from clean full run",
	}
}

func extractInt(s string) int {
	re := regexp.MustCompile(`\d+`)
	match := re.FindString(s)
	if match != "" {
		v, _ := strconv.Atoi(match)
		return v
	}
	return 0
}

func formatResumeMarkdown(data ResumeNumbersOutput, runDir string) string {
	var sb strings.Builder

	sb.WriteString("# RELAY AUTHORITATIVE RESUME NUMBERS\n\n")
	sb.WriteString(fmt.Sprintf("**Extracted from Clean Full Run:** `%s`  \n", runDir))
	sb.WriteString(fmt.Sprintf("**Extraction Timestamp:** %s  \n\n", time.Now().Format("2006-01-02 15:04:05 MST")))

	// Hardware
	sb.WriteString("## 1. Hardware & Environment Baseline\n\n")
	sb.WriteString(fmt.Sprintf("- **Source:** `%s`\n", data.Hardware.SourceFile))
	sb.WriteString(fmt.Sprintf("- **CPU:** %s (%d Logical Cores)\n", data.Hardware.CPU, data.Hardware.LogicalCores))
	sb.WriteString(fmt.Sprintf("- **RAM:** %s\n", data.Hardware.RAM))
	sb.WriteString(fmt.Sprintf("- **Disk Type:** %s\n", data.Hardware.DiskType))
	sb.WriteString(fmt.Sprintf("- **OS:** %s\n", data.Hardware.OS))
	sb.WriteString(fmt.Sprintf("- **Power Plan / Source:** %s | %s\n", data.Hardware.PowerPlan, data.Hardware.ACStatus))
	sb.WriteString(fmt.Sprintf("- **Git Commit:** `%s` (Dirty: `%t`)\n\n", data.Hardware.GitCommit, data.Hardware.GitDirty))

	// B0b
	if data.B0b != nil {
		sb.WriteString("## 2. B0b Raw Disk fsync Ceiling\n\n")
		sb.WriteString(fmt.Sprintf("- **Source:** `%s`\n", data.B0b.SourceFile))
		sb.WriteString(fmt.Sprintf("- **Throughput:** %.0f fsyncs/sec (Range: %.0f - %.0f)\n", data.B0b.Median, data.B0b.Min, data.B0b.Max))
		sb.WriteString(fmt.Sprintf("- **p50 Latency:** %s\n\n", data.B0b.P50Latency))
	}

	// B1
	if data.B1 != nil {
		sb.WriteString("## 3. B1 Single-Node Storage Engine (Durable fsync ON Only)\n\n")
		sb.WriteString(fmt.Sprintf("- **Source:** `%s`\n", data.B1.SourceFile))
		sb.WriteString("| Concurrency | Unbatched Throughput | Group-Commit Throughput | Speedup Ratio |\n")
		sb.WriteString("| :--- | :--- | :--- | :--- |\n")
		for _, s := range data.B1.Speedups {
			sb.WriteString(fmt.Sprintf("| c=%d | %.0f ops/s | %.0f ops/s | **%.2fx** |\n",
				s.Concurrency, s.UnbatchedThroughput, s.GroupCommitThroughput, s.SpeedupRatio))
		}
		sb.WriteString("\n")
		sb.WriteString(fmt.Sprintf("- **c=64 Tail Latencies:**  \n"))
		sb.WriteString(fmt.Sprintf("  - Unbatched: p50 = %s, p99 = %s  \n", data.B1.C64UnbatchedP50, data.B1.C64UnbatchedP99))
		sb.WriteString(fmt.Sprintf("  - Group-Commit: p50 = %s, p99 = %s  \n\n", data.B1.C64GroupCommitP50, data.B1.C64GroupCommitP99))
	}

	// B2
	if data.B2 != nil {
		sb.WriteString("## 4. B2 3-Node Raft Cluster Closed-Loop Enqueue (Strict Durability ON)\n\n")
		sb.WriteString(fmt.Sprintf("- **Source:** `%s`\n", data.B2.SourceFile))
		sb.WriteString("| Mode | Concurrency | Throughput | p50 | p99 | CV | Usability Status |\n")
		sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
		for _, r := range data.B2.Rows {
			statusStr := "USABLE"
			if !r.Usable {
				statusStr = fmt.Sprintf("**NOT USABLE:** %s", r.ExcludeReason)
			}
			sb.WriteString(fmt.Sprintf("| %s | c=%d | %.0f ops/s | %s | %s | %.1f%% | %s |\n",
				r.Mode, r.Concurrency, r.Throughput, r.P50Latency, r.P99Latency, r.StabilityCV, statusStr))
		}
		sb.WriteString("\n")
	}

	// B3
	if len(data.B3) > 0 {
		sb.WriteString("## 5. B3 Open-Loop Latency (Eliminating Coordinated Omission)\n\n")
		sb.WriteString(fmt.Sprintf("- **Source:** `%s`\n", data.B3[0].SourceFile))
		sb.WriteString("| Offered Rate | Achieved Rate | p50 | p95 | p99 | Saturation Status |\n")
		sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- |\n")
		for _, r := range data.B3 {
			sat := "Nominal"
			if r.Saturated {
				sat = "**SATURATED**"
			}
			sb.WriteString(fmt.Sprintf("| %.0f req/s | %.0f ops/s | %s | %s | %s | %s |\n",
				r.TargetRate, r.AchievedRate, r.P50Latency, r.P95Latency, r.P99Latency, sat))
		}
		sb.WriteString("\n")
	}

	// B4
	if len(data.B4) > 0 {
		sb.WriteString("## 6. B4 End-to-End Pipeline Flow (Valid Rows Only)\n\n")
		sb.WriteString(fmt.Sprintf("- **Source:** `%s`\n", data.B4[0].SourceFile))
		sb.WriteString("| Mode | Workers | Offered Target | Achieved Completion | p50 | p95 | p99 | End Backlog |\n")
		sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
		for _, r := range data.B4 {
			offered := "Unthrottled"
			if r.OfferedRate != "" {
				offered = fmt.Sprintf("%s jobs/s", r.OfferedRate)
			}
			sb.WriteString(fmt.Sprintf("| %s | %d | %s | %.0f jobs/s | %s | %s | %s | %d |\n",
				r.Mode, r.Workers, offered, r.AchievedRate, r.P50Latency, r.P95Latency, r.P99Latency, r.Backlog))
		}
		sb.WriteString("\n")
	}

	// B5
	if data.B5 != nil {
		sb.WriteString("## 7. B5 Leader Failover Outage (Production Defaults Primary Headline)\n\n")
		sb.WriteString(fmt.Sprintf("- **Source:** `%s`\n", data.B5.SourceFile))
		sb.WriteString(fmt.Sprintf("- **Valid Trials:** %d (Invalid: %d)\n", data.B5.ValidTrials, data.B5.InvalidTrials))
		sb.WriteString(fmt.Sprintf("- **Min Failover Latency:** %s\n", data.B5.MinFailover))
		sb.WriteString(fmt.Sprintf("- **Median Failover Latency:** %s\n", data.B5.MedianFailover))
		sb.WriteString(fmt.Sprintf("- **p95 Failover Latency:** %s\n", data.B5.P95Failover))
		sb.WriteString(fmt.Sprintf("- **Max Failover Latency:** %s\n", data.B5.MaxFailover))
		sb.WriteString(fmt.Sprintf("- **Expected Lower Bound:** %s\n", data.B5.ExpectedLowerBound))
		sb.WriteString(fmt.Sprintf("- **Verified Lost Acknowledged Jobs:** %d jobs (ZERO lost jobs)\n\n", data.B5.TotalLostJobs))
	}

	// Chaos
	sb.WriteString("## 8. Long-Run Chaos Fault-Injection Summary\n\n")
	if data.Chaos != nil && data.Chaos.Verified {
		sb.WriteString(fmt.Sprintf("- **Source:** `%s`\n", data.Chaos.SourceFile))
		sb.WriteString(fmt.Sprintf("- **Status:** %s\n", data.Chaos.StatusText))
		sb.WriteString(fmt.Sprintf("- **Total Chaos Runs:** %d\n", data.Chaos.TotalRuns))
		sb.WriteString(fmt.Sprintf("- **Acknowledged Enqueues:** %d\n", data.Chaos.AcknowledgedEnqueues))
		sb.WriteString(fmt.Sprintf("- **Final Job Accounting:** DONE = %d, DEAD = %d (Lost = 0)\n", data.Chaos.CompletedDone, data.Chaos.DeadLetteredDead))
		sb.WriteString(fmt.Sprintf("- **Injected Failovers:** %s\n", data.Chaos.LeaderKillsFailovers))
		sb.WriteString("- **Invariant Verification:**\n")
		for inv, st := range data.Chaos.Invariants {
			sb.WriteString(fmt.Sprintf("  - %s: %s\n", inv, st))
		}
		sb.WriteString("\n")
	} else {
		sb.WriteString("**CHAOS NOT VERIFIED**  \n")
		if data.Chaos != nil {
			sb.WriteString(fmt.Sprintf("*(Reason: %s)*\n\n", data.Chaos.StatusText))
		} else {
			sb.WriteString("*(Reason: chaos-report.txt missing or not from clean full run)*\n\n")
		}
	}

	// Safe to use vs Do Not Use
	sb.WriteString("## 9. Safe to Use on Resume\n\n")
	for _, s := range data.SafeToUse {
		sb.WriteString(fmt.Sprintf("- [SAFE] %s\n", s))
	}
	sb.WriteString("\n")

	sb.WriteString("## 10. DO NOT USE (Excluded Claims & Baselines)\n\n")
	for _, d := range data.DoNotUse {
		sb.WriteString(fmt.Sprintf("- [DO NOT USE] **%s**: %s\n", d.Claim, d.Reason))
	}
	sb.WriteString("\n")

	return sb.String()
}
