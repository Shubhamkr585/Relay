package bench

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// RunnerOptions controls benchmark execution parameters.
type RunnerOptions struct {
	Quick       bool
	Medium      bool
	DryRun      bool
	Only        string
	Repetitions int
	Duration    time.Duration
	Warmup      time.Duration
	OutputDir   string
	Servers     string
	BinPath     string
	DataDir     string
	Trials      int
	PayloadSize int
}

// DefaultRunnerOptions returns default runner settings for bench-full.
func DefaultRunnerOptions() RunnerOptions {
	return RunnerOptions{
		Quick:       false,
		Medium:      false,
		DryRun:      false,
		Only:        "",
		Repetitions: 5,
		Duration:    20 * time.Second,
		Warmup:      5 * time.Second,
		OutputDir:   "bench-results",
		BinPath:     "bin/relayd.exe",
		DataDir:     "data/bench",
		Trials:      20,
		PayloadSize: 256,
	}
}

// MediumRunnerOptions returns settings for medium mode (3 reps x 10s, 10 trials).
func MediumRunnerOptions() RunnerOptions {
	return RunnerOptions{
		Quick:       false,
		Medium:      true,
		DryRun:      false,
		Only:        "",
		Repetitions: 3,
		Duration:    10 * time.Second,
		Warmup:      2 * time.Second,
		OutputDir:   "bench-results",
		BinPath:     "bin/relayd.exe",
		DataDir:     "data/bench",
		Trials:      10,
		PayloadSize: 256,
	}
}

// QuickRunnerOptions returns settings for make bench quick mode.
func QuickRunnerOptions() RunnerOptions {
	return RunnerOptions{
		Quick:       true,
		Medium:      false,
		DryRun:      false,
		Only:        "",
		Repetitions: 1,
		Duration:    3 * time.Second,
		Warmup:      1 * time.Second,
		OutputDir:   "bench-results",
		BinPath:     "bin/relayd.exe",
		DataDir:     "data/bench",
		Trials:      3,
		PayloadSize: 256,
	}
}

// RunBenchmarkSuite executes the complete Phase 5 benchmark suite (B0-B5).
func RunBenchmarkSuite(opts RunnerOptions) (*SuiteReport, string, error) {
	if opts.DryRun {
		printDryRunMatrix(opts)
		return nil, "", nil
	}

	mode := "full"
	if opts.Only != "" {
		mode = "partial"
	} else if opts.Quick {
		if opts.Trials != 3 || opts.Repetitions != 1 || opts.Duration != 3*time.Second {
			mode = "partial"
		} else {
			mode = "quick"
		}
	} else if opts.Medium {
		if opts.Trials != 10 || opts.Repetitions != 3 || opts.Duration != 10*time.Second {
			mode = "partial"
		} else {
			mode = "medium"
		}
	} else {
		if opts.Trials != 20 || opts.Repetitions != 5 || opts.Duration != 20*time.Second {
			mode = "partial"
		} else {
			mode = "full"
		}
	}

	shouldRun := func(sec string) bool {
		if opts.Only == "" {
			return true
		}
		parts := strings.Split(opts.Only, ",")
		for _, p := range parts {
			if strings.EqualFold(strings.TrimSpace(p), sec) {
				return true
			}
		}
		return false
	}

	timestamp := time.Now().Format("20060102-150405")
	runDir := filepath.Join(opts.OutputDir, timestamp)
	if err := os.MkdirAll(runDir, 0755); err != nil {
		return nil, "", fmt.Errorf("failed to create run dir: %w", err)
	}

	benchDataDir := filepath.Join(opts.DataDir, timestamp)
	_ = os.MkdirAll(benchDataDir, 0755)
	defer os.RemoveAll(benchDataDir)

	fmt.Println("================================================================================")
	fmt.Printf("  RELAY REPRODUCIBLE BENCHMARK SUITE (PHASE 5) - MODE: %s\n", mode)
	fmt.Println("================================================================================")

	// B0. Environment capture
	relayCfg := DefaultRelayBenchConfig()
	env := CaptureEnv(relayCfg, mode)
	fmt.Println("  ENVIRONMENT SUMMARY:")
	fmt.Printf("  CPU Model            : %s\n", env.CPUModel)
	fmt.Printf("  Logical Cores        : %d (GOMAXPROCS=%d)\n", env.LogicalCores, env.GOMAXPROCS)
	fmt.Printf("  Total Memory         : %s\n", env.TotalRAMGB)
	fmt.Printf("  Operating System     : %s (%s)\n", env.OSCaption, env.Arch)
	fmt.Printf("  Storage Media        : %s\n", env.DiskType)
	fmt.Printf("  Power Plan           : %s\n", env.PowerPlan)
	fmt.Printf("  Power Source         : %s\n", env.ACStatus)
	fmt.Printf("  Clock Resolution     : %s (%d ns)\n", env.ClockResolution, env.ClockResolutionNs)
	fmt.Printf("  Git Commit           : %s (Dirty: %t)\n", env.GitCommit, env.GitDirty)
	fmt.Printf("  Configuration Line   : %s\n", env.ConfigLine)
	fmt.Println("--------------------------------------------------------------------------------")
	if env.GitDirty {
		fmt.Println("  WARNING: Working tree is DIRTY. Uncommitted changes detected!")
	}
	if env.IsOnBattery {
		fmt.Printf("  WARNING: Running on battery (%s). Connect AC power for full performance!\n", env.ACStatus)
	}
	fmt.Printf("  %s\n", env.PreflightNote)
	fmt.Println("================================================================================")

	report := &SuiteReport{Env: *env}

	reps := opts.Repetitions
	if reps <= 0 {
		reps = 1
	}

	// ---------------------------------------------------------------------------
	// B0b. Raw disk fsync baseline
	// ---------------------------------------------------------------------------
	if shouldRun("b0b") {
		fmt.Println("\n[1/6] Running B0b: Raw Disk fsync Baseline...")
		b0bResults := make([]RepetitionResult, 0, reps)
		for r := 1; r <= reps; r++ {
			fmt.Printf("  Repetition %d/%d (duration=%v, warmup=%v)...\n", r, reps, opts.Duration, opts.Warmup)
			res, err := RunFsyncBenchmark(benchDataDir, opts.PayloadSize, opts.Duration, opts.Warmup)
			if err != nil {
				return nil, "", fmt.Errorf("b0b failed on rep %d: %w", r, err)
			}
			res.Repetition = r
			b0bResults = append(b0bResults, *res)
		}
		report.B0b_Fsync = AggregateRepetitions("b0b_fsync", "B0b: Raw Disk fsync Baseline",
			"Direct file writes with sequential fsync to establish disk write ceiling",
			map[string]string{"record_size": fmt.Sprintf("%d", opts.PayloadSize)}, b0bResults, "")
		fmt.Printf("  -> Result: %s ops/sec | p50: %s | p95: %s | p99: %s\n",
			FormatOpsRange(report.B0b_Fsync.Throughput),
			FormatDurationRange(report.B0b_Fsync.LatencyP50),
			FormatDurationRange(report.B0b_Fsync.LatencyP95),
			FormatDurationRange(report.B0b_Fsync.LatencyP99))
	}

	// ---------------------------------------------------------------------------
	// B1. Single-node storage engine (no Raft)
	// Matrix: c = 1, 8, 32, 64 across 4 modes (full matrix always run)
	// ---------------------------------------------------------------------------
	if shouldRun("b1") {
		fmt.Println("\n[2/6] Running B1: Single-Node Storage Engine Full Matrix (c=1, 8, 32, 64)...")
		type b1Spec struct {
			mode         string
			concurrency  int
			batchWindow  time.Duration
			batchMaxSize int
			noSync       bool
		}

		b1Concurrencies := []int{1, 8, 32, 64}
		b1Specs := make([]b1Spec, 0, 16)

		// 1. Unbatched (0ms/1-op), fsync ON
		for _, c := range b1Concurrencies {
			b1Specs = append(b1Specs, b1Spec{
				mode:         "Unbatched (0ms/1-op)",
				concurrency:  c,
				batchWindow:  0,
				batchMaxSize: 1,
				noSync:       false,
			})
		}
		// 2. Group Commit (2ms/64-op), fsync ON
		for _, c := range b1Concurrencies {
			b1Specs = append(b1Specs, b1Spec{
				mode:         "Group Commit (2ms/64-op)",
				concurrency:  c,
				batchWindow:  2 * time.Millisecond,
				batchMaxSize: 64,
				noSync:       false,
			})
		}
		// 3. Unbatched (0ms/1-op), NoSync (comparison only)
		for _, c := range b1Concurrencies {
			b1Specs = append(b1Specs, b1Spec{
				mode:         "Unbatched (0ms/1-op)",
				concurrency:  c,
				batchWindow:  0,
				batchMaxSize: 1,
				noSync:       true,
			})
		}
		// 4. Group Commit (2ms/64-op), NoSync (comparison only)
		for _, c := range b1Concurrencies {
			b1Specs = append(b1Specs, b1Spec{
				mode:         "Group Commit (2ms/64-op)",
				concurrency:  c,
				batchWindow:  2 * time.Millisecond,
				batchMaxSize: 64,
				noSync:       true,
			})
		}

		report.B1_Storage = make([]*BenchmarkResult, 0, len(b1Specs))
		for _, spec := range b1Specs {
			durStr := "fsync ON"
			if spec.noSync {
				durStr = "NoSync"
			}
			label := fmt.Sprintf("%s, c=%d, %s", spec.mode, spec.concurrency, durStr)
			fmt.Printf("  Running %s...\n", label)
			repsResults := make([]RepetitionResult, 0, reps)
			for r := 1; r <= reps; r++ {
				res, err := RunStorageBenchmark(benchDataDir, StorageBenchConfig{
					Concurrency:  spec.concurrency,
					BatchWindow:  spec.batchWindow,
					BatchMaxSize: spec.batchMaxSize,
					NoSync:       spec.noSync,
					PayloadSize:  opts.PayloadSize,
					Duration:     opts.Duration,
					Warmup:       opts.Warmup,
				})
				if err != nil {
					return nil, "", fmt.Errorf("b1 failed (%s): %w", label, err)
				}
				res.Repetition = r
				repsResults = append(repsResults, *res)
			}
			agg := AggregateRepetitions(
				fmt.Sprintf("b1_%s_c%d_ns%t", spec.mode, spec.concurrency, spec.noSync),
				fmt.Sprintf("B1: %s", spec.mode),
				"Single-node BBoltEngine Enqueue workload",
				map[string]string{
					"mode":        spec.mode,
					"concurrency": fmt.Sprintf("%d", spec.concurrency),
					"window":      spec.batchWindow.String(),
					"batch_max":   fmt.Sprintf("%d", spec.batchMaxSize),
					"nosync":      fmt.Sprintf("%t", spec.noSync),
				},
				repsResults, "",
			)
			report.B1_Storage = append(report.B1_Storage, agg)
			fmt.Printf("    -> %s ops/sec | p50: %s | p95: %s | p99: %s [Sanity: %t]\n",
				FormatOpsRange(agg.Throughput),
				FormatDurationRange(agg.LatencyP50),
				FormatDurationRange(agg.LatencyP95),
				FormatDurationRange(agg.LatencyP99),
				agg.OverallPass,
			)
		}
	}

	// ---------------------------------------------------------------------------
	// B2. 3-node cluster closed-loop enqueue
	// Matrix: c = 1, 8, 32, 64, 128 for BOTH:
	//   (a) Leader micro-batcher (2ms/64-op), fsync ON
	//   (b) Unbatched (0ms/1-op), fsync ON
	// ---------------------------------------------------------------------------
	var maxMeasuredB2Throughput float64
	var defaultTarget ClusterTarget

	if shouldRun("b2") {
		fmt.Println("\n[3/6] Running B2: 3-Node Cluster Closed-Loop Enqueue Full Matrix (c=1, 8, 32, 64, 128)...")
		clusterConcurrencies := []int{1, 8, 32, 64, 128}

		type b2ModeSpec struct {
			name         string
			batchWindow  time.Duration
			batchMaxSize int
		}

		b2Modes := []b2ModeSpec{
			{
				name:         "Leader Micro-Batcher (2ms/64-op)",
				batchWindow:  2 * time.Millisecond,
				batchMaxSize: 64,
			},
			{
				name:         "Unbatched (0ms/1-op)",
				batchWindow:  0,
				batchMaxSize: 1,
			},
		}

		report.B2_Cluster = make([]*BenchmarkResult, 0, len(b2Modes)*len(clusterConcurrencies))

		for _, mSpec := range b2Modes {
			for _, c := range clusterConcurrencies {
				label := fmt.Sprintf("%s, Concurrency=%d", mSpec.name, c)
				fmt.Printf("  Running %s...\n", label)
				repsResults := make([]RepetitionResult, 0, reps)
				for r := 1; r <= reps; r++ {
					var repTarget ClusterTarget
					var repErr error
					if opts.Servers != "" {
						repTarget, repErr = NewRemoteTarget(opts.Servers)
						if repErr != nil {
							return nil, "", fmt.Errorf("failed connecting to remote target: %w", repErr)
						}
					} else {
						repDir := filepath.Join(benchDataDir, fmt.Sprintf("inproc_b2_%d_c%d_r%d", mSpec.batchMaxSize, c, r))
						_ = os.MkdirAll(repDir, 0755)
						repTarget, repErr = NewInProcessTargetCustom(repDir, mSpec.batchWindow, mSpec.batchMaxSize)
						if repErr != nil {
							return nil, "", fmt.Errorf("failed starting in-process cluster for %s (c=%d, rep=%d): %w", mSpec.name, c, r, repErr)
						}
					}

					// Verify warmup complete: leader stable, all followers know the leader
					if err := VerifyClusterWarmup(repTarget, 10*time.Second); err != nil {
						repTarget.Close()
						return nil, "", fmt.Errorf("b2 warmup verification failed on %s (rep %d): %w", label, r, err)
					}

					res, err := RunClusterBenchmark(repTarget, ClusterBenchConfig{
						Concurrency: c,
						PayloadSize: opts.PayloadSize,
						Duration:    opts.Duration,
						Warmup:      opts.Warmup,
					})
					repTarget.Close()
					if err != nil {
						return nil, "", fmt.Errorf("b2 failed on %s: %w", label, err)
					}
					res.Repetition = r
					repsResults = append(repsResults, *res)
				}
				agg := AggregateRepetitions(
					fmt.Sprintf("b2_cluster_%s_c%d", mSpec.name, c),
					"B2: 3-Node Cluster Enqueue",
					"3-Node Raft cluster closed-loop enqueue with strict fsync",
					map[string]string{
						"mode":        mSpec.name,
						"concurrency": fmt.Sprintf("%d", c),
					},
					repsResults, "",
				)
				report.B2_Cluster = append(report.B2_Cluster, agg)
				if agg.Throughput.Median > maxMeasuredB2Throughput {
					maxMeasuredB2Throughput = agg.Throughput.Median
				}
				fmt.Printf("    -> %s ops/sec | %s | p50: %s | p95: %s | p99: %s [Sanity: %t]\n",
					FormatOpsRange(agg.Throughput),
					agg.StabilityStatus,
					FormatDurationRange(agg.LatencyP50),
					FormatDurationRange(agg.LatencyP95),
					FormatDurationRange(agg.LatencyP99),
					agg.OverallPass,
				)
			}
		}
	}

	if (shouldRun("b3") || shouldRun("b4")) && defaultTarget == nil {
		inProcDir := filepath.Join(benchDataDir, "inproc_default")
		_ = os.MkdirAll(inProcDir, 0755)
		var err error
		defaultTarget, err = NewInProcessTarget(inProcDir)
		if err != nil {
			return nil, "", fmt.Errorf("failed starting in-process cluster: %w", err)
		}
		defer defaultTarget.Close()
	}

	// Settle cluster before next phase
	time.Sleep(1 * time.Second)

	// ---------------------------------------------------------------------------
	// B3. Open-loop latency (eliminating coordinated omission)
	// Base rates on highest throughput found across the whole B2 sweep!
	// ---------------------------------------------------------------------------
	if shouldRun("b3") {
		fmt.Printf("\n[4/6] Running B3: Open-Loop Latency (Base Max B2 Throughput = %.0f ops/sec)...\n", maxMeasuredB2Throughput)
		if maxMeasuredB2Throughput <= 0 {
			maxMeasuredB2Throughput = 500
		}

		fractions := []struct {
			label    string
			fraction float64
		}{
			{"25%", 0.25},
			{"50%", 0.50},
			{"80%", 0.80},
		}

		report.B3_OpenLoop = make([]*BenchmarkResult, 0, len(fractions))
		for _, frac := range fractions {
			targetRate := maxMeasuredB2Throughput * frac.fraction
			fmt.Printf("  Running Open-Loop at %s (target rate: %.0f req/sec)...\n", frac.label, targetRate)
			repsResults := make([]RepetitionResult, 0, reps)
			for r := 1; r <= reps; r++ {
				res, err := RunOpenLoopBenchmark(defaultTarget, OpenLoopConfig{
					TargetRate:  targetRate,
					Duration:    opts.Duration,
					Warmup:      opts.Warmup,
					PayloadSize: opts.PayloadSize,
				})
				if err != nil {
					return nil, "", fmt.Errorf("b3 failed (%s): %w", frac.label, err)
				}
				res.Repetition = r
				repsResults = append(repsResults, *res)
				if len(res.ThermalSlices) > 0 {
					fmt.Printf("    Slice Throughputs (5s): %v ops/sec\n", res.ThermalSlices)
				}
			}
			agg := AggregateRepetitions(
				fmt.Sprintf("b3_openloop_%s", frac.label),
				fmt.Sprintf("B3: Open-Loop Latency (%s)", frac.label),
				"Scheduled timeline open-loop latency measurement",
				map[string]string{
					"target_rate": fmt.Sprintf("%.0f", targetRate),
					"fraction":    frac.label,
				},
				repsResults, "",
			)
			report.B3_OpenLoop = append(report.B3_OpenLoop, agg)
			fmt.Printf("    -> Achieved: %s ops/sec | p50: %s | p95: %s | p99: %s\n",
				FormatOpsRange(agg.Throughput),
				FormatDurationRange(agg.LatencyP50),
				FormatDurationRange(agg.LatencyP95),
				FormatDurationRange(agg.LatencyP99),
			)
		}
	}

	// Settle cluster before next phase
	time.Sleep(1 * time.Second)

	// ---------------------------------------------------------------------------
	// B4. End-to-end flow: Enqueue -> Reserve -> Ack
	// Sweep workers: 8, 32, 64 in unthrottled and rate-limited producer modes
	// ---------------------------------------------------------------------------
	if shouldRun("b4") {
		fmt.Println("\n[5/6] Running B4: End-to-End Pipeline Flow (Sweep Workers: 8, 32, 64)...")
		workerCounts := []int{8, 32, 64}
		report.B4_E2E = make([]*BenchmarkResult, 0, len(workerCounts)*2)

		unthrottledAggs := make(map[int]*BenchmarkResult)

		// Phase A: Unthrottled Producers
		for _, w := range workerCounts {
			label := fmt.Sprintf("Unthrottled Producers, Workers=%d", w)
			fmt.Printf("  Running %s...\n", label)
			b4Reps := make([]RepetitionResult, 0, reps)
			for r := 1; r <= reps; r++ {
				res, err := RunE2EBenchmark(defaultTarget, E2EBenchConfig{
					Producers:    8,
					Workers:      w,
					RateLimitOps: 0, // unthrottled
					PayloadSize:  opts.PayloadSize,
					Duration:     opts.Duration,
					Warmup:       opts.Warmup,
				})
				if err != nil {
					return nil, "", fmt.Errorf("b4 failed on rep %d: %w", r, err)
				}
				res.Repetition = r
				b4Reps = append(b4Reps, *res)
			}
			agg := AggregateRepetitions(
				fmt.Sprintf("b4_e2e_unthrottled_w%d", w),
				"B4: End-to-End Flow",
				"Concurrent producers and workers measuring full lifecycle latency and throughput",
				map[string]string{
					"mode":      "Unthrottled Producers",
					"producers": "8",
					"workers":   fmt.Sprintf("%d", w),
					"backlog":   fmt.Sprintf("%d", b4Reps[0].Backlog),
				},
				b4Reps, "",
			)
			report.B4_E2E = append(report.B4_E2E, agg)
			unthrottledAggs[w] = agg
			if agg.Valid {
				fmt.Printf("    -> Completed: %s jobs/sec | p50: %s | p95: %s | Max: %s | Backlog: %d [Sanity: %t]\n",
					FormatOpsRange(agg.Throughput),
					FormatDurationRange(agg.LatencyP50),
					FormatDurationRange(agg.LatencyP95),
					FormatDurationRange(agg.LatencyMax),
					b4Reps[0].Backlog,
					agg.OverallPass,
				)
			} else {
				fmt.Printf("    -> INVALID: %s | Backlog: %d [Sanity: %t]\n",
					agg.InvalidReason,
					b4Reps[0].Backlog,
					agg.OverallPass,
				)
			}
		}

		// Phase B: Rate-Limited Producers (at 50% of MEASURED unthrottled completion rate of the SAME worker count)
		fmt.Printf("\n  --- Running B4 Rate-Limited Producer Mode (50%% of Measured Capacity per Worker Count) ---\n")
		for _, w := range workerCounts {
			unthrottledAgg := unthrottledAggs[w]
			if unthrottledAgg == nil || !unthrottledAgg.Valid || unthrottledAgg.Unstable || unthrottledAgg.Throughput.Median <= 0 {
				reason := "unthrottled baseline missing"
				if unthrottledAgg != nil {
					if !unthrottledAgg.Valid {
						reason = fmt.Sprintf("unthrottled baseline is INVALID (%s)", unthrottledAgg.InvalidReason)
					} else if unthrottledAgg.Unstable {
						reason = fmt.Sprintf("unthrottled baseline is UNSTABLE (CV: %.1f%%)", unthrottledAgg.CV)
					} else if unthrottledAgg.Throughput.Median <= 0 {
						reason = "unthrottled baseline achieved 0 throughput"
					}
				}
				fmt.Printf("  Skipping Rate-Limited Mode for Workers=%d: %s\n", w, reason)
				skippedAgg := &BenchmarkResult{
					ID:          fmt.Sprintf("b4_e2e_ratelimited_w%d", w),
					Name:        "B4: End-to-End Flow (Rate-Limited)",
					Description: "Rate-limited producers preventing backlog saturation to measure true service latency",
					Config: map[string]string{
						"mode":      "Rate-Limited (50% Capacity)",
						"producers": "8",
						"workers":   fmt.Sprintf("%d", w),
					},
					Valid:           false,
					InvalidReason:   fmt.Sprintf("SKIPPED: %s", reason),
					StabilityStatus: "SKIPPED",
					OverallPass:     false,
				}
				report.B4_E2E = append(report.B4_E2E, skippedAgg)
				continue
			}

			rateLimitTarget := unthrottledAgg.Throughput.Median * 0.50
			label := fmt.Sprintf("Rate-Limited Producers (50%% Capacity), Workers=%d (Target=%.0f ops/s)", w, rateLimitTarget)
			fmt.Printf("  Running %s...\n", label)
			b4Reps := make([]RepetitionResult, 0, reps)
			for r := 1; r <= reps; r++ {
				res, err := RunE2EBenchmark(defaultTarget, E2EBenchConfig{
					Producers:    8,
					Workers:      w,
					RateLimitOps: rateLimitTarget,
					PayloadSize:  opts.PayloadSize,
					Duration:     opts.Duration,
					Warmup:       opts.Warmup,
				})
				if err != nil {
					return nil, "", fmt.Errorf("b4 rate-limited failed on rep %d: %w", r, err)
				}
				res.Repetition = r
				b4Reps = append(b4Reps, *res)
			}
			agg := AggregateRepetitions(
				fmt.Sprintf("b4_e2e_ratelimited_w%d", w),
				"B4: End-to-End Flow (Rate-Limited)",
				"Rate-limited producers preventing backlog saturation to measure true service latency",
				map[string]string{
					"mode":          "Rate-Limited (50% Capacity)",
					"producers":     "8",
					"workers":       fmt.Sprintf("%d", w),
					"offered_rate":  fmt.Sprintf("%.0f", rateLimitTarget),
					"achieved_rate": fmt.Sprintf("%.0f", b4Reps[0].Throughput),
					"backlog":       fmt.Sprintf("%d", b4Reps[0].Backlog),
				},
				b4Reps, "",
			)
			report.B4_E2E = append(report.B4_E2E, agg)
			if agg.Valid {
				fmt.Printf("    -> Completed: %s jobs/sec | p50: %s | p95: %s | Max: %s | Backlog: %d [Sanity: %t]\n",
					FormatOpsRange(agg.Throughput),
					FormatDurationRange(agg.LatencyP50),
					FormatDurationRange(agg.LatencyP95),
					FormatDurationRange(agg.LatencyMax),
					b4Reps[0].Backlog,
					agg.OverallPass,
				)
			} else {
				fmt.Printf("    -> INVALID: %s | Backlog: %d [Sanity: %t]\n",
					agg.InvalidReason,
					b4Reps[0].Backlog,
					agg.OverallPass,
				)
			}
		}
	}

	// Settle cluster before subprocess failover
	time.Sleep(1 * time.Second)

	// ---------------------------------------------------------------------------
	// B5. Leader failover latency (real relayd child processes)
	// Run 1: Production Defaults (1s/1s/500ms) - Primary Result
	// Run 2: Aggressive Timeouts (100ms/300ms/100ms) - Secondary Result
	// ---------------------------------------------------------------------------
	if shouldRun("b5") {
		fmt.Printf("\n[6/6] Running B5: Leader Failover Latency (%d trials)...\n", opts.Trials)
		report.B5_Failover = make([]*FailoverResult, 0, 2)

		// Run 1: Production Defaults (Primary Headline)
		fmt.Printf("\n  [B5-Primary] Running with Production Defaults (1s/1s/500ms, Primary Headline, %d trials)...\n", opts.Trials)
		b5CfgDefault := FailoverBenchConfig{
			ID:                 "b5_failover_default",
			Name:               "B5: Leader Failover Latency (Production Defaults, Primary Headline)",
			BinPath:            opts.BinPath,
			BaseDir:            benchDataDir,
			TrialsCount:        opts.Trials,
			HeartbeatTimeout:   1000 * time.Millisecond,
			ElectionTimeout:    1000 * time.Millisecond,
			LeaderLeaseTimeout: 500 * time.Millisecond,
			IsPrimaryHeadline:  true,
		}
		resDefault, err := RunFailoverBenchmark(b5CfgDefault)
		if err != nil {
			fmt.Printf("  Warning: B5 failover (defaults) returned error: %v\n", err)
		} else {
			report.B5_Failover = append(report.B5_Failover, resDefault)
			p95Str := FormatDuration(resDefault.P95Duration)
			if !resDefault.P95Valid {
				p95Str = "n too small"
			}
			fmt.Printf("  -> Failover Latency: Min: %s | Median: %s | p95: %s | Max: %s [Lost Jobs: %d]\n",
				FormatDuration(resDefault.MinDuration),
				FormatDuration(resDefault.MedianDuration),
				p95Str,
				FormatDuration(resDefault.MaxDuration),
				resDefault.TotalLostJobs,
			)
		}

		time.Sleep(1 * time.Second)

		// Run 2: Aggressive Flags (Secondary Non-Default)
		fmt.Printf("\n  [B5-Aggressive] Running with Aggressive Timeouts (100ms/300ms/100ms, Non-Default, %d trials)...\n", opts.Trials)
		b5CfgAggressive := FailoverBenchConfig{
			ID:                 "b5_failover_aggressive",
			Name:               "B5: Leader Failover Latency (Aggressive Timeouts, Secondary Non-Default)",
			BinPath:            opts.BinPath,
			BaseDir:            benchDataDir,
			TrialsCount:        opts.Trials,
			HeartbeatTimeout:   100 * time.Millisecond,
			ElectionTimeout:    300 * time.Millisecond,
			LeaderLeaseTimeout: 100 * time.Millisecond,
			IsPrimaryHeadline:  false,
		}
		resAggressive, err := RunFailoverBenchmark(b5CfgAggressive)
		if err != nil {
			fmt.Printf("  Warning: B5 failover (aggressive) returned error: %v\n", err)
		} else {
			report.B5_Failover = append(report.B5_Failover, resAggressive)
			p95Str := FormatDuration(resAggressive.P95Duration)
			if !resAggressive.P95Valid {
				p95Str = "n too small"
			}
			fmt.Printf("  -> Failover Latency: Min: %s | Median: %s | p95: %s | Max: %s [Lost Jobs: %d]\n",
				FormatDuration(resAggressive.MinDuration),
				FormatDuration(resAggressive.MedianDuration),
				p95Str,
				FormatDuration(resAggressive.MaxDuration),
				resAggressive.TotalLostJobs,
			)
		}
	}

	// ---------------------------------------------------------------------------
	// Write Results and Generate Report
	// ---------------------------------------------------------------------------
	reportPath, err := GenerateReportFiles(runDir, report)
	if err != nil {
		return nil, "", fmt.Errorf("failed to generate report files: %w", err)
	}

	fmt.Println("\n================================================================================")
	fmt.Printf("  BENCHMARK SUITE COMPLETE! Reports saved to: %s\n", runDir)
	fmt.Printf("  Report Markdown File: %s\n", reportPath)
	fmt.Println("================================================================================")

	return report, reportPath, nil
}

func printDryRunMatrix(opts RunnerOptions) {
	mode := "full"
	if opts.Only != "" {
		mode = "partial"
	} else if opts.Quick {
		if opts.Trials != 3 || opts.Repetitions != 1 || opts.Duration != 3*time.Second {
			mode = "partial"
		} else {
			mode = "quick"
		}
	} else if opts.Medium {
		if opts.Trials != 10 || opts.Repetitions != 3 || opts.Duration != 10*time.Second {
			mode = "partial"
		} else {
			mode = "medium"
		}
	} else {
		if opts.Trials != 20 || opts.Repetitions != 5 || opts.Duration != 20*time.Second {
			mode = "partial"
		} else {
			mode = "full"
		}
	}

	reps := opts.Repetitions
	if reps <= 0 {
		reps = 1
	}
	trials := opts.Trials
	if trials <= 0 {
		trials = 20
	}

	shouldRun := func(sec string) bool {
		if opts.Only == "" {
			return true
		}
		parts := strings.Split(opts.Only, ",")
		for _, p := range parts {
			if strings.EqualFold(strings.TrimSpace(p), sec) {
				return true
			}
		}
		return false
	}

	testDuration := opts.Duration + opts.Warmup
	b0bEst := time.Duration(1*reps) * testDuration
	b1Est := time.Duration(16*reps) * testDuration
	clusterOverhead := 5 * time.Second // calibrated cluster start/stop & election overhead
	b2Est := time.Duration(10*reps)*testDuration + clusterOverhead
	b3Est := time.Duration(3*reps) * testDuration
	b4Est := time.Duration(6*reps) * testDuration
	b5Est := time.Duration(2*trials)*1800*time.Millisecond + 6*time.Second

	sumEst := time.Duration(0)

	fmt.Println("================================================================================")
	fmt.Println("  RELAY BENCHMARK SUITE - DRY RUN PLANNED MATRIX")
	fmt.Println("================================================================================")
	fmt.Printf("  Mode                 : %s\n", mode)
	if opts.Only != "" {
		fmt.Printf("  Only Workloads       : %s\n", opts.Only)
	}
	fmt.Printf("  Duration Per Test    : %v\n", opts.Duration)
	fmt.Printf("  Warmup Per Test      : %v\n", opts.Warmup)
	fmt.Printf("  Repetitions Per Test : %d\n", reps)
	fmt.Printf("  B5 Failover Trials   : %d\n", trials)
	fmt.Printf("  Payload Size         : %d bytes\n", opts.PayloadSize)
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("  PER-SECTION ESTIMATED EXECUTION TIMES:")
	if shouldRun("b0b") {
		fmt.Printf("  [B0b] Raw Disk fsync Baseline       : ~%v (1 config x %d reps x %v)\n", b0bEst, reps, testDuration)
		sumEst += b0bEst
	}
	if shouldRun("b1") {
		fmt.Printf("  [B1]  Single-Node Storage Matrix   : ~%v (16 configs x %d reps x %v)\n", b1Est, reps, testDuration)
		sumEst += b1Est
	}
	if shouldRun("b2") {
		fmt.Printf("  [B2]  3-Node Cluster Enqueue       : ~%v (10 configs x %d reps x %v + ~5s cluster setup)\n", b2Est, reps, testDuration)
		sumEst += b2Est
	}
	if shouldRun("b3") {
		fmt.Printf("  [B3]  Open-Loop Latency Sweep      : ~%v (3 configs x %d reps x %v)\n", b3Est, reps, testDuration)
		sumEst += b3Est
	}
	if shouldRun("b4") {
		fmt.Printf("  [B4]  End-to-End Pipeline Flow     : ~%v (6 configs x %d reps x %v)\n", b4Est, reps, testDuration)
		sumEst += b4Est
	}
	if shouldRun("b5") {
		fmt.Printf("  [B5]  Leader Failover (2 sweeps)   : ~%v (2 sweeps x %d trials + subprocess lifecycle)\n", b5Est, trials)
		sumEst += b5Est
	}

	overhead := time.Duration(float64(sumEst) * 0.10)
	totalEst := sumEst + overhead

	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("  Subtotal Estimated Workload Time : ~%v\n", sumEst)
	fmt.Printf("  Cluster/Process Overhead (+10%%)  : ~%v (calibrated from quick run start/stop telemetry)\n", overhead)
	fmt.Printf("  TOTAL ESTIMATED TIME             : ~%v\n", totalEst)
	fmt.Println("================================================================================")
	fmt.Println("  Dry-run complete. No benchmarks were executed.")
	fmt.Println("================================================================================")
}
