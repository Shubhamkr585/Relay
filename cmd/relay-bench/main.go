package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/shubhamkr585/relay/internal/bench"
)

func main() {
	var (
		quick       = flag.Bool("quick", false, "Run in quick mode (1 repetition, 3s duration, 3 trials)")
		medium      = flag.Bool("medium", false, "Run in medium mode (3 repetitions, 10s duration, 10 trials)")
		only        = flag.String("only", "", "Comma-separated list of benchmarks to run (b0b, b1, b2, b3, b4, b5)")
		repetitions = flag.Int("repetitions", 0, "Number of repetitions per benchmark (default: 5 full, 3 medium, 1 quick)")
		duration    = flag.Duration("duration", 0, "Measurement duration per repetition (default: 20s full, 10s medium, 3s quick)")
		warmup      = flag.Duration("warmup", 0, "Warmup duration per repetition (default: 5s full, 2s medium, 1s quick)")
		trials      = flag.Int("trials", 0, "Number of B5 failover trials (default: 20 full, 10 medium, 3 quick)")
		outputDir   = flag.String("output-dir", "bench-results", "Base directory to write benchmark output files")
		servers     = flag.String("servers", "", "Comma-separated list of existing cluster gRPC addrs (e.g. 127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053)")
		binPath     = flag.String("bin-path", "bin/relayd.exe", "Path to relayd executable binary")
		dataDir     = flag.String("data-dir", "data/bench", "Base scratch data directory")
		payloadSize = flag.Int("payload-size", 256, "Job payload size in bytes")
		dryRun      = flag.Bool("dry-run", false, "Print planned matrix, durations and repetitions without running")
	)
	flag.Parse()

	var opts bench.RunnerOptions
	if *quick {
		opts = bench.QuickRunnerOptions()
	} else if *medium {
		opts = bench.MediumRunnerOptions()
	} else {
		opts = bench.DefaultRunnerOptions()
	}

	opts.Only = *only

	opts.DryRun = *dryRun

	if *repetitions > 0 {
		opts.Repetitions = *repetitions
	}
	if *duration > 0 {
		opts.Duration = *duration
	}
	if *warmup > 0 {
		opts.Warmup = *warmup
	}
	if *trials > 0 {
		opts.Trials = *trials
	}
	if *outputDir != "" {
		opts.OutputDir = *outputDir
	}
	if *servers != "" {
		opts.Servers = *servers
	}
	if *binPath != "" {
		opts.BinPath = *binPath
	}
	if *dataDir != "" {
		opts.DataDir = *dataDir
	}
	if *payloadSize > 0 {
		opts.PayloadSize = *payloadSize
	}

	_, reportPath, err := bench.RunBenchmarkSuite(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Benchmark run error: %v\n", err)
		os.Exit(1)
	}
	if opts.DryRun {
		return
	}

	fmt.Printf("\nSuccess! Benchmark report generated at:\n%s\n", reportPath)
}
