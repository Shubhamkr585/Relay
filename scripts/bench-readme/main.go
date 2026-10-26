package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/shubhamkr585/relay/internal/bench"
)

func main() {
	benchResultsDir := "bench-results"
	entries, err := os.ReadDir(benchResultsDir)
	if err != nil || len(entries) == 0 {
		fmt.Fprintf(os.Stderr, "Error: No benchmark results found in %s! Run 'make bench-full' first.\n", benchResultsDir)
		os.Exit(1)
	}

	// Filter directories
	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		}
	}

	if len(dirs) == 0 {
		fmt.Fprintf(os.Stderr, "Error: No benchmark run directories found in %s! Run 'make bench-full' first.\n", benchResultsDir)
		os.Exit(1)
	}

	// Sort descending to get the latest timestamp
	sort.Slice(dirs, func(i, j int) bool {
		return dirs[i] > dirs[j]
	})

	latestDir := filepath.Join(benchResultsDir, dirs[0])
	fmt.Printf("Inspecting latest benchmark run: %s\n", latestDir)

	envPath := filepath.Join(latestDir, "env.json")
	envData, err := os.ReadFile(envPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", envPath, err)
		os.Exit(1)
	}

	var env bench.EnvInfo
	if err := json.Unmarshal(envData, &env); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing %s: %v\n", envPath, err)
		os.Exit(1)
	}

	// Check 1: Refuse results unless mode == "full" (refuses mode=quick)
	if env.Mode != "full" {
		fmt.Fprintf(os.Stderr, "Error: Latest benchmark results are from mode=%q! 'make bench-readme' requires results from 'make bench-full' (mode=full).\n", env.Mode)
		os.Exit(1)
	}

	// Check 2: Refuse results with git_dirty=true
	if env.GitDirty {
		fmt.Fprintf(os.Stderr, "Error: Git working tree was dirty (git_dirty=true) during benchmark! 'make bench-readme' requires results from a clean commit.\n")
		os.Exit(1)
	}

	reportPath := filepath.Join(latestDir, "bench-report.md")
	reportData, err := os.ReadFile(reportPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", reportPath, err)
		os.Exit(1)
	}

	readmePath := "README.md"
	readmeData, err := os.ReadFile(readmePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", readmePath, err)
		os.Exit(1)
	}

	startMarker := "<!-- BENCH:START -->"
	endMarker := "<!-- BENCH:END -->"

	content := string(readmeData)
	startIdx := strings.Index(content, startMarker)
	endIdx := strings.Index(content, endMarker)

	if startIdx == -1 || endIdx == -1 || startIdx >= endIdx {
		fmt.Fprintf(os.Stderr, "Error: README.md missing valid %s and %s markers\n", startMarker, endMarker)
		os.Exit(1)
	}

	var sb strings.Builder
	sb.WriteString(startMarker + "\n\n")
	sb.WriteString(fmt.Sprintf("**Benchmark Results (Generated %s, Commit `%s`)**\n\n", env.CapturedAt, env.GitCommit))
	sb.WriteString(fmt.Sprintf("*Hardware: %s, %d Logical Cores, %s RAM, %s (%s), Storage: %s (Single host/shared disk)*\n\n",
		env.CPUModel, env.LogicalCores, env.TotalRAMGB, env.OSCaption, env.Arch, env.DiskType))

	// Extract tables from bench-report.md (strip first title line)
	lines := strings.Split(string(reportData), "\n")
	tableLines := make([]string, 0)
	skipHeader := true
	for _, l := range lines {
		if strings.HasPrefix(l, "## B0b.") || strings.HasPrefix(l, "## B1.") {
			skipHeader = false
		}
		if !skipHeader {
			tableLines = append(tableLines, l)
		}
	}
	sb.WriteString(strings.Join(tableLines, "\n"))
	sb.WriteString("\n" + endMarker)

	newContent := content[:startIdx] + sb.String() + content[endIdx+len(endMarker):]
	if err := os.WriteFile(readmePath, []byte(newContent), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Error updating %s: %v\n", readmePath, err)
		os.Exit(1)
	}

	fmt.Printf("Successfully injected latest benchmark tables from %s into %s!\n", latestDir, readmePath)
}
