package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type EnvMeta struct {
	GitCommit string `json:"git_commit"`
}

func main() {
	readmePath := "README.md"
	designPath := "DESIGN.md"

	var errors []string

	// 1. Check README.md
	readmeBytes, err := os.ReadFile(readmePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", readmePath, err)
		os.Exit(1)
	}
	readmeStr := string(readmeBytes)

	// Strip out BENCH and CHAOS marker blocks from README.md
	benchStart := strings.Index(readmeStr, "<!-- BENCH:START -->")
	benchEnd := strings.Index(readmeStr, "<!-- BENCH:END -->")
	chaosStart := strings.Index(readmeStr, "<!-- CHAOS:START -->")
	chaosEnd := strings.Index(readmeStr, "<!-- CHAOS:END -->")

	var readmeUnmarked strings.Builder
	var benchBlock string

	if benchStart != -1 && benchEnd != -1 && benchStart < benchEnd {
		benchBlock = readmeStr[benchStart : benchEnd+len("<!-- BENCH:END -->")]
	}

	// Build text outside markers
	cursor := 0
	if benchStart != -1 && benchEnd != -1 && benchStart < benchEnd {
		readmeUnmarked.WriteString(readmeStr[cursor:benchStart])
		cursor = benchEnd + len("<!-- BENCH:END -->")
	}
	if chaosStart != -1 && chaosEnd != -1 && chaosStart >= cursor && chaosStart < chaosEnd {
		readmeUnmarked.WriteString(readmeStr[cursor:chaosStart])
		cursor = chaosEnd + len("<!-- CHAOS:END -->")
	}
	readmeUnmarked.WriteString(readmeStr[cursor:])

	unmarkedREADME := readmeUnmarked.String()

	// 2. Check DESIGN.md
	designBytes, err := os.ReadFile(designPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", designPath, err)
		os.Exit(1)
	}
	designStr := string(designBytes)

	// Check forbidden phrases in README outside markers and in DESIGN.md
	forbiddenPhrases := []string{"1,500", "20,000", "10,000+"}
	for _, phrase := range forbiddenPhrases {
		if strings.Contains(unmarkedREADME, phrase) {
			errors = append(errors, fmt.Sprintf("README.md contains unsupported claim %q outside marker blocks", phrase))
		}
		if strings.Contains(designStr, phrase) {
			errors = append(errors, fmt.Sprintf("DESIGN.md contains unsupported claim %q", phrase))
		}
	}

	// Check regex for performance metrics outside marker blocks
	// Pattern detects throughput claims like "5,000 ops/sec", "200 jobs/sec", "failover time = 12ms", "p50 latency 1.5ms"
	perfPatterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)\b\d+(?:,\d+)?\s*(?:ops/sec|jobs/sec|req/s|fsyncs/sec)\b`),
		regexp.MustCompile(`(?i)\b(?:p50|p95|p99|latency|failover(?: time)?)\s*(?:=|:|\bis\b)?\s*\d+(?:\.\d+)?\s*(?:ms|µs|us|s)\b`),
	}

	for _, re := range perfPatterns {
		if loc := re.FindString(unmarkedREADME); loc != "" {
			errors = append(errors, fmt.Sprintf("README.md contains un-marked performance figure outside marker blocks: %q", loc))
		}
		if loc := re.FindString(designStr); loc != "" {
			errors = append(errors, fmt.Sprintf("DESIGN.md contains performance figure (must be strictly qualitative): %q", loc))
		}
	}

	// 3. Check staleness of BENCH marker block
	latestCommit := getLatestBenchCommit()
	if benchBlock != "" && latestCommit != "" && !strings.Contains(benchBlock, "TBD (run make bench") {
		// Look for commit hash in benchBlock
		commitRe := regexp.MustCompile("Commit `([a-f0-9]+)`")
		match := commitRe.FindStringSubmatch(benchBlock)
		if len(match) > 1 {
			recordedCommit := match[1]
			if !strings.HasPrefix(latestCommit, recordedCommit) && !strings.HasPrefix(recordedCommit, latestCommit) {
				errors = append(errors, fmt.Sprintf("README.md benchmark block is stale: recorded commit %s != latest results commit %s", recordedCommit, latestCommit))
			}
		}
	}

	if len(errors) > 0 {
		fmt.Fprintln(os.Stderr, "=== docs-check FAILED with the following violations ===")
		for _, e := range errors {
			fmt.Fprintf(os.Stderr, "  - %s\n", e)
		}
		os.Exit(1)
	}

	fmt.Println("=== docs-check PASSED: All documentation is strictly verified and adheres to reporting invariants ===")
}

func getLatestBenchCommit() string {
	entries, err := os.ReadDir("bench-results")
	if err != nil {
		return ""
	}
	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		}
	}
	if len(dirs) == 0 {
		return ""
	}
	sort.Slice(dirs, func(i, j int) bool {
		return dirs[i] > dirs[j]
	})

	envFile := filepath.Join("bench-results", dirs[0], "env.json")
	data, err := os.ReadFile(envFile)
	if err != nil {
		return ""
	}
	var env EnvMeta
	_ = json.Unmarshal(data, &env)
	return env.GitCommit
}
