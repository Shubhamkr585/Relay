package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	chaosDir := "chaos-results"
	reportPath := findLatestChaosReport(chaosDir)
	if reportPath == "" {
		fmt.Fprintf(os.Stderr, "Error: No chaos report found in %s\n", chaosDir)
		os.Exit(1)
	}

	fmt.Printf("Injecting chaos results from: %s\n", reportPath)
	content, err := os.ReadFile(reportPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", reportPath, err)
		os.Exit(1)
	}

	reportText := string(content)
	grandTotalIdx := strings.LastIndex(reportText, "GRAND TOTAL ACROSS ALL SCENARIOS & RUNS:")
	if grandTotalIdx == -1 {
		fmt.Fprintf(os.Stderr, "Error: Could not find GRAND TOTAL block in %s\n", reportPath)
		os.Exit(1)
	}

	grandTotalBlock := reportText[grandTotalIdx:]
	endBorder := strings.Index(grandTotalBlock, "==========================================================================================")
	if endBorder != -1 {
		grandTotalBlock = grandTotalBlock[:endBorder]
	}
	grandTotalBlock = strings.TrimSpace(grandTotalBlock)

	readmePath := "README.md"
	readmeBytes, err := os.ReadFile(readmePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", readmePath, err)
		os.Exit(1)
	}

	startMarker := "<!-- CHAOS:START -->"
	endMarker := "<!-- CHAOS:END -->"

	readmeStr := string(readmeBytes)
	startIdx := strings.Index(readmeStr, startMarker)
	endIdx := strings.Index(readmeStr, endMarker)

	if startIdx == -1 || endIdx == -1 || startIdx >= endIdx {
		fmt.Fprintf(os.Stderr, "Error: README.md missing valid %s and %s markers\n", startMarker, endMarker)
		os.Exit(1)
	}

	var sb strings.Builder
	sb.WriteString(startMarker + "\n\n")
	sb.WriteString("```text\n")
	sb.WriteString(grandTotalBlock + "\n")
	sb.WriteString("```\n\n")

	sb.WriteString("### Invariant Definitions\n\n")
	sb.WriteString("- **Inv 1 (No Lost Jobs):** Every acknowledged enqueue reaches terminal `DONE` or `DEAD` state.\n")
	sb.WriteString("- **Inv 2 (No Stuck Jobs):** No leased job remains unacknowledged beyond its lease duration without being promoted/redelivered.\n")
	sb.WriteString("- **Inv 3 (Ack Durability):** Once an `Ack` succeeds, the job is never reserved again.\n")
	sb.WriteString("- **Inv 4 (Fencing Tokens):** Any stale or zombie worker trying to Ack/Nack with an expired lease token is 100% rejected.\n")
	sb.WriteString("- **Inv 5 (Valid Redelivery):** Redelivery occurs only after a documented worker crash or lease expiry.\n")
	sb.WriteString("- **Inv 6 (Replica Convergence):** All alive nodes converge to byte-for-byte identical state dumps upon quiescent recovery.\n")
	sb.WriteString("- **Inv 7 (Index Integrity):** No duplicate entries in secondary bbolt index buckets.\n")
	sb.WriteString("- **Inv 8 (Time Monotonicity):** Logical state machine time `lastAppliedNow` never moves backwards.\n\n")

	sb.WriteString("### Scope & Non-Goals (What Chaos Testing Does NOT Prove)\n\n")
	sb.WriteString("- **Power-loss consistency not proven:** Process termination via `SIGKILL` was tested; physical power-loss / fsync hardware barrier cuts were not tested.\n")
	sb.WriteString("- **Single host / shared disk:** All 3 nodes ran on one machine sharing a single physical SSD.\n")
	sb.WriteString("- **No WAN partitioning:** Network faults tested loopback transport partitions, not cross-datacenter BGP splits.\n\n")

	newContent := readmeStr[:startIdx] + sb.String() + readmeStr[endIdx:]
	if err := os.WriteFile(readmePath, []byte(newContent), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Error updating %s: %v\n", readmePath, err)
		os.Exit(1)
	}

	fmt.Println("Successfully updated README.md with chaos verification summary.")
}

func findLatestChaosReport(root string) string {
	var candidates []string
	// Check subdirectories
	entries, err := os.ReadDir(root)
	if err == nil {
		for _, e := range entries {
			if e.IsDir() {
				p := filepath.Join(root, e.Name(), "chaos-report.txt")
				if _, err := os.Stat(p); err == nil {
					candidates = append(candidates, p)
				}
			}
		}
	}
	// Check root
	rootFile := filepath.Join(root, "chaos-report.txt")
	if _, err := os.Stat(rootFile); err == nil {
		candidates = append(candidates, rootFile)
	}

	if len(candidates) == 0 {
		return ""
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i] > candidates[j]
	})
	return candidates[0]
}
