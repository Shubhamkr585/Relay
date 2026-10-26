package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
)

func main() {
	cmd := exec.Command("gofmt", "-l", ".")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error running gofmt: %v\n", err)
		os.Exit(1)
	}
	if out.Len() > 0 {
		fmt.Fprintf(os.Stderr, "Error: The following files are not formatted with gofmt:\n%s", out.String())
		os.Exit(1)
	}
	fmt.Println("gofmt check passed.")
}
