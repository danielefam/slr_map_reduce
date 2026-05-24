// Command collect runs commands on remote hosts via SSH in parallel and saves
// the combined output (one section per host) to a local file.
//
// It reads collect_commands from the same manifest used by deploy/cleanup:
//
//	{
//	  "n":               100,
//	  "collect_commands": ["hostname", "uptime", "free -m | head -2"]
//	}
//
// Usage:
//
//	collect -m manifest.json -h hosts.txt -o stats.txt
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// Manifest mirrors the deploy manifest; only the fields needed for collection are used.
type Manifest struct {
	N               int      `json:"n"`
	CollectCommands []string `json:"collect_commands"`
}

type hostResult struct {
	host   string
	output string
	err    error
}

func main() {
	m := flag.String("m", "manifest.json", "path to the manifest file")
	h := flag.String("h", "hosts.txt", "path to the file containing hosts")
	o := flag.String("o", "stats.txt", "path to the output file")
	flag.Parse()

	manifest, err := parseManifest(*m)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing manifest: %v\n", err)
		os.Exit(1)
	}

	if len(manifest.CollectCommands) == 0 {
		fmt.Fprintln(os.Stderr, "no collect_commands defined in manifest")
		os.Exit(1)
	}

	hosts, err := readHosts(*h)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading hosts: %v\n", err)
		os.Exit(1)
	}

	if manifest.N > 0 && manifest.N < len(hosts) {
		hosts = hosts[:manifest.N]
	}

	fmt.Printf("Collecting stats from %d host(s)...\n", len(hosts))

	results := make([]hostResult, len(hosts))
	var wg sync.WaitGroup
	for i, host := range hosts {
		wg.Add(1)
		go func(idx int, h string) {
			defer wg.Done()
			out, err := sshCapture(h, manifest.CollectCommands)
			results[idx] = hostResult{host: h, output: out, err: err}
		}(i, host)
	}
	wg.Wait()

	var buf bytes.Buffer
	var failed int
	for _, r := range results {
		fmt.Fprintf(&buf, "=== %s ===\n", r.host)
		if r.err != nil {
			fmt.Fprintf(&buf, "ERROR: %v\n", r.err)
			fmt.Fprintf(os.Stderr, "[%s] ERROR: %v\n", r.host, r.err)
			failed++
		} else {
			buf.WriteString(r.output)
		}
		buf.WriteByte('\n')
	}

	if err := os.WriteFile(*o, buf.Bytes(), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "error writing output file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Saved stats from %d host(s) to %s\n", len(hosts)-failed, *o)
	if failed > 0 {
		fmt.Fprintf(os.Stderr, "%d host(s) failed\n", failed)
		os.Exit(1)
	}
}

// sshCapture connects to host and runs all commands, returning combined output.
func sshCapture(host string, commands []string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ssh",
		"-o", "StrictHostKeyChecking=no",
		"-o", "BatchMode=yes",
		"-o", "ConnectTimeout=10",
		"-o", "ServerAliveInterval=5",
		"-o", "ServerAliveCountMax=3",
		host,
		strings.Join(commands, " && "),
	)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func parseManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Manifest
	return &m, json.Unmarshal(data, &m)
}

func readHosts(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var hosts []string
	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			hosts = append(hosts, line)
		}
	}
	return hosts, nil
}
