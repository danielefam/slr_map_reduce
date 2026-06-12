// Command cleanup runs cleanup commands on remote hosts via SSH and removes
// deployed files from the NFS share.
//
// It is the inverse of deploy and uses the same manifest and hosts files:
//
//	{
//	  "nfs":              "user@nfs-host:/shared/path/",
//	  "files":            ["./binary", "./config.json"],
//	  "n":                5,
//	  "commands":         ["cd /shared/path && ./binary"],
//	  "cleanup_commands": ["kill $(cat /tmp/app.pid)", "rm -f /tmp/app.pid"]
//	}
//
// Usage:
//
//	cleanup -m manifest.json -h hosts.txt
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"scripts/internal/remote"
)

// Manifest mirrors the deploy manifest; only the fields needed for cleanup are used.
type Manifest struct {
	NFS             string   `json:"nfs"`
	Files           []string `json:"files"`
	N               int      `json:"n"`
	CleanupCommands []string `json:"cleanup_commands"`
}

func main() {
	m := flag.String("m", "manifest.json", "path to the manifest file")
	h := flag.String("h", "hosts.txt", "path to the file containing hosts")
	parallel := flag.Int("parallel", remote.DefaultParallelism, "maximum number of concurrent SSH operations")
	flag.Parse()

	manifest, err := parseManifest(*m)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing manifest: %v\n", err)
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

	var exitCode int

	if len(manifest.CleanupCommands) > 0 {
		fmt.Printf("Running cleanup commands on %d host(s)...\n", len(hosts))
		var (
			wg sync.WaitGroup
			mu sync.Mutex
		)
		sem := make(chan struct{}, remote.NormalizeParallelism(*parallel, len(hosts)))
		for _, host := range hosts {
			wg.Add(1)
			sem <- struct{}{}
			go func(h string) {
				defer wg.Done()
				defer func() { <-sem }()
				out, err := sshRun(h, manifest.CleanupCommands)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					fmt.Fprintf(os.Stderr, "[%s] ERROR: %v\n%s\n", h, err, out)
					exitCode = 1
				} else {
					fmt.Printf("[%s] OK\n%s", h, out)
				}
			}(host)
		}
		wg.Wait()
	}

	if len(manifest.Files) > 0 && manifest.NFS != "" {
		fmt.Printf("Removing files from NFS (%s)...\n", manifest.NFS)
		if err := removeFromNFS(manifest.NFS, manifest.Files); err != nil {
			fmt.Fprintf(os.Stderr, "error removing files from NFS: %v\n", err)
			exitCode = 1
		}
	}

	if exitCode == 0 {
		fmt.Println("Cleanup done.")
	}
	os.Exit(exitCode)
}

func parseManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func readHosts(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var hosts []string
	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			hosts = append(hosts, line)
		}
	}
	return hosts, nil
}

// sshRun connects to host and executes all commands in a single SSH session,
// joined with " && " so that a failure in any command stops execution.
// Returns the combined stdout+stderr output and any error.
func sshRun(host string, commands []string) (string, error) {
	return remote.RunSSH(host, commands, remote.DefaultSSHTimeout)
}

// removeFromNFS parses the NFS target (e.g. "user@host:/path/") and SSHes into
// the host to remove the basename of each deployed file from that path.
func removeFromNFS(nfs string, files []string) error {
	before, after, ok := strings.Cut(nfs, ":")
	if !ok {
		return fmt.Errorf("invalid NFS target %q: expected [user@]host:/path", nfs)
	}
	sshHost := before
	basePath := strings.TrimRight(after, "/")

	remotePaths := make([]string, len(files))
	for i, f := range files {
		remotePaths[i] = filepath.Join(basePath, filepath.Base(f))
	}

	quoted := make([]string, len(remotePaths))
	for i, path := range remotePaths {
		quoted[i] = shellQuote(path)
	}
	_, err := remote.RunSSH(sshHost, []string{"rm -f -- " + strings.Join(quoted, " ")}, remote.DefaultSSHTimeout)
	return err
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'"
}
