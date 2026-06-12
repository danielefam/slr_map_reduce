// Command deploy copies files to an NFS share and runs commands on remote hosts via SSH.
//
// The manifest file (JSON) has the following structure:
//
//	{
//	  "nfs":      "user@nfs-host:/shared/path/",
//	  "files":    ["./binary", "./config.json"],
//	  "n":        5,
//	  "commands": ["cd /shared/path && ./binary"]
//	}
//
// Usage:
//
//	deploy -m manifest.json -h hosts.txt
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"

	"scripts/internal/remote"
)

// Manifest describes what to deploy and where.
type Manifest struct {
	// NFS is the scp destination for files, e.g. "user@host:/path/".
	NFS string `json:"nfs"`
	// Files is the list of local paths to copy to the NFS share.
	Files []string `json:"files"`
	// N is the number of hosts to deploy to (0 means all hosts).
	N int `json:"n"`
	// Commands are shell commands run on each remote host after the copy.
	Commands []string `json:"commands"`
}

func main() {
	m := flag.String("m", "manifest.json", "path to the manifest file")
	h := flag.String("h", "hosts.txt", "path to the file containing hosts")
	parallel := flag.Int("parallel", remote.DefaultParallelism, "maximum number of concurrent SSH/SCP operations")
	flag.Parse()

	manifest, err := parseManifest(*m)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing manifest: %v\n", err)
		os.Exit(1)
	}

	if len(manifest.Files) > 0 {
		fmt.Printf("Copying %d file(s) to %s...\n", len(manifest.Files), manifest.NFS)
		type copyResult struct {
			file string
			err  error
		}
		results := make([]copyResult, len(manifest.Files))
		indexes := make([]int, len(manifest.Files))
		for i := range manifest.Files {
			indexes[i] = i
		}
		remote.RunBounded(indexes, *parallel, func(i int) {
			results[i] = copyResult{
				file: manifest.Files[i],
				err:  scpFile(manifest.Files[i], manifest.NFS),
			}
		})
		for _, result := range results {
			if result.err != nil {
				fmt.Fprintf(os.Stderr, "error copying %s: %v\n", result.file, result.err)
				os.Exit(1)
			}
		}
	}

	hosts, err := readHosts(*h)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading hosts: %v\n", err)
		os.Exit(1)
	}

	if manifest.N > 0 && manifest.N < len(hosts) {
		hosts = hosts[:manifest.N]
	}

	fmt.Printf("Deploying to %d host(s)...\n", len(hosts))
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		failed int
	)
	sem := make(chan struct{}, remote.NormalizeParallelism(*parallel, len(hosts)))
	for _, host := range hosts {
		wg.Add(1)
		sem <- struct{}{}
		go func(h string) {
			defer wg.Done()
			defer func() { <-sem }()
			out, err := sshRun(h, manifest.Commands)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				fmt.Fprintf(os.Stderr, "[%s] ERROR: %v\n%s\n", h, err, out)
				failed++
			} else {
				fmt.Printf("[%s] OK\n%s", h, out)
			}
		}(host)
	}
	wg.Wait()

	if failed > 0 {
		fmt.Fprintf(os.Stderr, "%d host(s) failed\n", failed)
		os.Exit(1)
	}
	fmt.Println("Done.")
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

// scpFile copies a local file to the remote NFS destination.
func scpFile(src, dst string) error {
	return remote.RunSCP(src, dst, remote.DefaultSCPTimeout)
}

// sshRun connects to host and executes all commands in a single SSH session,
// joined with " && " so that a failure in any command stops execution.
// Returns the combined stdout+stderr output and any error.
func sshRun(host string, commands []string) (string, error) {
	return remote.RunSSH(host, commands, remote.DefaultSSHTimeout)
}
