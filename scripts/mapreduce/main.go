// Command mapreduce is the client-side orchestrator for the distributed MapReduce job.
//
// Workflow:
//
//  1. Read N hosts from hosts.txt (or -hosts flag)
//  2. Build the worker binary
//  3. SCP the binary to each node
//  4. SSH each node to start the worker HTTP server (nohup)
//  5. Wait for all workers to pass the health check
//  6. Split the input file into 64 MB chunks and POST each to a worker
//  7. Broadcast POST /map  (with peer list) — wait for all to finish
//  8. Broadcast POST /reduce               — wait for all to finish
//  9. GET /result from every worker, merge-sort, write to output file
// 10. SSH cleanup: kill worker processes
//
// Usage:
//
//	mapreduce -hosts hosts.txt -input data.txt -output result.txt [-n 10] [-port 9090]
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	chunkSize    = 64 * 1024 * 1024 // 64 MB
	workerBinary = "/tmp/mr-worker"
	healthRetry  = 30
	healthDelay  = 2 * time.Second
)

// HTTP clients with appropriate timeouts for each operation class.
var (
	shortClient = &http.Client{Timeout: 30 * time.Second}  // health checks
	dataClient  = &http.Client{Timeout: 30 * time.Minute}  // data upload/load
	longClient  = &http.Client{Timeout: 60 * time.Minute}  // map, reduce, collect
)

// KeyValue mirrors the worker type for result parsing.
type KeyValue struct {
	Key   string
	Value string
}

// mapRequest is sent to /map on each worker.
type mapRequest struct {
	ID    int      `json:"id"`
	Peers []string `json:"peers"`
}

func main() {
	hostsFile := flag.String("hosts", "../hosts.txt", "path to hosts file")
	inputFile := flag.String("input", "", "path to input data file")
	inputDir := flag.String("input-dir", "", "directory of WET files on NFS (alternative to -input)")
	outputFile := flag.String("output", "result.txt", "path for merged output file")
	n := flag.Int("n", 0, "number of workers (0 = all hosts)")
	filesLimit := flag.Int("files-limit", 0, "cap the number of WET files used from -input-dir (0 = all)")
	port := flag.String("port", "9090", "worker HTTP port")
	flag.Parse()

	if *inputFile == "" && *inputDir == "" {
		log.Fatal("one of -input or -input-dir is required")
	}

	hosts, err := readHosts(*hostsFile)
	if err != nil {
		log.Fatalf("read hosts: %v", err)
	}
	nWorkers := len(hosts)
	peers := make([]string, nWorkers)
	for i, h := range hosts {
		peers[i] = h + ":" + *port
	}

	log.Printf("candidate workers: %d (target -n=%d)", nWorkers, *n)

	// ── Step 1: build worker binary ────────────────────────────────────────
	log.Println("building worker binary…")
	if err := buildWorker(); err != nil {
		log.Fatalf("build failed: %v", err)
	}

	// ── Step 2: SCP binary to every node ───────────────────────────────────
	log.Println("deploying worker binary…")
	hosts, peers, err = deployWorker(hosts, peers, *port)
	if err != nil {
		log.Fatalf("deploy failed: %v", err)
	}
	log.Printf("deployed to %d workers", len(hosts))

	// ── Step 3: health-check all workers ───────────────────────────────────
	log.Println("waiting for workers to become ready…")
	hosts, peers, err = waitHealthy(hosts, peers)
	if err != nil {
		log.Fatalf("health check: %v", err)
	}
	log.Printf("%d workers ready", len(hosts))

	// Trim survivors down to the target -n now that we know who is healthy.
	if *n > 0 && *n < len(hosts) {
		hosts = hosts[:*n]
		peers = peers[:*n]
		log.Printf("using first %d healthy workers (target -n=%d)", len(hosts), *n)
	}

	// ── Step 4: split input and distribute data ─────────────────────────────
	log.Println("splitting and distributing input…")
	if *inputDir != "" {
		if err := distributeFiles(*inputDir, *filesLimit, hosts, peers); err != nil {
			log.Fatalf("distribute files: %v", err)
		}
	} else {
		if err := distributeData(*inputFile, peers); err != nil {
			log.Fatalf("distribute data: %v", err)
		}
	}

	// ── Step 5: map phase ──────────────────────────────────────────────────
	log.Println("starting map phase…")
	if err := broadcastMap(peers); err != nil {
		log.Fatalf("map phase: %v", err)
	}
	log.Println("map phase done")

	// ── Step 6: reduce phase ──────────────────────────────────────────────────
	log.Println("starting reduce phase…")
	if err := broadcastPost(longClient, peers, "/reduce", nil); err != nil {
		log.Fatalf("reduce phase: %v", err)
	}
	log.Println("reduce phase done")

	// ── Step 7: collect results ────────────────────────────────────────────
	log.Println("collecting results…")
	if err := collectResults(peers, *outputFile); err != nil {
		log.Fatalf("collect: %v", err)
	}
	log.Printf("results written to %s", *outputFile)

	// ── Step 9: cleanup ────────────────────────────────────────────────────
	log.Println("cleaning up workers…")
	cleanupWorkers(hosts)
}

// ── helpers ─────────────────────────────────────────────────────────────────

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

// buildWorker compiles the worker binary for linux/amd64 into a temp file.
// It expects to be run via `go run ./mapreduce` from the scripts/ directory,
// so the worker source is at ./worker relative to the current working directory.
func buildWorker() error {
	// When run via `go run ./mapreduce` the cwd is scripts/.
	// When run as a compiled binary the cwd may differ; accept a -worker-src flag
	// or fall back to locating the source relative to os.Executable.
	workerSrc := "./worker" // relative to scripts/
	if _, err := os.Stat(workerSrc); err != nil {
		// Try relative to the binary's directory
		exe, _ := os.Executable()
		workerSrc = filepath.Join(filepath.Dir(exe), "..", "worker")
	}

	out := "/tmp/mr-worker-build"
	cmd := exec.Command("go", "build", "-o", out, workerSrc)
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// deployWorker SCPs the worker binary to each host and starts the HTTP server.
// It returns the subset of hosts and peers that were successfully deployed.
// An error is returned only if every host fails.
func deployWorker(hosts, peers []string, port string) ([]string, []string, error) {
	type result struct {
		host string
		peer string
		err  error
	}
	results := make([]result, len(hosts))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8) // throttle to avoid ssh gateway overload
	for i, h := range hosts {
		wg.Add(1)
		go func(idx int, host, peer string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[idx] = result{host: host, peer: peer}
			// kill any leftover worker and remove the binary so SCP can overwrite it
			_, _ = sshRun(host, []string{
				"kill $(cat /tmp/mr-worker.pid) 2>/dev/null || true",
				"rm -f /tmp/mr-worker /tmp/mr-worker.pid /tmp/mr-worker.log",
			})
			// copy binary
			if err := scpTo("/tmp/mr-worker-build", host, workerBinary); err != nil {
				results[idx].err = fmt.Errorf("scp to %s: %w", host, err)
				return
			}
			// start server
			startCmd := fmt.Sprintf("chmod +x %s && nohup %s -port %s </dev/null >/tmp/mr-worker.log 2>&1 & echo $! > /tmp/mr-worker.pid",
				workerBinary, workerBinary, port)
			_, err := sshRun(host, []string{startCmd})
			if err != nil {
				results[idx].err = fmt.Errorf("ssh start on %s: %w", host, err)
			}
		}(i, h, peers[i])
	}
	wg.Wait()

	var goodHosts, goodPeers []string
	for _, r := range results {
		if r.err != nil {
			log.Printf("[deploy] skipping %s: %v", r.host, r.err)
		} else {
			goodHosts = append(goodHosts, r.host)
			goodPeers = append(goodPeers, r.peer)
		}
	}
	if len(goodHosts) == 0 {
		return nil, nil, fmt.Errorf("all %d hosts failed to deploy", len(hosts))
	}
	return goodHosts, goodPeers, nil
}

func scpTo(src, host, dst string) error {
	cmd := exec.Command("scp",
		"-o", "StrictHostKeyChecking=no",
		"-o", "BatchMode=yes",
		"-o", "ConnectTimeout=10",
		src, host+":"+dst)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, out)
	}
	return nil
}

func sshRun(host string, commands []string) (string, error) {
	cmd := exec.Command(
		"ssh",
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

// waitHealthy polls GET /health on all peers until they all return 200 or
// healthRetry attempts are exhausted. It returns the subset of hosts and peers
// that became ready. An error is returned only if no peer became ready.
func waitHealthy(hosts, peers []string) ([]string, []string, error) {
	ready := make([]bool, len(peers))
	for range healthRetry {
		allReady := true
		for i, p := range peers {
			if ready[i] {
				continue
			}
			resp, err := shortClient.Get("http://" + p + "/health") //nolint:gosec
			if err == nil && resp.StatusCode == http.StatusOK {
				resp.Body.Close()
				ready[i] = true
			} else if err == nil {
				resp.Body.Close()
			}
			if !ready[i] {
				allReady = false
			}
		}
		if allReady {
			break
		}
		time.Sleep(healthDelay)
	}

	var goodHosts, goodPeers []string
	for i, p := range peers {
		if ready[i] {
			goodHosts = append(goodHosts, hosts[i])
			goodPeers = append(goodPeers, p)
		} else {
			log.Printf("[health] skipping %s: not ready after %d attempts", p, healthRetry)
		}
	}
	if len(goodPeers) == 0 {
		return nil, nil, fmt.Errorf("no workers became ready after %d attempts", healthRetry)
	}
	return goodHosts, goodPeers, nil
}

// distributeFiles lists WET files in dir, assigns them round-robin to workers,
// and tells each worker to load its files via POST /load (read directly from NFS).
// If dir is not locally accessible, it lists files by SSH-ing into the first worker.
func distributeFiles(dir string, filesLimit int, hosts []string, peers []string) error {
	files, err := listWETFiles(dir, hosts)
	if err != nil {
		return fmt.Errorf("list WET files: %w", err)
	}
	if filesLimit > 0 && filesLimit < len(files) {
		files = files[:filesLimit]
	}

	filesByWorker := make([][]string, len(peers))
	for i, f := range files {
		filesByWorker[i%len(peers)] = append(filesByWorker[i%len(peers)], f)
	}
	log.Printf("found %d WET files, distributing across %d workers", len(files), len(peers))

	var wg sync.WaitGroup
	errs := make([]error, len(peers))
	for idx, peer := range peers {
		wg.Add(1)
		go func(workerIdx int, p string, workerFiles []string) {
			defer wg.Done()
			body, _ := json.Marshal(workerFiles)
			errs[workerIdx] = postJSON(dataClient, p, "/load", body)
		}(idx, peer, filesByWorker[idx])
	}
	wg.Wait()
	return firstErr(errs)
}

// listWETFiles returns all .wet/.wet.gz paths in dir. It first tries a local
// directory read; if the NFS is not mounted locally it falls back to an SSH
// listing on the first reachable worker host.
func listWETFiles(dir string, hosts []string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err == nil {
		var files []string
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if strings.HasSuffix(name, ".wet.gz") || strings.HasSuffix(name, ".wet") {
				files = append(files, filepath.Join(dir, name))
			}
		}
		if len(files) == 0 {
			return nil, fmt.Errorf("no .wet or .wet.gz files found in %s", dir)
		}
		return files, nil
	}

	// NFS not mounted locally — ask a worker via SSH.
	log.Printf("local access to %s unavailable (%v), listing via SSH", dir, err)
	for _, host := range hosts {
		out, sshErr := sshRun(host, []string{
			fmt.Sprintf("find %s -maxdepth 1 \\( -name '*.wet.gz' -o -name '*.wet' \\) 2>/dev/null | sort", dir),
		})
		if sshErr != nil {
			log.Printf("SSH list on %s failed: %v", host, sshErr)
			continue
		}
		var files []string
		for line := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
			if line = strings.TrimSpace(line); line != "" {
				files = append(files, line)
			}
		}
		if len(files) > 0 {
			return files, nil
		}
	}
	return nil, fmt.Errorf("could not list WET files in %s via SSH on any worker", dir)
}

// distributeData splits the input file into ≤64 MB chunks and POSTs each to a worker.
// If there are more workers than chunks the extra workers receive an empty payload.
func distributeData(inputFile string, peers []string) error {
	f, err := os.Open(inputFile)
	if err != nil {
		return err
	}
	defer f.Close()

	chunks, err := splitIntoChunks(f, chunkSize)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	errs := make([]error, len(peers))
	for i, peer := range peers {
		wg.Add(1)
		go func(idx int, p string, chunk []byte) {
			defer wg.Done()
			errs[idx] = postRaw(dataClient, p, "/data", chunk)
		}(i, peer, chunkForWorker(chunks, i))
	}
	wg.Wait()
	return firstErr(errs)
}

// splitIntoChunks reads r and returns byte slices of at most maxSize bytes,
// always splitting on newline boundaries.
func splitIntoChunks(r io.Reader, maxSize int) ([][]byte, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var chunks [][]byte
	for len(data) > 0 {
		if len(data) <= maxSize {
			chunks = append(chunks, data)
			break
		}
		// find last newline within maxSize bytes
		cut := maxSize
		for cut > 0 && data[cut-1] != '\n' {
			cut--
		}
		if cut == 0 {
			cut = maxSize // no newline found; hard cut
		}
		chunks = append(chunks, data[:cut])
		data = data[cut:]
	}
	return chunks, nil
}

// chunkForWorker returns the chunk for worker i, or an empty slice if i ≥ len(chunks).
func chunkForWorker(chunks [][]byte, i int) []byte {
	if i < len(chunks) {
		return chunks[i]
	}
	return []byte{}
}

// broadcastMap sends POST /map to all peers with the full peer list and each worker's ID.
func broadcastMap(peers []string) error {
	var wg sync.WaitGroup
	errs := make([]error, len(peers))
	for i, peer := range peers {
		wg.Add(1)
		go func(idx int, p string) {
			defer wg.Done()
			req := mapRequest{ID: idx, Peers: peers}
			body, _ := json.Marshal(req)
			errs[idx] = postJSON(longClient, p, "/map", body)
		}(i, peer)
	}
	wg.Wait()
	return firstErr(errs)
}

// broadcastPost sends a POST with an optional body to path on every peer concurrently.
func broadcastPost(client *http.Client, peers []string, path string, body []byte) error {
	var wg sync.WaitGroup
	errs := make([]error, len(peers))
	for i, peer := range peers {
		wg.Add(1)
		go func(idx int, p string) {
			defer wg.Done()
			errs[idx] = postRaw(client, p, path, body)
		}(i, peer)
	}
	wg.Wait()
	return firstErr(errs)
}

// collectResults fetches /result from every worker, merges word counts, sorts
// by descending count then alphabetically by key, and writes to outputFile.
func collectResults(peers []string, outputFile string) error {
	type peerResult struct {
		kvs []KeyValue
		err error
	}
	results := make([]peerResult, len(peers))
	var wg sync.WaitGroup
	for i, peer := range peers {
		wg.Add(1)
		go func(idx int, p string) {
			defer wg.Done()
			kvs, err := fetchResult(p)
			results[idx] = peerResult{kvs: kvs, err: err}
		}(i, peer)
	}
	wg.Wait()

	merged := make(map[string]int)
	for _, r := range results {
		if r.err != nil {
			return r.err
		}
		for _, kv := range r.kvs {
			v, _ := strconv.Atoi(kv.Value)
			merged[kv.Key] += v
		}
	}

	// sort: descending count, then alphabetical key
	type entry struct {
		key   string
		count int
	}
	entries := make([]entry, 0, len(merged))
	for k, v := range merged {
		entries = append(entries, entry{k, v})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].count != entries[j].count {
			return entries[i].count > entries[j].count
		}
		return entries[i].key < entries[j].key
	})

	var buf bytes.Buffer
	for _, e := range entries {
		fmt.Fprintf(&buf, "%s\t%d\n", e.key, e.count)
	}
	return os.WriteFile(outputFile, buf.Bytes(), 0o644)
}

// fetchResult calls GET /result on a peer and returns parsed KV pairs.
func fetchResult(peer string) ([]KeyValue, error) {
	resp, err := longClient.Get("http://" + peer + "/result") //nolint:gosec
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var kvs []KeyValue
	for line := range strings.SplitSeq(strings.TrimSpace(string(body)), "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			continue
		}
		kvs = append(kvs, KeyValue{Key: parts[0], Value: parts[1]})
	}
	return kvs, nil
}

// cleanupWorkers kills the worker processes on each host via SSH.
func cleanupWorkers(hosts []string) {
	var wg sync.WaitGroup
	for _, h := range hosts {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			_, err := sshRun(host, []string{
				"kill $(cat /tmp/mr-worker.pid) 2>/dev/null || true",
				"rm -f /tmp/mr-worker.pid /tmp/mr-worker.log " + workerBinary,
				"rm -rf /tmp/mr-worker-*",
			})
			if err != nil {
				log.Printf("[%s] cleanup warning: %v", host, err)
			}
		}(h)
	}
	wg.Wait()
}

// ── HTTP helpers ─────────────────────────────────────────────────────────────

func postRaw(client *http.Client, peer, path string, body []byte) error {
	ct := "application/octet-stream"
	if len(body) == 0 {
		body = []byte{}
	}
	resp, err := client.Post("http://"+peer+path, ct, bytes.NewReader(body)) //nolint:gosec
	if err != nil {
		return fmt.Errorf("POST %s%s: %w", peer, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("POST %s%s returned %d: %s", peer, path, resp.StatusCode, b)
	}
	return nil
}

func postJSON(client *http.Client, peer, path string, body []byte) error {
	resp, err := client.Post("http://"+peer+path, "application/json", bytes.NewReader(body)) //nolint:gosec
	if err != nil {
		return fmt.Errorf("POST %s%s: %w", peer, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("POST %s%s returned %d: %s", peer, path, resp.StatusCode, b)
	}
	return nil
}

func firstErr(errs []error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
