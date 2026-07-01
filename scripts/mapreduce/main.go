// Command mapreduce is the client-side orchestrator for the distributed MapReduce job.
//
// Workflow:
//
//  1. Read N hosts from hosts.txt (or -hosts flag)
//  2. Build the worker binary
//  3. SCP the binary to the initial active nodes
//  4. SSH those nodes to start the worker HTTP server (nohup)
//  5. Wait for the active workers to pass the health check
//  6. Split a local input file into 64 MB chunks or resolve Common Crawl WET URLs
//     and assign them to workers
//  7. Broadcast POST /map  (with peer list) — wait for all to finish
//  8. Broadcast POST /reduce               — wait for all to finish
//  9. GET /result from every worker, merge-sort, write to output file
//
// 10. SSH cleanup: kill worker processes on every touched host
//
// Usage:
//
//	mapreduce -hosts hosts.txt -job ./jobs/wordcount -input data.txt -output result.txt [-n 10] [-port 9090]
//	mapreduce -hosts hosts.txt -job ./jobs/wordcount -commoncrawl [-crawl CC-MAIN-2026-05] -output result.txt [-n 10] [-port 9090]
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"scripts/internal/remote"
)

const (
	chunkSize    = 64 * 1024 * 1024 // 64 MB
	workerBinary = "/tmp/mr-worker"
	healthRetry  = 6
	healthDelay  = 2 * time.Second
)

// HTTP clients with appropriate timeouts for each operation class.
var (
	shortClient         = &http.Client{Timeout: 5 * time.Second}  // health checks
	dataClient          = &http.Client{Timeout: 30 * time.Minute} // data upload/load
	longClient          = &http.Client{Timeout: 60 * time.Minute} // map, reduce, collect
	commonCrawlIndexURL = "https://index.commoncrawl.org/collinfo.json"
	commonCrawlDataURL  = "https://data.commoncrawl.org"
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
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	hostsFile := flag.String("hosts", "../hosts.txt", "path to hosts file")
	inputFile := flag.String("input", "", "path to input data file")
	commonCrawl := flag.Bool("commoncrawl", false, "read input from the official Common Crawl website instead of a local file")
	cleanupOnly := flag.Bool("cleanup-only", false, "kill workers on the selected hosts and exit")
	crawl := flag.String("crawl", "", "Common Crawl ID to use in -commoncrawl mode (default: latest crawl)")
	outputFile := flag.String("output", "result.txt", "path for merged output file")
	jobPath := flag.String("job", "", "path to Go package directory that exports NewMapper and NewReducer")
	n := flag.Int("n", 0, "number of workers (0 = all hosts)")
	filesLimit := flag.Int("files-limit", 0, "cap the number of Common Crawl WET files used in -commoncrawl mode (0 = all)")
	chunksLimit := flag.Int("chunks-limit", 0, "additional Common Crawl workload cap kept for benchmark compatibility; applied as a second URL-count limit (0 = disabled)")
	port := flag.String("port", "9090", "worker HTTP port")
	maxAttempts := flag.Int("max-attempts", 4, "per-slot retry attempts before failing the job")
	healthInterval := flag.Duration("health-interval", 5*time.Second, "active /health poll interval during long phases")
	backoffInitial := flag.Duration("backoff-initial", 250*time.Millisecond, "initial retry backoff (doubles up to 5s)")
	parallel := flag.Int("parallel", remote.DefaultParallelism, "maximum number of concurrent SSH/SCP or readiness-check operations")
	workerPprof := flag.Bool("worker-pprof", false, "start remote workers with -pprof (exposes /debug/pprof/ for bottleneck analysis)")
	nfsDeploy := flag.Bool("nfs-deploy", false, "deploy by copying the binary ONCE to HOME (NFS) then starting workers via SSH from that shared path (rubric 1-SCP+N-SSH pattern)")
	flag.Parse()

	if *cleanupOnly {
		hosts, err := readHosts(*hostsFile)
		if err != nil {
			return fmt.Errorf("read hosts: %w", err)
		}
		cleanupWorkers(hosts, *parallel)
		return nil
	}

	switch {
	case strings.TrimSpace(*jobPath) == "":
		return fmt.Errorf("missing required -job <path> flag")
	case *inputFile == "" && !*commonCrawl:
		return fmt.Errorf("choose exactly one input mode: -input <file> or -commoncrawl [-crawl CC-MAIN-...]")
	case *inputFile != "" && *commonCrawl:
		return fmt.Errorf("-input and -commoncrawl are mutually exclusive")
	}

	hosts, err := readHosts(*hostsFile)
	if err != nil {
		return fmt.Errorf("read hosts: %w", err)
	}
	nWorkers := len(hosts)

	var buildDur, deployDur, inputPrepDur time.Duration
	var loadDur, mapDur, reduceDur, collectDur, computeDur time.Duration
	var cleanupStart time.Time

	var coord *coordinator
	var deployedHosts []string
	cancel := func() {}
	defer func() {
		cancel()
		if len(deployedHosts) == 0 {
			return
		}
		log.Println("cleaning up workers…")
		cleanupStart = time.Now()
		hostsForCleanup := append([]string(nil), deployedHosts...)
		if coord != nil {
			hostsForCleanup = peerHosts(coord.hostsForCleanup())
		}
		cleanupWorkers(hostsForCleanup, *parallel)
		cleanupDur := time.Since(cleanupStart)
		log.Printf("TIMING_CLEANUP hosts=%d cleanup_seconds=%.3f", len(hostsForCleanup), cleanupDur.Seconds())
	}()

	log.Printf("candidate workers: %d (target -n=%d)", nWorkers, *n)

	target := *n
	if target <= 0 || target > nWorkers {
		target = nWorkers
	}

	// ── Step 1: build worker binary ────────────────────────────────────────
	log.Println("building worker binary…")
	buildStart := time.Now()
	workerBuildPath, err := buildWorker(*jobPath)
	if err != nil {
		return fmt.Errorf("build failed: %w", err)
	}
	buildDur = time.Since(buildStart)
	defer os.Remove(workerBuildPath)

	// ── Step 2: deploy only the initial active workers ─────────────────────
	log.Printf("deploying up to %d initial worker(s)…", target)
	deployStart := time.Now()
	var activeHosts, activePeers, sparePeers []string
	if *nfsDeploy {
		log.Println("[nfs-deploy] using 1-SCP-to-HOME + N-SSH pattern")
		activeHosts, activePeers, sparePeers, err = deployWorkersNFS(hosts, *port, workerBuildPath, target, *workerPprof)
	} else {
		activeHosts, activePeers, sparePeers, err = deployInitialWorkers(hosts, *port, workerBuildPath, target, *workerPprof)
	}
	if err != nil {
		return fmt.Errorf("deploy failed: %w", err)
	}
	deployDur = time.Since(deployStart)
	deployedHosts = append([]string(nil), activeHosts...)
	log.Printf("%d workers ready", len(activePeers))
	log.Printf("using %d active slots + %d cold spares (target -n=%d)",
		len(activePeers), len(sparePeers), *n)

	activateSpare := func(peer string) error {
		host, peerPort, found := strings.Cut(peer, ":")
		if !found {
			host = peer
			peerPort = *port
		}
		if *nfsDeploy {
			// In NFS mode the binary is already on the shared HOME; just SSH-start.
			if err := sshStartNFS(host, peerPort, *workerPprof); err != nil {
				return err
			}
			readyHosts, _, hErr := waitHealthy([]string{host}, []string{peer}, 1)
			if hErr != nil || len(readyHosts) == 0 {
				cleanupWorkers([]string{host}, 1)
				return fmt.Errorf("%s did not become ready (nfs-deploy)", peer)
			}
			return nil
		}
		return activateWorker(host, peer, workerBuildPath, *workerPprof)
	}

	// Assemble the coordinator.
	coord = newCoordinator(activePeers, sparePeers, *maxAttempts, *backoffInitial, *healthInterval, activateSpare)

	// ── Step 4: assign input to slots ───────────────────────────────────────
	log.Println("preparing slot inputs…")
	inputPrepStart := time.Now()
	if *commonCrawl {
		resolvedCrawl, urls, err := resolveCommonCrawlURLs(*crawl, *filesLimit, *chunksLimit)
		if err != nil {
			return fmt.Errorf("resolve Common Crawl inputs: %w", err)
		}
		log.Printf("resolved %d Common Crawl WET URLs from %s, distributing round-robin across %d slots",
			len(urls), resolvedCrawl, len(coord.slots))
		if len(urls) < len(coord.slots) {
			log.Printf("WARNING: only %d WET URLs for %d slots — %d worker(s) will get empty input; "+
				"raise -files-limit/-chunks-limit for a meaningful high-N measurement",
				len(urls), len(coord.slots), len(coord.slots)-len(urls))
		}
		assignURLsRoundRobin(coord.slots, urls)
		logSlotURLBalance(coord.slots)
	} else {
		f, err := os.Open(*inputFile)
		if err != nil {
			return fmt.Errorf("open input: %w", err)
		}
		chunks, err := splitIntoChunks(f, chunkSize)
		f.Close()
		if err != nil {
			return fmt.Errorf("split input: %w", err)
		}
		for i, s := range coord.slots {
			s.chunk = chunkForWorker(chunks, i)
		}
	}
	inputPrepDur = time.Since(inputPrepStart)

	// ── Step 5: drive all slots through map → reduce → result ───────────────
	// Background health watcher cancels in-flight calls on dead hosts.
	ctx, stopHealth := context.WithCancel(context.Background())
	cancel = stopHealth
	go coord.watchHealth(ctx)

	// Load data onto workers BEFORE the compute timer starts so that the
	// /data upload (or /load) is excluded from the measured compute time.
	log.Println("loading data onto workers…")
	loadStart := time.Now()
	if err := coord.drive(ctx, sLoaded); err != nil {
		return fmt.Errorf("load phase: %w", err)
	}
	loadDur = time.Since(loadStart)
	log.Println("load phase done")

	log.Println("starting map phase…")
	computeStart := time.Now()
	mapStart := computeStart
	if err := coord.drive(ctx, sMapped); err != nil {
		return fmt.Errorf("map phase: %w", err)
	}
	mapDur = time.Since(mapStart)
	log.Println("map phase done")

	log.Println("starting reduce phase…")
	reduceStart := time.Now()
	if err := coord.drive(ctx, sReduced); err != nil {
		return fmt.Errorf("reduce phase: %w", err)
	}
	reduceDur = time.Since(reduceStart)
	log.Println("reduce phase done")

	log.Println("collecting results…")
	collectStart := time.Now()
	if err := coord.drive(ctx, sDone); err != nil {
		return fmt.Errorf("collect: %w", err)
	}
	if err := coord.mergeResults(*outputFile); err != nil {
		return fmt.Errorf("merge: %w", err)
	}
	collectDur = time.Since(collectStart)
	computeDur = time.Since(computeStart)
	log.Printf("results written to %s", *outputFile)

	coordMetrics := coord.metricsSnapshot()
	ioDur := inputPrepDur + loadDur + collectDur
	syncWaitDur := coordMetrics.BackoffWait + coordMetrics.SettleWait
	networkDur := deployDur + coordMetrics.RemoteTotal()

	// Machine-parseable timing line for benchmarking (compute phases only).
	log.Printf("TIMING nodes=%d map_seconds=%.3f reduce_seconds=%.3f collect_seconds=%.3f compute_seconds=%.3f",
		len(coord.slots), mapDur.Seconds(), reduceDur.Seconds(), collectDur.Seconds(), computeDur.Seconds())
	log.Printf("TIMING_BREAKDOWN nodes=%d build_seconds=%.3f deployment_seconds=%.3f input_prep_seconds=%.3f load_seconds=%.3f map_seconds=%.3f reduce_seconds=%.3f collect_seconds=%.3f io_seconds=%.3f sync_wait_seconds=%.3f network_seconds=%.3f remote_load_seconds=%.3f remote_map_seconds=%.3f remote_reduce_seconds=%.3f remote_result_seconds=%.3f compute_seconds=%.3f",
		len(coord.slots),
		buildDur.Seconds(), deployDur.Seconds(), inputPrepDur.Seconds(), loadDur.Seconds(),
		mapDur.Seconds(), reduceDur.Seconds(), collectDur.Seconds(),
		ioDur.Seconds(), syncWaitDur.Seconds(), networkDur.Seconds(),
		coordMetrics.RemoteLoad.Seconds(), coordMetrics.RemoteMap.Seconds(), coordMetrics.RemoteReduce.Seconds(), coordMetrics.RemoteResult.Seconds(),
		computeDur.Seconds())

	// Stop the health watcher before cleanup so it doesn't race with kills.
	cancel()
	return nil
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

// buildWorker compiles a worker binary for linux/amd64 into a temp file.
// It injects the selected job package into a temporary worker package copy so
// that the deployed binary has no fallback/default job.
func buildWorker(jobPath string) (string, error) {
	scriptsDir, err := resolveScriptsDir()
	if err != nil {
		return "", err
	}
	jobImportPath, err := resolveJobImportPath(scriptsDir, jobPath)
	if err != nil {
		return "", err
	}

	workerSrc := filepath.Join(scriptsDir, "worker")
	buildDir, err := os.MkdirTemp(scriptsDir, ".mr-worker-build-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(buildDir)

	if err := copyWorkerPackage(workerSrc, buildDir); err != nil {
		return "", err
	}
	if err := writeInjectedJobBinding(buildDir, jobImportPath); err != nil {
		return "", err
	}

	outFile, err := os.CreateTemp("", "mr-worker-build-*")
	if err != nil {
		return "", err
	}
	out := outFile.Name()
	if err := outFile.Close(); err != nil {
		return "", err
	}
	if err := os.Remove(out); err != nil {
		return "", err
	}

	buildTarget := "./" + filepath.Base(buildDir)
	cmd := exec.Command("go", "build", "-o", out, buildTarget)
	cmd.Dir = scriptsDir
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", err
	}
	return out, nil
}

func deployInitialWorkers(hosts []string, port, workerBuildPath string, target int, withPprof bool) ([]string, []string, []string, error) {
	activeHosts := make([]string, 0, target)
	activePeers := make([]string, 0, target)

	for i, host := range hosts {
		if len(activePeers) == target {
			return activeHosts, activePeers, peersForHosts(hosts[i:], port), nil
		}
		peer := host + ":" + port
		if err := activateWorker(host, peer, workerBuildPath, withPprof); err != nil {
			log.Printf("[startup] skipping %s: %v", peer, err)
			continue
		}
		activeHosts = append(activeHosts, host)
		activePeers = append(activePeers, peer)
	}
	if len(activePeers) < target {
		return nil, nil, nil, fmt.Errorf("need at least %d healthy workers but only %d are ready", target, len(activePeers))
	}
	return activeHosts, activePeers, nil, nil
}

func activateWorker(host, peer, workerBuildPath string, withPprof bool) error {
	if err := deployWorker(host, peer, workerBuildPath, withPprof); err != nil {
		return err
	}
	readyHosts, _, err := waitHealthy([]string{host}, []string{peer}, 1)
	if err == nil && len(readyHosts) == 1 {
		return nil
	}
	cleanupWorkers([]string{host}, 1)
	if err != nil {
		return err
	}
	return fmt.Errorf("%s did not become ready", peer)
}

// deployWorkersNFS implements the rubric-preferred 1-SCP-to-HOME + N-SSH pattern:
// the binary is SCPed ONCE to the first reachable host's HOME directory (which
// sits on the lab NFS, so every machine can read it at ~/mr-worker), then each
// of the N hosts is started via SSH without any additional SCP.
func deployWorkersNFS(hosts []string, port, workerBuildPath string, target int, withPprof bool) ([]string, []string, []string, error) {
	// Stage 1: SCP the binary once to the first reachable host's NFS home.
	stagingHost := ""
	for _, h := range hosts {
		if err := scpTo(workerBuildPath, h, "~/mr-worker"); err != nil {
			log.Printf("[nfs-deploy] scp to %s failed: %v — trying next host", h, err)
			continue
		}
		if _, err := sshRun(h, []string{"chmod +x ~/mr-worker"}); err != nil {
			log.Printf("[nfs-deploy] chmod on %s failed: %v — trying next host", h, err)
			continue
		}
		stagingHost = h
		break
	}
	if stagingHost == "" {
		return nil, nil, nil, fmt.Errorf("nfs-deploy: could not stage binary to any host's HOME")
	}
	log.Printf("[nfs-deploy] binary staged to %s:~/mr-worker (HOME is on NFS, visible to all nodes)", stagingHost)

	// Stage 2: SSH to the first N reachable hosts and start from ~/mr-worker.
	activeHosts := make([]string, 0, target)
	activePeers := make([]string, 0, target)

	for i, host := range hosts {
		if len(activePeers) == target {
			return activeHosts, activePeers, peersForHosts(hosts[i:], port), nil
		}
		peer := host + ":" + port
		if err := ensureRemotePortFree(host, port); err != nil {
			log.Printf("[nfs-deploy] port preflight failed on %s: %v — skipping", host, err)
			continue
		}
		if err := sshStartNFS(host, port, withPprof); err != nil {
			log.Printf("[nfs-deploy] ssh start failed on %s: %v — skipping", host, err)
			continue
		}
		readyHosts, _, err := waitHealthy([]string{host}, []string{peer}, 1)
		if err != nil || len(readyHosts) == 0 {
			log.Printf("[nfs-deploy] health check failed on %s — skipping", host)
			cleanupWorkers([]string{host}, 1)
			continue
		}
		activeHosts = append(activeHosts, host)
		activePeers = append(activePeers, peer)
	}
	if len(activePeers) < target {
		return nil, nil, nil, fmt.Errorf("nfs-deploy: need at least %d workers but only %d ready", target, len(activePeers))
	}
	return activeHosts, activePeers, nil, nil
}

// sshStartNFS starts the worker from ~/mr-worker (the NFS home copy) without SCP.
func sshStartNFS(host, port string, withPprof bool) error {
	startCmd := fmt.Sprintf(
		"kill $(cat /tmp/mr-worker.pid) 2>/dev/null || true && "+
			"rm -f /tmp/mr-worker.pid /tmp/mr-worker.log && "+
			"nohup ~/mr-worker -port %s%s </dev/null >/tmp/mr-worker.log 2>&1 & echo $! > /tmp/mr-worker.pid",
		port, pprofArg(withPprof))
	_, err := sshRun(host, []string{startCmd})
	return err
}

// deployWorker SCPs the worker binary to one host and starts the HTTP server.
func deployWorker(host, peer, workerBuildPath string, withPprof bool) error {
	_, port, found := strings.Cut(peer, ":")
	if !found {
		return fmt.Errorf("peer %q is missing port", peer)
	}
	if err := ensureRemotePortFree(host, port); err != nil {
		return fmt.Errorf("port %s on %s: %w", port, host, err)
	}
	remoteBinary := fmt.Sprintf("%s.deploy-%d", workerBinary, time.Now().UnixNano())
	if err := scpTo(workerBuildPath, host, remoteBinary); err != nil {
		return fmt.Errorf("scp to %s: %w", host, err)
	}
	startCmd := fmt.Sprintf(
		"kill $(cat /tmp/mr-worker.pid) 2>/dev/null || true && "+
			"rm -f /tmp/mr-worker.pid /tmp/mr-worker.log %s && "+
			"mv %s %s && chmod +x %s && nohup %s -port %s%s </dev/null >/tmp/mr-worker.log 2>&1 & echo $! > /tmp/mr-worker.pid",
		workerBinary, remoteBinary, workerBinary, workerBinary, workerBinary, port, pprofArg(withPprof))
	_, err := sshRun(host, []string{startCmd})
	if err != nil {
		_, _ = sshRun(host, []string{"rm -f " + remoteBinary})
		return fmt.Errorf("ssh start on %s: %w", host, err)
	}
	return nil
}

func ensureRemotePortFree(host, port string) error {
	cmd := fmt.Sprintf("bash -lc 'if : </dev/tcp/127.0.0.1/%s; then echo port %s already in use >&2; exit 1; fi'", port, port)
	_, err := sshRun(host, []string{cmd})
	if err != nil {
		return err
	}
	return nil
}

func scpTo(src, host, dst string) error {
	return remote.RunSCP(src, host+":"+dst, remote.DefaultSCPTimeout)
}

// pprofArg returns the extra worker CLI argument enabling profiling endpoints.
func pprofArg(enabled bool) string {
	if enabled {
		return " -pprof"
	}
	return ""
}

func resolveScriptsDir() (string, error) {
	if _, err := os.Stat("go.mod"); err == nil {
		return filepath.Abs(".")
	}

	_, sourceFile, _, ok := runtime.Caller(0)
	if ok {
		candidate := filepath.Join(filepath.Dir(sourceFile), "..")
		if _, err := os.Stat(filepath.Join(candidate, "go.mod")); err == nil {
			return filepath.Abs(candidate)
		}
	}

	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("locate executable: %w", err)
	}
	candidate := filepath.Join(filepath.Dir(exe), "..")
	if _, err := os.Stat(filepath.Join(candidate, "go.mod")); err != nil {
		return "", fmt.Errorf("locate scripts directory: %w", err)
	}
	return filepath.Abs(candidate)
}

func resolveJobImportPath(scriptsDir, jobPath string) (string, error) {
	jobPath = strings.TrimSpace(jobPath)
	if jobPath == "" {
		return "", fmt.Errorf("job path is required")
	}

	absJobPath, err := filepath.Abs(jobPath)
	if err != nil {
		return "", fmt.Errorf("resolve job path %q: %w", jobPath, err)
	}
	info, err := os.Stat(absJobPath)
	if err != nil {
		return "", fmt.Errorf("stat job path %q: %w", jobPath, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("job path %q is not a directory", jobPath)
	}

	rel, err := filepath.Rel(scriptsDir, absJobPath)
	if err != nil {
		return "", fmt.Errorf("relativize job path %q: %w", jobPath, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("job path %q must be inside %s", absJobPath, scriptsDir)
	}

	matches, err := filepath.Glob(filepath.Join(absJobPath, "*.go"))
	if err != nil {
		return "", fmt.Errorf("list Go files in %q: %w", jobPath, err)
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("job path %q does not contain any Go files", jobPath)
	}

	return "scripts/" + filepath.ToSlash(rel), nil
}

func copyWorkerPackage(srcDir, dstDir string) error {
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || name == "builtin_job.go" {
			continue
		}
		if err := copyFile(filepath.Join(srcDir, name), filepath.Join(dstDir, name)); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, info.Mode().Perm())
}

func writeInjectedJobBinding(buildDir, jobImportPath string) error {
	content := fmt.Sprintf(`package main

import (
	"scripts/mrjob"
	job %q
)

func loadInjectedJob() (mrjob.Mapper, mrjob.Reducer, error) {
	return job.NewMapper(), job.NewReducer(), nil
}
`, jobImportPath)
	return os.WriteFile(filepath.Join(buildDir, "job_binding_generated.go"), []byte(content), 0o644)
}

func sshRun(host string, commands []string) (string, error) {
	return remote.RunSSH(host, commands, remote.DefaultSSHTimeout)
}

// waitHealthy polls GET /health on all peers until they all return 200 or
// healthRetry attempts are exhausted. It returns the subset of hosts and peers
// that became ready. An error is returned only if no peer became ready.
func waitHealthy(hosts, peers []string, parallel int) ([]string, []string, error) {
	return waitHealthyWithConfig(hosts, peers, parallel, healthRetry, healthDelay, shortClient)
}

func waitHealthyWithConfig(hosts, peers []string, parallel, retries int, delay time.Duration, client *http.Client) ([]string, []string, error) {
	ready := make([]bool, len(peers))
	indexes := make([]int, len(peers))
	for i := range peers {
		indexes[i] = i
	}
	for range retries {
		allReady := true
		var mu sync.Mutex
		remote.RunBounded(indexes, parallel, func(i int) {
			if ready[i] {
				return
			}
			resp, err := client.Get("http://" + peers[i] + "/health") //nolint:gosec
			if err == nil {
				if resp.StatusCode == http.StatusOK {
					resp.Body.Close()
					mu.Lock()
					ready[i] = true
					mu.Unlock()
					return
				}
				resp.Body.Close()
			}
			mu.Lock()
			allReady = false
			mu.Unlock()
		})
		if allReady {
			break
		}
		time.Sleep(delay)
	}

	var goodHosts, goodPeers []string
	for i, p := range peers {
		if ready[i] {
			goodHosts = append(goodHosts, hosts[i])
			goodPeers = append(goodPeers, p)
		} else {
			log.Printf("[health] skipping %s: not ready after %d attempts", p, retries)
		}
	}
	if len(goodPeers) == 0 {
		return nil, nil, fmt.Errorf("no workers became ready after %d attempts", retries)
	}
	return goodHosts, goodPeers, nil
}

type commonCrawlCollection struct {
	ID string `json:"id"`
}

func resolveCommonCrawlURLs(crawl string, filesLimit, chunksLimit int) (string, []string, error) {
	resolvedCrawl := crawl
	if resolvedCrawl == "" {
		var err error
		resolvedCrawl, err = resolveLatestCommonCrawl()
		if err != nil {
			return "", nil, err
		}
	}
	urls, err := fetchCommonCrawlWETURLs(resolvedCrawl)
	if err != nil {
		return "", nil, err
	}
	urls = applyCommonCrawlLimits(urls, filesLimit, chunksLimit)
	if len(urls) == 0 {
		return "", nil, fmt.Errorf("no Common Crawl WET URLs selected for crawl %s", resolvedCrawl)
	}
	return resolvedCrawl, urls, nil
}

func resolveLatestCommonCrawl() (string, error) {
	var collections []commonCrawlCollection
	if err := retryCommonCrawl(func() error {
		resp, err := shortClient.Get(commonCrawlIndexURL) //nolint:gosec
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("collinfo returned %d", resp.StatusCode)
		}
		return json.NewDecoder(resp.Body).Decode(&collections)
	}); err != nil {
		return "", fmt.Errorf("fetch crawl index: %w", err)
	}
	if len(collections) == 0 {
		return "", fmt.Errorf("crawl index is empty")
	}
	ids := make([]string, 0, len(collections))
	for _, collection := range collections {
		if collection.ID != "" {
			ids = append(ids, collection.ID)
		}
	}
	if len(ids) == 0 {
		return "", fmt.Errorf("crawl index contained no IDs")
	}
	sort.Strings(ids)
	return ids[len(ids)-1], nil
}

func fetchCommonCrawlWETURLs(crawl string) ([]string, error) {
	manifestURL := fmt.Sprintf("%s/crawl-data/%s/wet.paths.gz", commonCrawlDataURL, crawl)
	var urls []string
	if err := retryCommonCrawl(func() error {
		resp, err := dataClient.Get(manifestURL) //nolint:gosec
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("wet paths returned %d", resp.StatusCode)
		}
		gr, err := gzip.NewReader(resp.Body)
		if err != nil {
			return err
		}
		defer gr.Close()
		body, err := io.ReadAll(gr)
		if err != nil {
			return err
		}
		lines := strings.Split(strings.TrimSpace(string(body)), "\n")
		urls = urls[:0]
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			urls = append(urls, fmt.Sprintf("%s/%s", commonCrawlDataURL, strings.TrimPrefix(line, "/")))
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("fetch %s: %w", manifestURL, err)
	}
	sort.Strings(urls)
	return urls, nil
}

func retryCommonCrawl(fn func() error) error {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return lastErr
}

func applyCommonCrawlLimits(urls []string, filesLimit, chunksLimit int) []string {
	limit := len(urls)
	if filesLimit > 0 && filesLimit < limit {
		limit = filesLimit
	}
	if chunksLimit > 0 && chunksLimit < limit {
		limit = chunksLimit
	}
	return append([]string(nil), urls[:limit]...)
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

// assignURLsRoundRobin distributes Common Crawl URLs across slots so that slot i
// receives URLs i, i+N, i+2N, … (N = len(slots)).
func assignURLsRoundRobin(slots []*slot, urls []string) {
	n := len(slots)
	if n == 0 {
		return
	}
	for _, s := range slots {
		s.urls = nil
	}
	for i, rawURL := range urls {
		slots[i%n].urls = append(slots[i%n].urls, rawURL)
	}
}

// assignChunksRoundRobin distributes chunks across slots so that slot i receives
// chunks i, i+N, i+2N, … (N = len(slots)), concatenating them into slot.chunk.
// A trailing newline is inserted between concatenated chunks so words at chunk
// boundaries are never merged. Slots that receive no chunk get an empty payload.
func assignChunksRoundRobin(slots []*slot, chunks [][]byte) {
	n := len(slots)
	if n == 0 {
		return
	}
	bufs := make([][]byte, n)
	for j, c := range chunks {
		idx := j % n
		bufs[idx] = append(bufs[idx], c...)
		if len(c) > 0 && c[len(c)-1] != '\n' {
			bufs[idx] = append(bufs[idx], '\n')
		}
	}
	for i, s := range slots {
		s.chunk = bufs[i]
	}
}

// logSlotBalance reports the per-slot input byte distribution so workload skew
// (from uneven chunk counts or sub-64 MB tail chunks) is visible in the logs.
func logSlotBalance(slots []*slot) {
	if len(slots) == 0 {
		return
	}
	var total, min, max int
	min = -1
	for _, s := range slots {
		b := len(s.chunk)
		total += b
		if min < 0 || b < min {
			min = b
		}
		if b > max {
			max = b
		}
	}
	avg := total / len(slots)
	log.Printf("slot input balance: total=%d MB, avg=%d MB, min=%d MB, max=%d MB across %d slots",
		total>>20, avg>>20, min>>20, max>>20, len(slots))
}

func logSlotURLBalance(slots []*slot) {
	if len(slots) == 0 {
		return
	}
	total := 0
	min := -1
	max := 0
	for _, s := range slots {
		count := len(s.urls)
		total += count
		if min < 0 || count < min {
			min = count
		}
		if count > max {
			max = count
		}
	}
	avg := float64(total) / float64(len(slots))
	log.Printf("slot URL balance: total=%d, avg=%.2f, min=%d, max=%d across %d slots",
		total, avg, min, max, len(slots))
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
	return writeFileAtomic(outputFile, buf.Bytes(), 0o644)
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}()
	if err := tmp.Chmod(perm); err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return nil
}

// fetchResult calls GET /result on a peer and returns parsed KV pairs.
func fetchResult(peer string) ([]KeyValue, error) {
	return fetchResultCtx(context.Background(), peer)
}

func fetchResultCtx(ctx context.Context, peer string) ([]KeyValue, error) {
	return fetchSlotResultCtx(ctx, peer, 0)
}

func fetchSlotResultCtx(ctx context.Context, peer string, slotID int) ([]KeyValue, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/result?slot=%d", peer, slotID), nil)
	if err != nil {
		return nil, err
	}
	resp, err := longClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("GET %s/result?slot=%d returned %d: %s", peer, slotID, resp.StatusCode, b)
	}
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
func cleanupWorkers(hosts []string, parallel int) {
	remote.RunBounded(hosts, parallel, func(host string) {
		_, err := sshRun(host, []string{
			"kill $(cat /tmp/mr-worker.pid) 2>/dev/null || true",
			"rm -f /tmp/mr-worker.pid /tmp/mr-worker.log " + workerBinary,
			"rm -rf /tmp/mr-worker-*",
		})
		if err != nil {
			log.Printf("[%s] cleanup warning: %v", host, err)
		}
	})
}

func peerHosts(peers []string) []string {
	hosts := make([]string, 0, len(peers))
	for _, peer := range peers {
		host, _, found := strings.Cut(peer, ":")
		if !found {
			host = peer
		}
		hosts = append(hosts, host)
	}
	return hosts
}

func peersForHosts(hosts []string, port string) []string {
	peers := make([]string, 0, len(hosts))
	for _, host := range hosts {
		peers = append(peers, host+":"+port)
	}
	return peers
}

// ── HTTP helpers ─────────────────────────────────────────────────────────────

func postRaw(client *http.Client, peer, path string, body []byte) error {
	return postRawCtx(context.Background(), client, peer, path, body)
}

func postJSON(client *http.Client, peer, path string, body []byte) error {
	return postJSONCtx(context.Background(), client, peer, path, body)
}

// postRawCtx posts body to peer+path with the given context. The context controls
// both connection setup and the in-flight request, so cancelling it aborts a stuck call.
func postRawCtx(ctx context.Context, client *http.Client, peer, path string, body []byte) error {
	if body == nil {
		body = []byte{}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+peer+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build POST %s%s: %w", peer, path, err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := client.Do(req)
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

func postJSONCtx(ctx context.Context, client *http.Client, peer, path string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+peer+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build POST %s%s: %w", peer, path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
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
