// Command worker is an HTTP server that participates in a distributed MapReduce job.
//
// Endpoints:
//
//	GET  /health              — readiness probe
//	POST /data                — receive raw text data chunk
//	POST /load                — download Common Crawl WET URLs into workDir
//	POST /map                 — start map phase
//	GET  /intermediate        — serve map output for a given reducer bucket
//	POST /reduce              — start reduce phase (pulls from peers via /intermediate)
//	GET  /result              — download reduce output
//
// Usage:
//
//	worker -port 9090
package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// pprofEnabled controls whether Go profiling endpoints are registered under
// /debug/pprof/. Set by the -pprof flag in main(); off by default so that
// production benchmark runs are not perturbed.
var pprofEnabled bool

type slotState struct {
	// input data written directly by the client in local-file mode
	inputData string
	// inputFiles are Common Crawl WET files downloaded under workDir.
	inputFiles []string
	peers      []string // "host:port" for all logical slots including self
	output     []KeyValue
}

// state holds runtime data for every logical slot currently assigned to the worker.
type state struct {
	mu    sync.Mutex
	slots map[int]*slotState
}

// server wraps per-instance state and the map/reduce functions so that multiple
// independent workers can run in the same process (useful for testing).
type server struct {
	state    *state
	mapFn    MapFunc
	reduceFn ReduceFunc
	// workDir is the directory where intermediate files are written.
	workDir string
}

// newServer creates a worker server that stores intermediate files under workDir.
func newServer(mapFn MapFunc, reduceFn ReduceFunc, workDir string) *server {
	return &server{
		state:    &state{slots: make(map[int]*slotState)},
		mapFn:    mapFn,
		reduceFn: reduceFn,
		workDir:  workDir,
	}
}

// bucketFile returns the path for the pre-partitioned intermediate file
// for reducer idx out of n total reducers.
func (s *server) bucketFile(slotID, n, idx int) string {
	return filepath.Join(s.workDir, fmt.Sprintf("slot-%d-map-bucket-%d-%d.jsonl", slotID, n, idx))
}

// writeKVLines writes kvs to path as JSON lines, overwriting any existing file.
func writeKVLines(path string, kvs []KeyValue) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, kv := range kvs {
		if err := enc.Encode(kv); err != nil {
			return err
		}
	}
	return nil
}

// readKVFlat reads JSON-line KV pairs from path and returns them as a flat slice.
// Returns nil (not an error) if the file does not exist.
func readKVFlat(path string) ([]KeyValue, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []KeyValue
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024*1024), 64*1024*1024)
	for scanner.Scan() {
		var kv KeyValue
		if err := json.Unmarshal(scanner.Bytes(), &kv); err != nil {
			return nil, fmt.Errorf("readKVFlat %s: %w", path, err)
		}
		out = append(out, kv)
	}
	return out, scanner.Err()
}

// removeIntermediate deletes all pre-partitioned map bucket files for slotID.
func (s *server) removeIntermediate(slotID int) {
	matches, _ := filepath.Glob(filepath.Join(s.workDir, fmt.Sprintf("slot-%d-map-bucket-*.jsonl", slotID)))
	for _, f := range matches {
		_ = os.Remove(f)
	}
}

// handler returns an http.Handler with all worker routes registered.
func (s *server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("POST /data", s.handleData)
	mux.HandleFunc("POST /load", s.handleLoad)
	mux.HandleFunc("POST /map", s.handleMap)
	mux.HandleFunc("GET /intermediate", s.handleIntermediate)
	mux.HandleFunc("POST /reduce", s.handleReduce)
	mux.HandleFunc("GET /result", s.handleResult)
	if pprofEnabled {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	return mux
}

func main() {
	port := flag.String("port", "9090", "port to listen on")
	enablePprof := flag.Bool("pprof", false, "expose Go profiling endpoints under /debug/pprof/")
	flag.Parse()
	pprofEnabled = *enablePprof

	workDir, err := os.MkdirTemp("", "mr-worker-*")
	if err != nil {
		log.Fatalf("create work dir: %v", err)
	}

	mapper, reducer, err := loadInjectedJob()
	if err != nil {
		log.Fatalf("load injected job: %v", err)
	}

	srv := newServer(mapFuncFrom(mapper), reduceFuncFrom(reducer), workDir)
	addr := ":" + *port
	log.Printf("worker listening on %s (workDir: %s)", addr, workDir)
	if err := http.ListenAndServe(addr, srv.handler()); err != nil {
		log.Fatal(err)
	}
}

// mapRequest is the JSON body sent by the client to start the map phase.
type mapRequest struct {
	ID    int      `json:"id"`
	Peers []string `json:"peers"`
}

type loadRequest struct {
	ID   int      `json:"id"`
	URLs []string `json:"urls"`
}

type reduceRequest struct {
	ID    int      `json:"id"`
	Peers []string `json:"peers,omitempty"`
}

var commonCrawlDownloadClient = &http.Client{Timeout: 30 * time.Minute}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *server) handleData(w http.ResponseWriter, r *http.Request) {
	slotID, err := slotIDFromQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := s.resetInputs(slotID); err != nil {
		http.Error(w, "reset inputs: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.state.mu.Lock()
	slot := s.slotStateLocked(slotID)
	slot.inputData = string(body)
	slot.inputFiles = nil
	slot.output = nil
	s.state.mu.Unlock()

	s.removeIntermediate(slotID)
	w.WriteHeader(http.StatusOK)
}

// handleLoad accepts Common Crawl WET URLs, downloads them into workDir, and
// sets the worker's local input file list for the next /map run.
func (s *server) handleLoad(w http.ResponseWriter, r *http.Request) {
	var req loadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.resetInputs(req.ID); err != nil {
		http.Error(w, "reset inputs: "+err.Error(), http.StatusInternalServerError)
		return
	}
	paths, err := s.downloadInputs(req.ID, req.URLs)
	if err != nil {
		http.Error(w, "download inputs: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.state.mu.Lock()
	slot := s.slotStateLocked(req.ID)
	slot.inputData = ""
	slot.inputFiles = paths
	slot.output = nil
	s.state.mu.Unlock()

	s.removeIntermediate(req.ID)
	w.WriteHeader(http.StatusOK)
}

func (s *server) handleMap(w http.ResponseWriter, r *http.Request) {
	var req mapRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.state.mu.Lock()
	slot := s.slotStateLocked(req.ID)
	data := slot.inputData
	files := append([]string(nil), slot.inputFiles...)
	slot.peers = append([]string(nil), req.Peers...)
	slot.output = nil
	s.state.mu.Unlock()

	// Ensure idempotency: a repeat /map call (e.g. a replacement worker
	// re-running the task) must not see stale buckets from a previous run.
	s.removeIntermediate(req.ID)

	n := len(req.Peers)
	var err error

	filesOut := make([]*os.File, n)
	writers := make([]*bufio.Writer, n)
	for i := 0; i < n; i++ {
		f, err := os.OpenFile(s.bucketFile(req.ID, n, i), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			for j := 0; j < i; j++ {
				filesOut[j].Close()
			}
			http.Error(w, "create map bucket: "+err.Error(), http.StatusInternalServerError)
			return
		}
		filesOut[i] = f
		writers[i] = bufio.NewWriter(f)
	}

	var keysCount int
	if len(files) > 0 {
		keysCount, err = s.runMapFiles(files, n, writers)
	} else {
		keysCount, err = runMap(strings.NewReader(data), s.mapFn, n, writers)
	}

	for _, w := range writers {
		w.Flush()
	}

	for _, f := range filesOut {
		f.Close()
	}

	if err != nil {
		http.Error(w, "run map: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if len(files) > 0 {
		if err := s.clearMappedInputs(req.ID, files); err != nil {
			http.Error(w, "cleanup mapped inputs: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"keys":%d}`, keysCount)
}

// handleIntermediate serves the pre-partitioned bucket file for the requested
// reducer. Query params: reducer (0-based index), n (total workers).
func (s *server) handleIntermediate(w http.ResponseWriter, r *http.Request) {
	slotID, err := slotIDFromQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	reducer, err1 := strconv.Atoi(r.URL.Query().Get("reducer"))
	n, err2 := strconv.Atoi(r.URL.Query().Get("n"))
	if err1 != nil || err2 != nil || n <= 0 || reducer < 0 || reducer >= n {
		http.Error(w, "bad request: need integer params reducer in [0,n) and n>0", http.StatusBadRequest)
		return
	}

	path := s.bucketFile(slotID, n, reducer)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK) // empty bucket
		return
	}

	w.Header().Set("Content-Type", "text/tab-separated-values")
	http.ServeFile(w, r, path)
}

// intermediateClient is used for fetching pre-partitioned map buckets from peers.
// The lab hostnames publish IPv6 addresses that are not routable between all
// machines, so shuffle traffic is forced onto IPv4.
var intermediateClient = &http.Client{
	Timeout: 10 * time.Minute,
	Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			dialer := &net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}
			return dialer.DialContext(ctx, "tcp4", addr)
		},
	},
}

// fetchIntermediate GETs /intermediate from peer, downloading the raw TSV bucket
// for reducerID out of n total workers into outPath.
func fetchIntermediate(peer string, sourceSlotID, reducerID, n int, outPath string) error {
	url := fmt.Sprintf("http://%s/intermediate?slot=%d&reducer=%d&n=%d", peer, sourceSlotID, reducerID, n)
	resp, err := intermediateClient.Get(url) //nolint:gosec
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("peer %s returned %d: %s", peer, resp.StatusCode, b)
	}

	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, resp.Body)
	return err
}

// handleReduce pulls intermediate data from all peers via /intermediate, then runs
// the reduce function over the merged KV pairs.
func (s *server) handleReduce(w http.ResponseWriter, r *http.Request) {
	var req reduceRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	s.state.mu.Lock()
	slot, ok := s.state.slots[req.ID]
	if !ok {
		s.state.mu.Unlock()
		http.Error(w, fmt.Sprintf("unknown slot %d: run load/data and map first", req.ID), http.StatusBadRequest)
		return
	}
	peers := append([]string(nil), slot.peers...)
	s.state.mu.Unlock()

	if len(req.Peers) > 0 {
		peers = append([]string(nil), req.Peers...)
		s.state.mu.Lock()
		slot = s.slotStateLocked(req.ID)
		slot.peers = append([]string(nil), req.Peers...)
		s.state.mu.Unlock()
	}

	if len(peers) == 0 {
		http.Error(w, "no peers: run map phase first", http.StatusBadRequest)
		return
	}

	n := len(peers)
	errs := make([]error, n)
	var wg sync.WaitGroup

	peerFiles := make([]string, n)
	for i, peer := range peers {
		wg.Add(1)
		go func(idx int, addr string) {
			defer wg.Done()
			if addr == "" {
				return
			}
			outPath := filepath.Join(s.workDir, fmt.Sprintf("slot-%d-peer-%d.tsv", req.ID, idx))
			peerFiles[idx] = outPath
			if err := fetchIntermediate(addr, idx, req.ID, n, outPath); err != nil {
				errs[idx] = err
			}
		}(i, peer)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			http.Error(w, fmt.Sprintf("fetch from slot %d peer %s: %v", i, peers[i], err), http.StatusInternalServerError)
			return
		}
	}

	sortedFile := filepath.Join(s.workDir, fmt.Sprintf("slot-%d-sorted.tsv", req.ID))
	sortArgs := []string{"-k1,1", "-t\t", "-o", sortedFile}
	for _, pf := range peerFiles {
		if pf != "" {
			sortArgs = append(sortArgs, pf)
		}
	}

	cmd := exec.Command("sort", sortArgs...)
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	if out, err := cmd.CombinedOutput(); err != nil {
		http.Error(w, fmt.Sprintf("sort failed: %v, out: %s", err, string(out)), http.StatusInternalServerError)
		return
	}

	output, err := runReduce(sortedFile, s.reduceFn)
	if err != nil {
		http.Error(w, fmt.Sprintf("runReduce failed: %v", err), http.StatusInternalServerError)
		return
	}

	s.state.mu.Lock()
	slot = s.slotStateLocked(req.ID)
	slot.output = output
	s.state.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"keys":%d}`, len(output))
}

// handleResult returns the reduce output as newline-delimited "key\tvalue" pairs,
// sorted by key.
func (s *server) handleResult(w http.ResponseWriter, r *http.Request) {
	slotID, err := slotIDFromQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.state.mu.Lock()
	slot, ok := s.state.slots[slotID]
	if !ok {
		s.state.mu.Unlock()
		http.Error(w, fmt.Sprintf("unknown slot %d", slotID), http.StatusBadRequest)
		return
	}
	output := append([]KeyValue(nil), slot.output...)
	s.state.mu.Unlock()

	sort.Slice(output, func(i, j int) bool { return output[i].Key < output[j].Key })

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	for _, kv := range output {
		fmt.Fprintf(w, "%s\t%s\n", kv.Key, kv.Value)
	}
}

func (s *server) resetInputs(slotID int) error {
	s.state.mu.Lock()
	slot := s.slotStateLocked(slotID)
	stale := append([]string(nil), slot.inputFiles...)
	slot.inputData = ""
	slot.inputFiles = nil
	slot.peers = nil
	slot.output = nil
	s.state.mu.Unlock()
	return removeFiles(stale)
}

func (s *server) runMapFiles(files []string, n int, writers []*bufio.Writer) (int, error) {
	totalKeys := 0
	for _, file := range files {
		keysCount, err := s.runMapFile(file, n, writers)
		if err != nil {
			return totalKeys, err
		}
		totalKeys += keysCount
	}
	return totalKeys, nil
}

func (s *server) runMapFile(file string, n int, writers []*bufio.Writer) (int, error) {
	f, err := os.Open(file)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", file, err)
	}
	defer f.Close()

	var reader io.Reader = f
	if strings.HasSuffix(file, ".gz") {
		gr, err := gzip.NewReader(f)
		if err != nil {
			return 0, fmt.Errorf("gzip open %s: %w", file, err)
		}
		defer gr.Close()
		reader = gr
	}
	keysCount, err := runMap(reader, s.mapFn, n, writers)
	if err != nil {
		return keysCount, fmt.Errorf("map %s: %w", file, err)
	}
	return keysCount, nil
}

func (s *server) clearMappedInputs(slotID int, files []string) error {
	if err := removeFiles(files); err != nil {
		return err
	}
	s.state.mu.Lock()
	slot := s.slotStateLocked(slotID)
	if equalStringSlices(slot.inputFiles, files) {
		slot.inputFiles = nil
	}
	s.state.mu.Unlock()
	return nil
}

func (s *server) downloadInputs(slotID int, urls []string) ([]string, error) {
	paths := make([]string, 0, len(urls))
	for i, rawURL := range urls {
		path, err := s.downloadInput(slotID, i, rawURL)
		if err != nil {
			_ = removeFiles(paths)
			return nil, err
		}
		paths = append(paths, path)
	}
	return paths, nil
}

func (s *server) downloadInput(slotID, index int, rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse url %q: %w", rawURL, err)
	}
	base := path.Base(parsed.Path)
	if base == "" || base == "." || base == "/" {
		base = "input.wet.gz"
	}
	finalPath := filepath.Join(s.workDir, fmt.Sprintf("slot-%d-cc-input-%05d-%s", slotID, index, base))

	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if err := s.downloadInputOnce(rawURL, finalPath); err == nil {
			return finalPath, nil
		} else {
			lastErr = err
		}
	}
	return "", fmt.Errorf("download %s: %w", rawURL, lastErr)
}

func (s *server) downloadInputOnce(rawURL, finalPath string) error {
	resp, err := commonCrawlDownloadClient.Get(rawURL) //nolint:gosec
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	tmp, err := os.CreateTemp(s.workDir, "cc-input-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err := io.Copy(tmp, resp.Body); err != nil {
		tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Remove(finalPath); err != nil && !os.IsNotExist(err) {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

func removeFiles(paths []string) error {
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func slotIDFromQuery(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("slot")
	if raw == "" {
		return 0, fmt.Errorf("missing required slot query parameter")
	}
	slotID, err := strconv.Atoi(raw)
	if err != nil || slotID < 0 {
		return 0, fmt.Errorf("bad request: slot must be a non-negative integer")
	}
	return slotID, nil
}

func (s *server) slotStateLocked(slotID int) *slotState {
	slot := s.state.slots[slotID]
	if slot == nil {
		slot = &slotState{}
		s.state.slots[slotID] = slot
	}
	return slot
}
