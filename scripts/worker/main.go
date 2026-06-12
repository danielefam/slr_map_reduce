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
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
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

// state holds all runtime data for one MapReduce job.
type state struct {
	mu sync.Mutex

	// input data written directly by the client in local-file mode
	inputData string
	// inputFiles are Common Crawl WET files downloaded under workDir.
	inputFiles []string

	// index of this worker in the peer list (set at map phase)
	workerID int
	peers    []string // "host:port" for all workers including self

	// final reduce output
	output []KeyValue
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
	return &server{state: &state{}, mapFn: mapFn, reduceFn: reduceFn, workDir: workDir}
}

// bucketFile returns the path for the pre-partitioned intermediate file
// for reducer idx out of n total reducers.
func (s *server) bucketFile(n, idx int) string {
	return filepath.Join(s.workDir, fmt.Sprintf("map-bucket-%d-%d.jsonl", n, idx))
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

// removeIntermediate deletes all pre-partitioned map bucket files.
func (s *server) removeIntermediate() {
	matches, _ := filepath.Glob(filepath.Join(s.workDir, "map-bucket-*.jsonl"))
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
	URLs []string `json:"urls"`
}

var commonCrawlDownloadClient = &http.Client{Timeout: 30 * time.Minute}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *server) handleData(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := s.resetInputs(); err != nil {
		http.Error(w, "reset inputs: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.state.mu.Lock()
	s.state.inputData = string(body)
	s.state.inputFiles = nil
	s.state.output = nil
	s.state.mu.Unlock()

	s.removeIntermediate()
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
	if err := s.resetInputs(); err != nil {
		http.Error(w, "reset inputs: "+err.Error(), http.StatusInternalServerError)
		return
	}
	paths, err := s.downloadInputs(req.URLs)
	if err != nil {
		http.Error(w, "download inputs: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.state.mu.Lock()
	s.state.inputData = ""
	s.state.inputFiles = paths
	s.state.output = nil
	s.state.mu.Unlock()

	s.removeIntermediate()
	w.WriteHeader(http.StatusOK)
}

func (s *server) handleMap(w http.ResponseWriter, r *http.Request) {
	var req mapRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.state.mu.Lock()
	data := s.state.inputData
	files := append([]string(nil), s.state.inputFiles...)
	s.state.workerID = req.ID
	s.state.peers = req.Peers
	s.state.output = nil
	s.state.mu.Unlock()

	// Ensure idempotency: a repeat /map call (e.g. a replacement worker
	// re-running the task) must not see stale buckets from a previous run.
	s.removeIntermediate()

	n := len(req.Peers)
	var err error
	var intermediate map[string][]string
	if len(files) > 0 {
		intermediate, err = s.runMapFiles(files)
	} else {
		intermediate, err = runMap(strings.NewReader(data), s.mapFn)
	}
	if err != nil {
		http.Error(w, "run map: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Partition intermediate KVs into n bucket files (one per reducer) so that
	// /intermediate?reducer=X can serve its file directly without scanning all output.
	buckets := make([][]KeyValue, n)
	for key, values := range intermediate {
		idx := targetNode(key, n)
		for _, v := range values {
			buckets[idx] = append(buckets[idx], KeyValue{Key: key, Value: v})
		}
	}
	for i, kvs := range buckets {
		if err := writeKVLines(s.bucketFile(n, i), kvs); err != nil {
			http.Error(w, "write map bucket: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if len(files) > 0 {
		if err := s.clearMappedInputs(files); err != nil {
			http.Error(w, "cleanup mapped inputs: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"keys":%d}`, len(intermediate))
}

// handleIntermediate serves the pre-partitioned bucket file for the requested
// reducer. Query params: reducer (0-based index), n (total workers).
func (s *server) handleIntermediate(w http.ResponseWriter, r *http.Request) {
	reducer, err1 := strconv.Atoi(r.URL.Query().Get("reducer"))
	n, err2 := strconv.Atoi(r.URL.Query().Get("n"))
	if err1 != nil || err2 != nil || n <= 0 || reducer < 0 || reducer >= n {
		http.Error(w, "bad request: need integer params reducer in [0,n) and n>0", http.StatusBadRequest)
		return
	}

	kvs, err := readKVFlat(s.bucketFile(n, reducer))
	if err != nil {
		http.Error(w, "read map bucket: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if kvs == nil {
		kvs = []KeyValue{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(kvs); err != nil {
		log.Printf("encode intermediate: %v", err)
	}
}

// intermediateClient is used for fetching pre-partitioned map buckets from peers.
// A 10-minute timeout is generous enough for large buckets over a LAN.
var intermediateClient = &http.Client{Timeout: 10 * time.Minute}

// fetchIntermediate GETs /intermediate from peer, requesting the KV pairs destined
// for reducerID out of n total workers.
func fetchIntermediate(peer string, reducerID, n int) ([]KeyValue, error) {
	url := fmt.Sprintf("http://%s/intermediate?reducer=%d&n=%d", peer, reducerID, n)
	resp, err := intermediateClient.Get(url) //nolint:gosec
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("peer %s returned %d: %s", peer, resp.StatusCode, b)
	}
	var kvs []KeyValue
	if err := json.NewDecoder(resp.Body).Decode(&kvs); err != nil {
		return nil, fmt.Errorf("decode intermediate from %s: %w", peer, err)
	}
	return kvs, nil
}

// reduceRequest is the optional JSON body of POST /reduce. When Peers is
// non-empty it overrides the peer list stored by the most recent /map call,
// allowing the orchestrator to update routing after replacing a dead host.
type reduceRequest struct {
	Peers []string `json:"peers,omitempty"`
}

// handleReduce pulls intermediate data from all peers via /intermediate, then runs
// the reduce function over the merged KV pairs.
//
// Peer list selection (in order of preference):
//  1. peers from the request body (if provided and non-empty)
//  2. peers stored from the prior /map call
func (s *server) handleReduce(w http.ResponseWriter, r *http.Request) {
	var req reduceRequest
	if r.Body != nil {
		// Body is optional; ignore decode errors for empty/short bodies.
		_ = json.NewDecoder(r.Body).Decode(&req)
	}

	s.state.mu.Lock()
	peers := s.state.peers
	id := s.state.workerID
	s.state.mu.Unlock()

	if len(req.Peers) > 0 {
		peers = req.Peers
		s.state.mu.Lock()
		s.state.peers = req.Peers
		s.state.mu.Unlock()
	}

	if len(peers) == 0 {
		http.Error(w, "no peers: run map phase first", http.StatusBadRequest)
		return
	}

	n := len(peers)
	var mu sync.Mutex
	intermediate := make(map[string][]string)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i, peer := range peers {
		wg.Add(1)
		go func(idx int, addr string) {
			defer wg.Done()
			kvs, err := fetchIntermediate(addr, id, n)
			if err != nil {
				errs[idx] = err
				return
			}
			mu.Lock()
			for _, kv := range kvs {
				intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
			}
			mu.Unlock()
		}(i, peer)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			http.Error(w, fmt.Sprintf("fetch from peer %s: %v", peers[i], err), http.StatusInternalServerError)
			return
		}
	}

	output := runReduce(intermediate, s.reduceFn)

	s.state.mu.Lock()
	s.state.output = output
	s.state.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"keys":%d}`, len(output))
}

// handleResult returns the reduce output as newline-delimited "key\tvalue" pairs,
// sorted by key.
func (s *server) handleResult(w http.ResponseWriter, _ *http.Request) {
	s.state.mu.Lock()
	output := s.state.output
	s.state.mu.Unlock()

	sort.Slice(output, func(i, j int) bool { return output[i].Key < output[j].Key })

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	for _, kv := range output {
		fmt.Fprintf(w, "%s\t%s\n", kv.Key, kv.Value)
	}
}

func (s *server) resetInputs() error {
	s.state.mu.Lock()
	stale := append([]string(nil), s.state.inputFiles...)
	s.state.inputData = ""
	s.state.inputFiles = nil
	s.state.output = nil
	s.state.mu.Unlock()
	return removeFiles(stale)
}

func (s *server) runMapFiles(files []string) (map[string][]string, error) {
	out := make(map[string][]string)
	for _, file := range files {
		fileMap, err := s.runMapFile(file)
		if err != nil {
			return nil, err
		}
		for key, values := range fileMap {
			out[key] = append(out[key], values...)
		}
	}
	return out, nil
}

func (s *server) runMapFile(file string) (map[string][]string, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", file, err)
	}
	defer f.Close()

	var reader io.Reader = f
	if strings.HasSuffix(file, ".gz") {
		gr, err := gzip.NewReader(f)
		if err != nil {
			return nil, fmt.Errorf("gzip open %s: %w", file, err)
		}
		defer gr.Close()
		reader = gr
	}
	intermediate, err := runMap(reader, s.mapFn)
	if err != nil {
		return nil, fmt.Errorf("map %s: %w", file, err)
	}
	return intermediate, nil
}

func (s *server) clearMappedInputs(files []string) error {
	if err := removeFiles(files); err != nil {
		return err
	}
	s.state.mu.Lock()
	if equalStringSlices(s.state.inputFiles, files) {
		s.state.inputFiles = nil
	}
	s.state.mu.Unlock()
	return nil
}

func (s *server) downloadInputs(urls []string) ([]string, error) {
	paths := make([]string, 0, len(urls))
	for i, rawURL := range urls {
		path, err := s.downloadInput(i, rawURL)
		if err != nil {
			_ = removeFiles(paths)
			return nil, err
		}
		paths = append(paths, path)
	}
	return paths, nil
}

func (s *server) downloadInput(index int, rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse url %q: %w", rawURL, err)
	}
	base := path.Base(parsed.Path)
	if base == "" || base == "." || base == "/" {
		base = "input.wet.gz"
	}
	finalPath := filepath.Join(s.workDir, fmt.Sprintf("cc-input-%05d-%s", index, base))

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
