// Command worker is an HTTP server that participates in a distributed MapReduce job.
//
// Endpoints:
//
//	GET  /health              — readiness probe
//	POST /data                — receive raw text data chunk
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
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// state holds all runtime data for one MapReduce job.
type state struct {
	mu sync.Mutex

	// input data written by the client
	inputData string

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
	return mux
}

func main() {
	port := flag.String("port", "9090", "port to listen on")
	flag.Parse()

	workDir, err := os.MkdirTemp("", "mr-worker-*")
	if err != nil {
		log.Fatalf("create work dir: %v", err)
	}

	srv := newServer(wordCountMap, wordCountReduce, workDir)
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
	s.state.mu.Lock()
	s.state.inputData = string(body)
	s.state.output = nil
	s.state.mu.Unlock()

	s.removeIntermediate()
	w.WriteHeader(http.StatusOK)
}

// handleLoad accepts a JSON array of file paths, reads them (decompressing .gz files),
// and sets the worker's input data. Used when input lives on a shared NFS mount.
func (s *server) handleLoad(w http.ResponseWriter, r *http.Request) {
	var paths []string
	if err := json.NewDecoder(r.Body).Decode(&paths); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	var buf strings.Builder
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			http.Error(w, "open file: "+err.Error(), http.StatusInternalServerError)
			return
		}
		var reader io.Reader = f
		if strings.HasSuffix(path, ".gz") {
			gr, err := gzip.NewReader(f)
			if err != nil {
				f.Close()
				http.Error(w, "gzip open "+path+": "+err.Error(), http.StatusInternalServerError)
				return
			}
			defer gr.Close()
			reader = gr
		}
		data, err := io.ReadAll(reader)
		f.Close()
		if err != nil {
			http.Error(w, "read file "+path+": "+err.Error(), http.StatusInternalServerError)
			return
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}

	s.state.mu.Lock()
	s.state.inputData = buf.String()
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
	s.state.workerID = req.ID
	s.state.peers = req.Peers
	s.state.output = nil
	s.state.mu.Unlock()

	// Ensure idempotency: a repeat /map call (e.g. a replacement worker
	// re-running the task) must not see stale buckets from a previous run.
	s.removeIntermediate()

	n := len(req.Peers)
	intermediate := runMap(data, s.mapFn)

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
