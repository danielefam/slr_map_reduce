// Command worker is an HTTP server that participates in a distributed MapReduce job.
//
// Endpoints:
//
//	GET  /health        — readiness probe
//	POST /data          — receive raw text data chunk
//	POST /map           — start map phase
//	POST /shuffle/recv  — receive intermediate KV pairs from a peer
//	POST /shuffle       — start shuffle: push map output to correct peers
//	POST /reduce        — start reduce phase
//	GET  /result        — download reduce output
//
// Usage:
//
//	worker -port 9090
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
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

// mapFile returns the path for the map-phase output file.
func (s *server) mapFile() string { return filepath.Join(s.workDir, "map-output.jsonl") }

// shuffleFile returns the path for the shuffle-received data file.
func (s *server) shuffleFile() string { return filepath.Join(s.workDir, "shuffle-recv.jsonl") }

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

// appendKVLines appends kvs to path as JSON lines, creating the file if needed.
func appendKVLines(path string, kvs []KeyValue) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
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

// readKVLines reads all JSON-line KV pairs from path and returns them grouped by key.
// Returns an empty map (not an error) if the file does not exist.
func readKVLines(path string) (map[string][]string, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return make(map[string][]string), nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	out := make(map[string][]string)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024*1024), 64*1024*1024)
	for scanner.Scan() {
		var kv KeyValue
		if err := json.Unmarshal(scanner.Bytes(), &kv); err != nil {
			return nil, fmt.Errorf("readKVLines %s: %w", path, err)
		}
		out[kv.Key] = append(out[kv.Key], kv.Value)
	}
	return out, scanner.Err()
}

// flattenKV converts the grouped intermediate map into a flat []KeyValue slice.
func flattenKV(intermediate map[string][]string) []KeyValue {
	var out []KeyValue
	for k, vals := range intermediate {
		for _, v := range vals {
			out = append(out, KeyValue{Key: k, Value: v})
		}
	}
	return out
}

// removeIntermediate deletes both intermediate files, ignoring not-found errors.
func (s *server) removeIntermediate() {
	_ = os.Remove(s.mapFile())
	_ = os.Remove(s.shuffleFile())
}

// handler returns an http.Handler with all worker routes registered.
func (s *server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("POST /data", s.handleData)
	mux.HandleFunc("POST /map", s.handleMap)
	mux.HandleFunc("POST /shuffle/recv", s.handleShuffleRecv)
	mux.HandleFunc("POST /shuffle", s.handleShuffle)
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
	s.state.mu.Unlock()

	intermediate := runMap(data, s.mapFn)
	if err := writeKVLines(s.mapFile(), flattenKV(intermediate)); err != nil {
		http.Error(w, "write map output: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"keys":%d}`, len(intermediate))
}

// handleShuffleRecv accepts a batch of KV pairs from a peer and appends them
// to the local shuffle file.
func (s *server) handleShuffleRecv(w http.ResponseWriter, r *http.Request) {
	var batch []KeyValue
	if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.state.mu.Lock()
	err := appendKVLines(s.shuffleFile(), batch)
	s.state.mu.Unlock()

	if err != nil {
		http.Error(w, "write shuffle data: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleShuffle drives the shuffle step: for each intermediate key, compute the
// target node via fnv32(key)%N, batch the KVs, and POST them to the target's
// /shuffle/recv endpoint. Blocks until all sends are acknowledged.
func (s *server) handleShuffle(w http.ResponseWriter, _ *http.Request) {
	s.state.mu.Lock()
	peers := s.state.peers
	id := s.state.workerID
	s.state.mu.Unlock()

	if len(peers) == 0 {
		http.Error(w, "no peers: run map phase first", http.StatusBadRequest)
		return
	}

	localMap, err := readKVLines(s.mapFile())
	if err != nil {
		http.Error(w, "read map output: "+err.Error(), http.StatusInternalServerError)
		return
	}

	n := len(peers)
	buckets := make([][]KeyValue, n)
	for key, values := range localMap {
		target := targetNode(key, n)
		for _, v := range values {
			buckets[target] = append(buckets[target], KeyValue{Key: key, Value: v})
		}
	}

	// Send buckets to all peers concurrently (skip self).
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i, peer := range peers {
		if i == id {
			continue // self-destined keys are written below
		}
		if len(buckets[i]) == 0 {
			continue
		}
		wg.Add(1)
		go func(idx int, addr string, batch []KeyValue) {
			defer wg.Done()
			errs[idx] = sendBatch(addr, batch)
		}(i, peer, buckets[i])
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			http.Error(w, fmt.Sprintf("send to peer %s failed: %v", peers[i], err), http.StatusInternalServerError)
			return
		}
	}

	// Write self-destined keys to the shuffle file and delete the map file.
	s.state.mu.Lock()
	if len(buckets[id]) > 0 {
		err = appendKVLines(s.shuffleFile(), buckets[id])
	}
	s.state.mu.Unlock()
	if err != nil {
		http.Error(w, "write self shuffle data: "+err.Error(), http.StatusInternalServerError)
		return
	}
	_ = os.Remove(s.mapFile())

	w.WriteHeader(http.StatusOK)
}

// sendBatch POSTs a batch of KV pairs to peer's /shuffle/recv endpoint.
func sendBatch(peer string, batch []KeyValue) error {
	body, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	url := "http://" + peer + "/shuffle/recv"
	resp, err := http.Post(url, "application/json", bytes.NewReader(body)) //nolint:gosec
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("peer returned %d: %s", resp.StatusCode, b)
	}
	return nil
}

func (s *server) handleReduce(w http.ResponseWriter, _ *http.Request) {
	// Read from the shuffle-received file (normal flow) and also from the
	// map-output file if it still exists (e.g. when shuffle was skipped in tests).
	intermediate, err := readKVLines(s.shuffleFile())
	if err != nil {
		http.Error(w, "read shuffle data: "+err.Error(), http.StatusInternalServerError)
		return
	}
	mapOut, err := readKVLines(s.mapFile())
	if err != nil {
		http.Error(w, "read map output: "+err.Error(), http.StatusInternalServerError)
		return
	}
	for k, vals := range mapOut {
		intermediate[k] = append(intermediate[k], vals...)
	}

	output := runReduce(intermediate, s.reduceFn)
	_ = os.Remove(s.shuffleFile())
	_ = os.Remove(s.mapFile())

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
