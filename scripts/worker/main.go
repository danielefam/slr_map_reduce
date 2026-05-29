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
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
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

	// intermediate: key → list of values (populated by map + shuffle recv)
	intermediate map[string][]string

	// final reduce output
	output []KeyValue
}

// server wraps per-instance state and the map/reduce functions so that multiple
// independent workers can run in the same process (useful for testing).
type server struct {
	state    *state
	mapFn    MapFunc
	reduceFn ReduceFunc
}

func newServer(mapFn MapFunc, reduceFn ReduceFunc) *server {
	return &server{state: &state{}, mapFn: mapFn, reduceFn: reduceFn}
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

	srv := newServer(wordCountMap, wordCountReduce)
	addr := ":" + *port
	log.Printf("worker listening on %s", addr)
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
	s.state.intermediate = nil
	s.state.output = nil
	s.state.mu.Unlock()
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
	intermediate := runMap(data, s.mapFn)
	s.state.intermediate = intermediate
	s.state.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"keys":%d}`, len(intermediate))
}

// handleShuffleRecv accepts a batch of KV pairs from a peer and merges them
// into the local intermediate store.
func (s *server) handleShuffleRecv(w http.ResponseWriter, r *http.Request) {
	var batch []KeyValue
	if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.state.mu.Lock()
	if s.state.intermediate == nil {
		s.state.intermediate = make(map[string][]string)
	}
	for _, kv := range batch {
		s.state.intermediate[kv.Key] = append(s.state.intermediate[kv.Key], kv.Value)
	}
	s.state.mu.Unlock()

	w.WriteHeader(http.StatusOK)
}

// handleShuffle drives the shuffle step: for each intermediate key, compute the
// target node via fnv32(key)%N, batch the KVs, and POST them to the target's
// /shuffle/recv endpoint. Blocks until all sends are acknowledged.
func (s *server) handleShuffle(w http.ResponseWriter, _ *http.Request) {
	s.state.mu.Lock()
	peers := s.state.peers
	id := s.state.workerID
	// Take ownership of the local map output and replace s.state.intermediate
	// with a fresh map so that concurrent /shuffle/recv calls write into the new
	// map without racing against our iteration below.
	localMap := s.state.intermediate
	s.state.intermediate = make(map[string][]string)
	s.state.mu.Unlock()

	if len(peers) == 0 {
		http.Error(w, "no peers: run map phase first", http.StatusBadRequest)
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
			continue // self-destined keys are merged below
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

	// Merge self-destined keys into the received-data map.
	s.state.mu.Lock()
	for _, kv := range buckets[id] {
		s.state.intermediate[kv.Key] = append(s.state.intermediate[kv.Key], kv.Value)
	}
	s.state.mu.Unlock()

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
	s.state.mu.Lock()
	intermediate := s.state.intermediate
	s.state.mu.Unlock()

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
