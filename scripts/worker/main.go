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
//	worker -port 9090 -plugin wordcount
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

var (
	globalState = &state{}
	mapFn       MapFunc    = wordCountMap
	reduceFn    ReduceFunc = wordCountReduce
)

func main() {
	port := flag.String("port", "9090", "port to listen on")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("POST /data", handleData)
	mux.HandleFunc("POST /map", handleMap)
	mux.HandleFunc("POST /shuffle/recv", handleShuffleRecv)
	mux.HandleFunc("POST /shuffle", handleShuffle)
	mux.HandleFunc("POST /reduce", handleReduce)
	mux.HandleFunc("GET /result", handleResult)

	addr := ":" + *port
	log.Printf("worker listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

// handleHealth returns 200 OK when the server is ready.
func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// handleData stores the raw text data chunk sent by the client.
func handleData(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	globalState.mu.Lock()
	globalState.inputData = string(body)
	globalState.intermediate = nil
	globalState.output = nil
	globalState.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

// mapRequest is the JSON body sent by the client to start the map phase.
type mapRequest struct {
	ID    int      `json:"id"`
	Peers []string `json:"peers"`
}

// handleMap starts the map phase synchronously and returns when done.
func handleMap(w http.ResponseWriter, r *http.Request) {
	var req mapRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	globalState.mu.Lock()
	data := globalState.inputData
	globalState.workerID = req.ID
	globalState.peers = req.Peers
	intermediate := runMap(data, mapFn)
	globalState.intermediate = intermediate
	globalState.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"keys":%d}`, len(intermediate))
}

// handleShuffleRecv accepts a batch of KV pairs from a peer and merges them
// into the local intermediate store.
func handleShuffleRecv(w http.ResponseWriter, r *http.Request) {
	var batch []KeyValue
	if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	globalState.mu.Lock()
	if globalState.intermediate == nil {
		globalState.intermediate = make(map[string][]string)
	}
	for _, kv := range batch {
		globalState.intermediate[kv.Key] = append(globalState.intermediate[kv.Key], kv.Value)
	}
	globalState.mu.Unlock()

	w.WriteHeader(http.StatusOK)
}

// handleShuffle drives the shuffle step: for each intermediate key, compute the
// target node via fnv32(key)%N, batch the KVs, and POST them to the target's
// /shuffle/recv endpoint. Blocks until all sends are acknowledged.
func handleShuffle(w http.ResponseWriter, r *http.Request) {
	globalState.mu.Lock()
	peers := globalState.peers
	id := globalState.workerID
	// Take ownership of the local map output and replace globalState.intermediate
	// with a fresh map so that concurrent /shuffle/recv calls write into the new
	// map without racing against our iteration below.
	localMap := globalState.intermediate
	globalState.intermediate = make(map[string][]string)
	globalState.mu.Unlock()

	if len(peers) == 0 {
		http.Error(w, "no peers: run map phase first", http.StatusBadRequest)
		return
	}

	n := len(peers)
	// buckets[i] = KV pairs destined for peers[i]
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

	// Merge self-destined keys from local map output into the received-data map.
	globalState.mu.Lock()
	for _, kv := range buckets[id] {
		globalState.intermediate[kv.Key] = append(globalState.intermediate[kv.Key], kv.Value)
	}
	globalState.mu.Unlock()

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

// handleReduce runs the reduce phase synchronously on the locally held intermediate data.
func handleReduce(w http.ResponseWriter, _ *http.Request) {
	globalState.mu.Lock()
	intermediate := globalState.intermediate
	globalState.mu.Unlock()

	output := runReduce(intermediate, reduceFn)

	globalState.mu.Lock()
	globalState.output = output
	globalState.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"keys":%d}`, len(output))
}

// handleResult returns the reduce output as newline-delimited "key\tvalue" pairs,
// sorted by key.
func handleResult(w http.ResponseWriter, _ *http.Request) {
	globalState.mu.Lock()
	output := globalState.output
	globalState.mu.Unlock()

	sort.Slice(output, func(i, j int) bool { return output[i].Key < output[j].Key })

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	for _, kv := range output {
		fmt.Fprintf(w, "%s\t%s\n", kv.Key, kv.Value)
	}
}
