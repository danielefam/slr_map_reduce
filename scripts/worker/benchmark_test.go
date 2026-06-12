package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// mapBenchInput is a ~440 KB text block reused across benchmarks.
var mapBenchInput string

func init() {
	var sb strings.Builder
	for range 10_000 {
		fmt.Fprintln(&sb, "the quick brown fox jumps over the lazy dog")
	}
	mapBenchInput = sb.String()
}

func BenchmarkWordCountMap(b *testing.B) {
	text := strings.Repeat("the quick brown fox jumps over the lazy dog ", 100)
	b.ResetTimer()
	for range b.N {
		wordCountMap("0", text)
	}
}

func BenchmarkWordCountReduce(b *testing.B) {
	values := make([]string, 1_000)
	for i := range values {
		values[i] = "1"
	}
	b.ResetTimer()
	for range b.N {
		wordCountReduce("word", values)
	}
}

func BenchmarkRunMap(b *testing.B) {
	b.ResetTimer()
	for range b.N {
		if _, err := runMap(strings.NewReader(mapBenchInput), wordCountMap); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRunReduce(b *testing.B) {
	intermediate := make(map[string][]string)
	for _, w := range strings.Fields("the quick brown fox jumps over lazy dog") {
		vals := make([]string, 10_000)
		for i := range vals {
			vals[i] = "1"
		}
		intermediate[w] = vals
	}
	b.ResetTimer()
	for range b.N {
		runReduce(intermediate, wordCountReduce)
	}
}

// BenchmarkSingleWorkerPipeline measures the end-to-end HTTP cycle for one worker:
// data upload → map → shuffle → reduce. Server creation/teardown is included.
func BenchmarkSingleWorkerPipeline(b *testing.B) {
	input := []byte(mapBenchInput)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		ts := httptest.NewServer(newServer(wordCountMap, wordCountReduce, b.TempDir()).handler())
		peer := strings.TrimPrefix(ts.URL, "http://")

		resp, _ := http.Post(ts.URL+"/data", "application/octet-stream", bytes.NewReader(input))
		resp.Body.Close()

		mapBody, _ := json.Marshal(mapRequest{ID: 0, Peers: []string{peer}})
		resp, _ = http.Post(ts.URL+"/map", "application/json", bytes.NewReader(mapBody))
		resp.Body.Close()

		resp, _ = http.Post(ts.URL+"/shuffle", "application/octet-stream", bytes.NewReader(nil))
		resp.Body.Close()

		resp, _ = http.Post(ts.URL+"/reduce", "application/octet-stream", bytes.NewReader(nil))
		resp.Body.Close()

		ts.Close()
	}
}

// BenchmarkMultiWorkerShuffle measures concurrent shuffle across N in-process workers,
// matching the production broadcastPost pattern. Setup (map phase) is included.
func BenchmarkMultiWorkerShuffle(b *testing.B) {
	const n = 4
	chunkLines := strings.Repeat("the quick brown fox jumps over the lazy dog\n", 2_500)
	chunk := []byte(chunkLines)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		servers := make([]*httptest.Server, n)
		peers := make([]string, n)
		for i := range n {
			servers[i] = httptest.NewServer(newServer(wordCountMap, wordCountReduce, b.TempDir()).handler())
			peers[i] = strings.TrimPrefix(servers[i].URL, "http://")
		}

		// Load data and run map on every worker.
		for i, ts := range servers {
			resp, _ := http.Post(ts.URL+"/data", "application/octet-stream", bytes.NewReader(chunk))
			resp.Body.Close()
			mapBody, _ := json.Marshal(mapRequest{ID: i, Peers: peers})
			resp, _ = http.Post(ts.URL+"/map", "application/json", bytes.NewReader(mapBody))
			resp.Body.Close()
		}

		// Concurrent shuffle — mirrors broadcastPost.
		var wg sync.WaitGroup
		for _, ts := range servers {
			wg.Add(1)
			go func(s *httptest.Server) {
				defer wg.Done()
				resp, _ := http.Post(s.URL+"/shuffle", "application/octet-stream", bytes.NewReader(nil))
				resp.Body.Close()
			}(ts)
		}
		wg.Wait()

		for _, ts := range servers {
			ts.Close()
		}
	}
}
