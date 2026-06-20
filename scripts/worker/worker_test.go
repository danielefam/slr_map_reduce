package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// newTestServer spins up an isolated in-process worker server.
func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(newServer(wordCountMap, wordCountReduce, t.TempDir()).handler())
}

// mustPost posts body to url and fails the test if the response is not 200.
func mustPost(t *testing.T, url, contentType string, body []byte) {
	t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	} else {
		r = bytes.NewReader([]byte{})
	}
	resp, err := http.Post(url, contentType, r)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST %s returned %d: %s", url, resp.StatusCode, b)
	}
}

func dataURL(baseURL string, slotID int) string {
	return fmt.Sprintf("%s/data?slot=%d", baseURL, slotID)
}

func resultURL(baseURL string, slotID int) string {
	return fmt.Sprintf("%s/result?slot=%d", baseURL, slotID)
}

func mustMap(t *testing.T, baseURL string, slotID int, peers []string) {
	t.Helper()
	body, _ := json.Marshal(mapRequest{ID: slotID, Peers: peers})
	mustPost(t, baseURL+"/map", "application/json", body)
}

func mustReduce(t *testing.T, baseURL string, slotID int, peers []string) {
	t.Helper()
	body, _ := json.Marshal(reduceRequest{ID: slotID, Peers: peers})
	mustPost(t, baseURL+"/reduce", "application/json", body)
}

// getResult fetches /result and returns the parsed word→count map.
func getResult(t *testing.T, baseURL string, slotID int) map[string]int {
	t.Helper()
	resp, err := http.Get(resultURL(baseURL, slotID))
	if err != nil {
		t.Fatalf("GET /result: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	counts := make(map[string]int)
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			t.Errorf("malformed result line: %q", line)
			continue
		}
		v, _ := strconv.Atoi(parts[1])
		counts[parts[0]] += v
	}
	return counts
}

func TestHealth(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
}

func TestHandleData(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	mustPost(t, dataURL(ts.URL, 0), "application/octet-stream", []byte("hello world\n"))
}

func TestHandleLoadDownloadsAndCleansInputs(t *testing.T) {
	workDir := t.TempDir()
	srv := newServer(wordCountMap, wordCountReduce, workDir)
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		_, _ = gz.Write([]byte("hello crawl\nhello web\n"))
		_ = gz.Close()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(buf.Bytes())
	}))
	defer source.Close()

	body, _ := json.Marshal(loadRequest{ID: 0, URLs: []string{source.URL + "/segment-00000.wet.gz"}})
	mustPost(t, ts.URL+"/load", "application/json", body)

	peer := strings.TrimPrefix(ts.URL, "http://")
	mustMap(t, ts.URL, 0, []string{peer})
	mustReduce(t, ts.URL, 0, nil)

	got := getResult(t, ts.URL, 0)
	if got["hello"] != 2 || got["crawl"] != 1 || got["web"] != 1 {
		t.Fatalf("unexpected result counts: %v", got)
	}
	matches, err := filepath.Glob(filepath.Join(workDir, "slot-0-cc-input-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("expected downloaded inputs to be removed after map, found %v", matches)
	}
}

// TestSingleWorkerPipeline runs the full map→reduce→result cycle on one worker.
func TestSingleWorkerPipeline(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	input := "hello world\nhello go\nworld cup\n"
	mustPost(t, dataURL(ts.URL, 0), "application/octet-stream", []byte(input))

	peer := strings.TrimPrefix(ts.URL, "http://")
	mustMap(t, ts.URL, 0, []string{peer})
	mustReduce(t, ts.URL, 0, nil)

	got := getResult(t, ts.URL, 0)

	want := map[string]int{"hello": 2, "world": 2, "go": 1, "cup": 1}
	for word, count := range want {
		if got[word] != count {
			t.Errorf("word %q: want %d, got %d", word, count, got[word])
		}
	}
	if len(got) != len(want) {
		t.Errorf("result has %d distinct words, want %d: %v", len(got), len(want), got)
	}
}

// TestMultiWorkerPipeline runs the full pipeline across N in-process workers and
// verifies that the merged counts match the expected word frequencies.
func TestMultiWorkerPipeline(t *testing.T) {
	const n = 3
	servers := make([]*httptest.Server, n)
	peers := make([]string, n)
	for i := range n {
		servers[i] = httptest.NewServer(newServer(wordCountMap, wordCountReduce, t.TempDir()).handler())
		defer servers[i].Close()
		peers[i] = strings.TrimPrefix(servers[i].URL, "http://")
	}

	// Distribute data across workers.
	chunks := []string{
		"hello world\nhello go\n",
		"world cup\ngo fast\n",
		"fast car\nhello car\n",
	}
	for i, ts := range servers {
		mustPost(t, dataURL(ts.URL, i), "application/octet-stream", []byte(chunks[i]))
	}

	// Map phase.
	for i, ts := range servers {
		mustMap(t, ts.URL, i, peers)
	}

	// Reduce phase: each worker pulls intermediate data from all peers.
	for i, ts := range servers {
		mustReduce(t, ts.URL, i, nil)
	}

	// Merge results from all workers.
	merged := make(map[string]int)
	for i, ts := range servers {
		for word, count := range getResult(t, ts.URL, i) {
			merged[word] += count
		}
	}

	want := map[string]int{
		"hello": 3,
		"world": 2,
		"go":    2,
		"cup":   1,
		"fast":  2,
		"car":   2,
	}
	for word, count := range want {
		if merged[word] != count {
			t.Errorf("word %q: want %d, got %d", word, count, merged[word])
		}
	}
	if len(merged) != len(want) {
		t.Errorf("merged has %d words, want %d: %v", len(merged), len(want), merged)
	}
}

// TestIntermediateEndpoint verifies that GET /intermediate returns only the KV pairs
// whose keys hash to the requested reducer bucket, and that every bucket is served.
func TestIntermediateEndpoint(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	peer := strings.TrimPrefix(ts.URL, "http://")
	mustPost(t, dataURL(ts.URL, 0), "application/octet-stream", []byte("hello world\nhello go\n"))

	// Use n=3 peers so the map phase writes 3 bucket files; the two extra
	// addresses are never contacted during this test (no reduce phase runs).
	const n = 3
	mustMap(t, ts.URL, 0, []string{peer, "unused1:9090", "unused2:9090"})

	for r := 0; r < n; r++ {
		url := fmt.Sprintf("%s/intermediate?slot=%d&reducer=%d&n=%d", ts.URL, 0, r, n)
		resp, err := http.Get(url) //nolint:gosec
		if err != nil {
			t.Fatalf("GET /intermediate reducer=%d: %v", r, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("reducer %d: want 200, got %d", r, resp.StatusCode)
		}
		var kvs []KeyValue
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.SplitN(line, "\t", 2)
			if len(parts) == 2 {
				kvs = append(kvs, KeyValue{Key: parts[0], Value: parts[1]})
			}
		}
		if err := scanner.Err(); err != nil {
			t.Fatalf("scan reducer %d: %v", r, err)
		}
		for _, kv := range kvs {
			if got := targetNode(kv.Key, n); got != r {
				t.Errorf("reducer %d: key %q hashes to bucket %d", r, kv.Key, got)
			}
		}
	}
}

func TestDataResetsState(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	peer := strings.TrimPrefix(ts.URL, "http://")

	// First job.
	mustPost(t, dataURL(ts.URL, 0), "application/octet-stream", []byte("apple apple\n"))
	mustMap(t, ts.URL, 0, []string{peer})
	mustReduce(t, ts.URL, 0, nil)

	// Second job with different data — /data must reset state.
	mustPost(t, dataURL(ts.URL, 0), "application/octet-stream", []byte("banana\n"))
	mustMap(t, ts.URL, 0, []string{peer})
	mustReduce(t, ts.URL, 0, nil)

	got := getResult(t, ts.URL, 0)
	if got["apple"] != 0 {
		t.Errorf("apple should be gone after reset, got %d", got["apple"])
	}
	if got["banana"] != 1 {
		t.Errorf("banana: want 1, got %d", got["banana"])
	}
}

// TestHandleReduceNoMapPhase verifies that /reduce returns 400 when the map phase
// has not been run (no peer list is available).
func TestHandleReduceNoMapPhase(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	body, _ := json.Marshal(reduceRequest{ID: 0})
	resp, err := http.Post(ts.URL+"/reduce", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("reduce without map: want 400, got %d", resp.StatusCode)
	}
}

// TestConcurrentReducePipeline mirrors the production broadcastPost behaviour by
// firing all /reduce requests concurrently and verifying the merged word counts.
func TestConcurrentReducePipeline(t *testing.T) {
	const n = 4
	servers := make([]*httptest.Server, n)
	peers := make([]string, n)
	for i := range n {
		servers[i] = httptest.NewServer(newServer(wordCountMap, wordCountReduce, t.TempDir()).handler())
		defer servers[i].Close()
		peers[i] = strings.TrimPrefix(servers[i].URL, "http://")
	}

	chunks := []string{
		"the quick brown fox\n",
		"the lazy dog\n",
		"quick brown cat\n",
		"fox and dog\n",
	}
	for i, ts := range servers {
		mustPost(t, dataURL(ts.URL, i), "application/octet-stream", []byte(chunks[i]))
	}

	for i, ts := range servers {
		mustMap(t, ts.URL, i, peers)
	}

	// All reduces run concurrently; each worker pulls /intermediate from all peers.
	var reduceWg sync.WaitGroup
	for i, ts := range servers {
		reduceWg.Add(1)
		go func(slotID int, s *httptest.Server) {
			defer reduceWg.Done()
			mustReduce(t, s.URL, slotID, nil)
		}(i, ts)
	}
	reduceWg.Wait()

	merged := make(map[string]int)
	for i, ts := range servers {
		for word, count := range getResult(t, ts.URL, i) {
			merged[word] += count
		}
	}

	// Tokens from: "the quick brown fox\nthe lazy dog\nquick brown cat\nfox and dog\n"
	want := map[string]int{
		"the":   2,
		"quick": 2,
		"brown": 2,
		"fox":   2,
		"lazy":  1,
		"dog":   2,
		"cat":   1,
		"and":   1,
	}
	for word, count := range want {
		if merged[word] != count {
			t.Errorf("word %q: want %d, got %d", word, count, merged[word])
		}
	}
	if len(merged) != len(want) {
		t.Errorf("merged has %d words, want %d: %v", len(merged), len(want), merged)
	}
}

// TestWorkersReceiveEmptyData verifies that workers with no input (empty chunk) don't
// produce spurious output and don't break the reduce cycle.
func TestWorkersReceiveEmptyData(t *testing.T) {
	const n = 3
	servers := make([]*httptest.Server, n)
	peers := make([]string, n)
	for i := range n {
		servers[i] = httptest.NewServer(newServer(wordCountMap, wordCountReduce, t.TempDir()).handler())
		defer servers[i].Close()
		peers[i] = strings.TrimPrefix(servers[i].URL, "http://")
	}

	// Only the first worker receives data; the others get an empty payload.
	mustPost(t, dataURL(servers[0].URL, 0), "application/octet-stream", []byte("hello world\nhello\n"))
	mustPost(t, dataURL(servers[1].URL, 1), "application/octet-stream", []byte{})
	mustPost(t, dataURL(servers[2].URL, 2), "application/octet-stream", []byte{})

	for i, ts := range servers {
		mustMap(t, ts.URL, i, peers)
	}

	for i, ts := range servers {
		mustReduce(t, ts.URL, i, nil)
	}

	merged := make(map[string]int)
	for i, ts := range servers {
		for word, count := range getResult(t, ts.URL, i) {
			merged[word] += count
		}
	}

	want := map[string]int{"hello": 2, "world": 1}
	for word, count := range want {
		if merged[word] != count {
			t.Errorf("word %q: want %d, got %d", word, count, merged[word])
		}
	}
	if len(merged) != len(want) {
		t.Errorf("unexpected words in result: %v", merged)
	}
}

// TestUnicodeInput verifies that non-ASCII letters (accented characters) are treated
// as part of a word token, matching unicode.IsLetter semantics in wordCountMap.
func TestUnicodeInput(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	input := "café naïve résumé\nfoo café bar\n"
	peer := strings.TrimPrefix(ts.URL, "http://")
	mustPost(t, dataURL(ts.URL, 0), "application/octet-stream", []byte(input))
	mustMap(t, ts.URL, 0, []string{peer})
	mustReduce(t, ts.URL, 0, nil)

	got := getResult(t, ts.URL, 0)
	for word, wantCount := range map[string]int{"café": 2, "naïve": 1, "résumé": 1, "foo": 1, "bar": 1} {
		if got[word] != wantCount {
			t.Errorf("%q: want %d, got %d", word, wantCount, got[word])
		}
	}
}

// TestHighFrequencyWord checks that a single word repeated many times is counted correctly.
func TestHighFrequencyWord(t *testing.T) {
	const count = 10_000
	ts := newTestServer(t)
	defer ts.Close()

	var sb strings.Builder
	for range count {
		fmt.Fprintln(&sb, "word")
	}

	peer := strings.TrimPrefix(ts.URL, "http://")
	mustPost(t, dataURL(ts.URL, 0), "application/octet-stream", []byte(sb.String()))
	mustMap(t, ts.URL, 0, []string{peer})
	mustReduce(t, ts.URL, 0, nil)

	got := getResult(t, ts.URL, 0)
	if got["word"] != count {
		t.Errorf("word: want %d, got %d", count, got["word"])
	}
	if len(got) != 1 {
		t.Errorf("want exactly 1 distinct word, got %d: %v", len(got), got)
	}
}

// TestLargeInput verifies the pipeline handles more than a trivial amount of data.
func TestLargeInput(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	var sb strings.Builder
	for range 1000 {
		fmt.Fprintln(&sb, "the quick brown fox jumps over the lazy dog")
	}
	input := sb.String()

	peer := strings.TrimPrefix(ts.URL, "http://")
	mustPost(t, dataURL(ts.URL, 0), "application/octet-stream", []byte(input))
	mustMap(t, ts.URL, 0, []string{peer})
	mustReduce(t, ts.URL, 0, nil)

	got := getResult(t, ts.URL, 0)
	if got["the"] != 2000 {
		t.Errorf("the: want 2000, got %d", got["the"])
	}
	if got["fox"] != 1000 {
		t.Errorf("fox: want 1000, got %d", got["fox"])
	}
}

func TestSingleWorkerHostsMultipleSlots(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	peer := strings.TrimPrefix(ts.URL, "http://")
	peers := []string{peer, peer}

	mustPost(t, dataURL(ts.URL, 0), "application/octet-stream", []byte("alpha beta\nalpha\n"))
	mustPost(t, dataURL(ts.URL, 1), "application/octet-stream", []byte("beta gamma\ngamma\n"))

	mustMap(t, ts.URL, 0, peers)
	mustMap(t, ts.URL, 1, peers)
	mustReduce(t, ts.URL, 0, peers)
	mustReduce(t, ts.URL, 1, peers)

	merged := make(map[string]int)
	for _, slotID := range []int{0, 1} {
		for word, count := range getResult(t, ts.URL, slotID) {
			merged[word] += count
		}
	}

	want := map[string]int{"alpha": 2, "beta": 2, "gamma": 2}
	for word, count := range want {
		if merged[word] != count {
			t.Errorf("word %q: want %d, got %d", word, count, merged[word])
		}
	}
	if len(merged) != len(want) {
		t.Errorf("merged has %d words, want %d: %v", len(merged), len(want), merged)
	}
}
