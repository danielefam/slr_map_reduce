package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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

// getResult fetches /result and returns the parsed word→count map.
func getResult(t *testing.T, baseURL string) map[string]int {
	t.Helper()
	resp, err := http.Get(baseURL + "/result")
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

	mustPost(t, ts.URL+"/data", "application/octet-stream", []byte("hello world\n"))
}

// TestSingleWorkerPipeline runs the full map→shuffle→reduce→result cycle on one worker.
func TestSingleWorkerPipeline(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	input := "hello world\nhello go\nworld cup\n"
	mustPost(t, ts.URL+"/data", "application/octet-stream", []byte(input))

	peer := strings.TrimPrefix(ts.URL, "http://")
	mapReq, _ := json.Marshal(mapRequest{ID: 0, Peers: []string{peer}})
	mustPost(t, ts.URL+"/map", "application/json", mapReq)
	mustPost(t, ts.URL+"/shuffle", "application/octet-stream", nil)
	mustPost(t, ts.URL+"/reduce", "application/octet-stream", nil)

	got := getResult(t, ts.URL)

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
		mustPost(t, ts.URL+"/data", "application/octet-stream", []byte(chunks[i]))
	}

	// Map phase.
	for i, ts := range servers {
		req, _ := json.Marshal(mapRequest{ID: i, Peers: peers})
		mustPost(t, ts.URL+"/map", "application/json", req)
	}

	// Shuffle phase (sequential is correct: hash routing is idempotent).
	for _, ts := range servers {
		mustPost(t, ts.URL+"/shuffle", "application/octet-stream", nil)
	}

	// Reduce phase.
	for _, ts := range servers {
		mustPost(t, ts.URL+"/reduce", "application/octet-stream", nil)
	}

	// Merge results from all workers.
	merged := make(map[string]int)
	for _, ts := range servers {
		for word, count := range getResult(t, ts.URL) {
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

func TestShuffleRecvMerges(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	peer := strings.TrimPrefix(ts.URL, "http://")
	mapReq, _ := json.Marshal(mapRequest{ID: 0, Peers: []string{peer}})
	mustPost(t, ts.URL+"/data", "application/octet-stream", []byte("hello\n"))
	mustPost(t, ts.URL+"/map", "application/json", mapReq)

	// Manually inject additional KV pairs via /shuffle/recv.
	extra := []KeyValue{{Key: "hello", Value: "1"}, {Key: "new", Value: "1"}}
	body, _ := json.Marshal(extra)
	mustPost(t, ts.URL+"/shuffle/recv", "application/json", body)

	mustPost(t, ts.URL+"/reduce", "application/octet-stream", nil)

	got := getResult(t, ts.URL)
	if got["hello"] != 2 {
		t.Errorf("hello: want 2, got %d", got["hello"])
	}
	if got["new"] != 1 {
		t.Errorf("new: want 1, got %d", got["new"])
	}
}

func TestDataResetsState(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	peer := strings.TrimPrefix(ts.URL, "http://")

	// First job.
	mustPost(t, ts.URL+"/data", "application/octet-stream", []byte("apple apple\n"))
	mapReq, _ := json.Marshal(mapRequest{ID: 0, Peers: []string{peer}})
	mustPost(t, ts.URL+"/map", "application/json", mapReq)
	mustPost(t, ts.URL+"/shuffle", "application/octet-stream", nil)
	mustPost(t, ts.URL+"/reduce", "application/octet-stream", nil)

	// Second job with different data — /data must reset state.
	mustPost(t, ts.URL+"/data", "application/octet-stream", []byte("banana\n"))
	mapReq2, _ := json.Marshal(mapRequest{ID: 0, Peers: []string{peer}})
	mustPost(t, ts.URL+"/map", "application/json", mapReq2)
	mustPost(t, ts.URL+"/shuffle", "application/octet-stream", nil)
	mustPost(t, ts.URL+"/reduce", "application/octet-stream", nil)

	got := getResult(t, ts.URL)
	if got["apple"] != 0 {
		t.Errorf("apple should be gone after reset, got %d", got["apple"])
	}
	if got["banana"] != 1 {
		t.Errorf("banana: want 1, got %d", got["banana"])
	}
}

func TestHandleShuffleNoMapPhase(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/shuffle", "application/octet-stream", bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("shuffle without map: want 400, got %d", resp.StatusCode)
	}
}

// TestConcurrentShufflePipeline mirrors the production broadcastPost behaviour by
// firing all /shuffle requests concurrently and verifying the merged word counts.
func TestConcurrentShufflePipeline(t *testing.T) {
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
		mustPost(t, ts.URL+"/data", "application/octet-stream", []byte(chunks[i]))
	}

	for i, ts := range servers {
		req, _ := json.Marshal(mapRequest{ID: i, Peers: peers})
		mustPost(t, ts.URL+"/map", "application/json", req)
	}

	// All shuffles run concurrently, just like the real orchestrator's broadcastPost.
	var shuffleWg sync.WaitGroup
	for _, ts := range servers {
		shuffleWg.Add(1)
		go func(s *httptest.Server) {
			defer shuffleWg.Done()
			mustPost(t, s.URL+"/shuffle", "application/octet-stream", nil)
		}(ts)
	}
	shuffleWg.Wait()

	var reduceWg sync.WaitGroup
	for _, ts := range servers {
		reduceWg.Add(1)
		go func(s *httptest.Server) {
			defer reduceWg.Done()
			mustPost(t, s.URL+"/reduce", "application/octet-stream", nil)
		}(ts)
	}
	reduceWg.Wait()

	merged := make(map[string]int)
	for _, ts := range servers {
		for word, count := range getResult(t, ts.URL) {
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
// produce spurious output and don't break the shuffle/reduce cycle.
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
	mustPost(t, servers[0].URL+"/data", "application/octet-stream", []byte("hello world\nhello\n"))
	mustPost(t, servers[1].URL+"/data", "application/octet-stream", []byte{})
	mustPost(t, servers[2].URL+"/data", "application/octet-stream", []byte{})

	for i, ts := range servers {
		req, _ := json.Marshal(mapRequest{ID: i, Peers: peers})
		mustPost(t, ts.URL+"/map", "application/json", req)
	}

	var wg sync.WaitGroup
	for _, ts := range servers {
		wg.Add(1)
		go func(s *httptest.Server) {
			defer wg.Done()
			mustPost(t, s.URL+"/shuffle", "application/octet-stream", nil)
		}(ts)
	}
	wg.Wait()

	for _, ts := range servers {
		mustPost(t, ts.URL+"/reduce", "application/octet-stream", nil)
	}

	merged := make(map[string]int)
	for _, ts := range servers {
		for word, count := range getResult(t, ts.URL) {
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
	mustPost(t, ts.URL+"/data", "application/octet-stream", []byte(input))
	mapReq, _ := json.Marshal(mapRequest{ID: 0, Peers: []string{peer}})
	mustPost(t, ts.URL+"/map", "application/json", mapReq)
	mustPost(t, ts.URL+"/shuffle", "application/octet-stream", nil)
	mustPost(t, ts.URL+"/reduce", "application/octet-stream", nil)

	got := getResult(t, ts.URL)
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
	mustPost(t, ts.URL+"/data", "application/octet-stream", []byte(sb.String()))
	mapReq, _ := json.Marshal(mapRequest{ID: 0, Peers: []string{peer}})
	mustPost(t, ts.URL+"/map", "application/json", mapReq)
	mustPost(t, ts.URL+"/shuffle", "application/octet-stream", nil)
	mustPost(t, ts.URL+"/reduce", "application/octet-stream", nil)

	got := getResult(t, ts.URL)
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
	mustPost(t, ts.URL+"/data", "application/octet-stream", []byte(input))
	mapReq, _ := json.Marshal(mapRequest{ID: 0, Peers: []string{peer}})
	mustPost(t, ts.URL+"/map", "application/json", mapReq)
	mustPost(t, ts.URL+"/shuffle", "application/octet-stream", nil)
	mustPost(t, ts.URL+"/reduce", "application/octet-stream", nil)

	got := getResult(t, ts.URL)
	if got["the"] != 2000 {
		t.Errorf("the: want 2000, got %d", got["the"])
	}
	if got["fox"] != 1000 {
		t.Errorf("fox: want 1000, got %d", got["fox"])
	}
}
