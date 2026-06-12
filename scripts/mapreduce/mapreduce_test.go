package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSplitIntoChunks_SmallInput(t *testing.T) {
	data := "hello\nworld\n"
	chunks, err := splitIntoChunks(bytes.NewReader([]byte(data)), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 {
		t.Fatalf("want 1 chunk, got %d", len(chunks))
	}
	if string(chunks[0]) != data {
		t.Errorf("chunk = %q, want %q", chunks[0], data)
	}
}

func TestSplitIntoChunks_SplitsOnNewline(t *testing.T) {
	// Two 11-byte lines; maxSize=15 forces a split after the first line.
	line1 := "aaaaaaaaaa\n" // 11 bytes
	line2 := "bbbbbbbbbb\n" // 11 bytes
	data := line1 + line2

	chunks, err := splitIntoChunks(bytes.NewReader([]byte(data)), 15)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 2 {
		t.Fatalf("want 2 chunks, got %d: %q", len(chunks), chunks)
	}
	if string(chunks[0]) != line1 {
		t.Errorf("chunk[0] = %q, want %q", chunks[0], line1)
	}
	if string(chunks[1]) != line2 {
		t.Errorf("chunk[1] = %q, want %q", chunks[1], line2)
	}
}

func TestSplitIntoChunks_ReassemblesExactly(t *testing.T) {
	original := strings.Repeat("word count test\n", 200)
	chunks, err := splitIntoChunks(bytes.NewReader([]byte(original)), 64)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) < 2 {
		t.Fatal("expected multiple chunks for this input size")
	}
	var reassembled strings.Builder
	for _, c := range chunks {
		reassembled.Write(c)
	}
	if reassembled.String() != original {
		t.Error("reassembled chunks do not equal the original input")
	}
}

func TestSplitIntoChunks_EmptyInput(t *testing.T) {
	chunks, err := splitIntoChunks(bytes.NewReader(nil), 64)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 0 {
		t.Errorf("empty input: want 0 chunks, got %d", len(chunks))
	}
}

func TestSplitIntoChunks_NoChunkExceedsMaxSize(t *testing.T) {
	maxSize := 50
	original := strings.Repeat("hello world\n", 100)
	chunks, err := splitIntoChunks(bytes.NewReader([]byte(original)), maxSize)
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range chunks {
		if len(c) > maxSize {
			t.Errorf("chunk[%d] has %d bytes, exceeds maxSize %d", i, len(c), maxSize)
		}
	}
}

func TestChunkForWorker_InBounds(t *testing.T) {
	chunks := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	for i, want := range []string{"a", "b", "c"} {
		got := chunkForWorker(chunks, i)
		if string(got) != want {
			t.Errorf("worker %d: want %q, got %q", i, want, got)
		}
	}
}

func TestChunkForWorker_OutOfBounds(t *testing.T) {
	chunks := [][]byte{[]byte("only")}
	for _, i := range []int{1, 5, 100} {
		got := chunkForWorker(chunks, i)
		if len(got) != 0 {
			t.Errorf("worker %d: want empty chunk, got %q", i, got)
		}
	}
}

func TestChunkForWorker_EmptyChunks(t *testing.T) {
	got := chunkForWorker(nil, 0)
	if len(got) != 0 {
		t.Errorf("nil chunks: want empty, got %q", got)
	}
}

func TestResolveCommonCrawlURLs(t *testing.T) {
	oldIndexURL := commonCrawlIndexURL
	oldDataURL := commonCrawlDataURL
	t.Cleanup(func() {
		commonCrawlIndexURL = oldIndexURL
		commonCrawlDataURL = oldDataURL
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/collinfo.json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]commonCrawlCollection{
			{ID: "CC-MAIN-2025-13"},
			{ID: "CC-MAIN-2026-05"},
			{ID: "CC-MAIN-2024-99"},
		})
	})
	mux.HandleFunc("/crawl-data/CC-MAIN-2026-05/wet.paths.gz", func(w http.ResponseWriter, _ *http.Request) {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		_, _ = gz.Write([]byte("crawl-data/CC-MAIN-2026-05/segments/b.wet.gz\ncrawl-data/CC-MAIN-2026-05/segments/a.wet.gz\n"))
		_ = gz.Close()
		_, _ = w.Write(buf.Bytes())
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	commonCrawlIndexURL = server.URL + "/collinfo.json"
	commonCrawlDataURL = server.URL

	crawl, urls, err := resolveCommonCrawlURLs("", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if crawl != "CC-MAIN-2026-05" {
		t.Fatalf("crawl = %q, want %q", crawl, "CC-MAIN-2026-05")
	}
	want := []string{server.URL + "/crawl-data/CC-MAIN-2026-05/segments/a.wet.gz"}
	if len(urls) != len(want) {
		t.Fatalf("got %d urls, want %d", len(urls), len(want))
	}
	for i := range want {
		if urls[i] != want[i] {
			t.Errorf("urls[%d] = %q, want %q", i, urls[i], want[i])
		}
	}
}

func TestWaitHealthyWithConfig_ProbesPeersInParallel(t *testing.T) {
	t.Parallel()

	var listens []net.Listener
	t.Cleanup(func() {
		for _, ln := range listens {
			_ = ln.Close()
		}
	})

	startSlowServer := func() string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listens = append(listens, ln)
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(100 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})
		server := &http.Server{Handler: mux}
		go func() {
			_ = server.Serve(ln)
		}()
		t.Cleanup(func() {
			_ = server.Close()
		})
		return ln.Addr().String()
	}

	hosts := []string{"h1", "h2", "h3"}
	peers := []string{startSlowServer(), startSlowServer(), startSlowServer()}

	start := time.Now()
	gotHosts, gotPeers, err := waitHealthyWithConfig(hosts, peers, len(peers), 1, 0, &http.Client{Timeout: time.Second})
	if err != nil {
		t.Fatalf("waitHealthyWithConfig: %v", err)
	}
	if len(gotHosts) != len(hosts) || len(gotPeers) != len(peers) {
		t.Fatalf("got %d hosts/%d peers, want %d/%d", len(gotHosts), len(gotPeers), len(hosts), len(peers))
	}
	if elapsed := time.Since(start); elapsed >= 250*time.Millisecond {
		t.Fatalf("health checks took %v, expected parallel probes to finish well under 250ms", elapsed)
	}
}

func TestReadHosts(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    []string
	}{
		{
			name:    "normal hosts",
			content: "host1.example.com\nhost2.example.com\nhost3.example.com\n",
			want:    []string{"host1.example.com", "host2.example.com", "host3.example.com"},
		},
		{
			name:    "blank lines ignored",
			content: "host1.example.com\n\nhost2.example.com\n\n",
			want:    []string{"host1.example.com", "host2.example.com"},
		},
		{
			name:    "leading and trailing spaces trimmed",
			content: "  host1.example.com  \n  host2.example.com\n",
			want:    []string{"host1.example.com", "host2.example.com"},
		},
		{
			name:    "empty file",
			content: "",
			want:    nil,
		},
		{
			name:    "only whitespace lines",
			content: "   \n   \n",
			want:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := os.CreateTemp(t.TempDir(), "hosts*.txt")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.WriteString(tt.content); err != nil {
				t.Fatal(err)
			}
			f.Close()

			got, err := readHosts(f.Name())
			if err != nil {
				t.Fatalf("readHosts: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("[%d]: got %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestReadHostsNotFound(t *testing.T) {
	_, err := readHosts(filepath.Join(t.TempDir(), "nonexistent.txt"))
	if err == nil {
		t.Error("want error for missing file, got nil")
	}
}

func TestFirstErr(t *testing.T) {
	err1 := fmt.Errorf("error one")
	err2 := fmt.Errorf("error two")
	tests := []struct {
		name string
		errs []error
		want error
	}{
		{"all nil", []error{nil, nil, nil}, nil},
		{"empty slice", nil, nil},
		{"first is error", []error{err1, nil, nil}, err1},
		{"last is error", []error{nil, nil, err2}, err2},
		{"multiple errors returns first", []error{err1, err2}, err1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := firstErr(tt.errs); got != tt.want {
				t.Errorf("firstErr = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestCollectResults verifies that results from multiple mock workers are merged,
// counted correctly, and written sorted by descending count then alphabetical key.
func TestCollectResults(t *testing.T) {
	makeHandler := func(result string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			fmt.Fprint(w, result)
		})
	}

	ts1 := httptest.NewServer(makeHandler("apple\t3\nbanana\t1\n"))
	defer ts1.Close()
	ts2 := httptest.NewServer(makeHandler("apple\t2\ncherry\t5\n"))
	defer ts2.Close()

	peer1 := strings.TrimPrefix(ts1.URL, "http://")
	peer2 := strings.TrimPrefix(ts2.URL, "http://")
	outFile := filepath.Join(t.TempDir(), "result.txt")

	if err := collectResults([]string{peer1, peer2}, outFile); err != nil {
		t.Fatalf("collectResults: %v", err)
	}

	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	// Merged: apple=5, cherry=5, banana=1.
	// Sorted: descending count, then alphabetical key → apple, cherry, banana.
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 3 {
		t.Fatalf("want 3 lines, got %d:\n%s", len(lines), data)
	}
	want := []string{"apple\t5", "cherry\t5", "banana\t1"}
	for i, line := range lines {
		if line != want[i] {
			t.Errorf("line[%d]: want %q, got %q", i, want[i], line)
		}
	}
}
