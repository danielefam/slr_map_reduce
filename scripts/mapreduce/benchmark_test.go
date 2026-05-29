package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

// Shared inputs built once for all benchmarks.
var (
	bench1KBInput []byte
	bench1MBInput []byte
)

func init() {
	const line = "the quick brown fox jumps over the lazy dog\n"
	var sb strings.Builder
	for sb.Len() < 1024 {
		sb.WriteString(line)
	}
	bench1KBInput = []byte(sb.String())

	sb.Reset()
	for sb.Len() < 1024*1024 {
		sb.WriteString(line)
	}
	bench1MBInput = []byte(sb.String())
}

// BenchmarkSplitIntoChunksSmall splits a ~1 KB input into 64-byte chunks.
func BenchmarkSplitIntoChunksSmall(b *testing.B) {
	for range b.N {
		_, _ = splitIntoChunks(bytes.NewReader(bench1KBInput), 64)
	}
}

// BenchmarkSplitIntoChunksMedium splits a ~1 MB input into 64 KB chunks.
func BenchmarkSplitIntoChunksMedium(b *testing.B) {
	for range b.N {
		_, _ = splitIntoChunks(bytes.NewReader(bench1MBInput), 64*1024)
	}
}

// BenchmarkCollectResults measures fetching /result from N mock workers,
// merging counts, sorting, and writing the output file.
func BenchmarkCollectResults(b *testing.B) {
	const n = 10

	// Build per-server result payloads: each server owns a non-overlapping key range.
	payloads := make([]string, n)
	for i := range n {
		var sb strings.Builder
		for j := range 500 {
			fmt.Fprintf(&sb, "word%05d\t%d\n", j*n+i, j+1)
		}
		payloads[i] = sb.String()
	}

	servers := make([]*httptest.Server, n)
	peers := make([]string, n)
	for i := range n {
		payload := payloads[i]
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, payload)
		}))
		peers[i] = strings.TrimPrefix(servers[i].URL, "http://")
	}
	defer func() {
		for _, ts := range servers {
			ts.Close()
		}
	}()

	outFile := filepath.Join(b.TempDir(), "result.txt")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := collectResults(peers, outFile); err != nil {
			b.Fatal(err)
		}
	}
}
