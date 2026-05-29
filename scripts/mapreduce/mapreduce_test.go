package main

import (
	"bytes"
	"strings"
	"testing"
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
