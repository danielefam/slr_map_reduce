package main

import (
	"bufio"
	"os"
	"strings"
	"testing"
)

func TestTargetNodeInRange(t *testing.T) {
	for _, key := range []string{"hello", "world", "go", "mapreduce", "", "a"} {
		for n := 1; n <= 8; n++ {
			node := targetNode(key, n)
			if node < 0 || node >= n {
				t.Errorf("targetNode(%q, %d) = %d, out of range [0, %d)", key, n, node, n)
			}
		}
	}
}

func TestTargetNodeDeterministic(t *testing.T) {
	for _, key := range []string{"hello", "world", "foo", "bar"} {
		a := targetNode(key, 5)
		b := targetNode(key, 5)
		if a != b {
			t.Errorf("targetNode(%q, 5) not deterministic: %d != %d", key, a, b)
		}
	}
}

func TestRunMapBasic(t *testing.T) {
	data := "hello world\nhello go\n"
	var buf strings.Builder
	writers := []*bufio.Writer{bufio.NewWriter(&buf)}
	_, err := runMap(strings.NewReader(data), wordCountMap, 1, writers)
	if err != nil {
		t.Fatal(err)
	}
	writers[0].Flush()
	out := buf.String()
	if strings.Count(out, "hello\t1\n") != 2 {
		t.Errorf("hello: want 2 values, got output:\n%s", out)
	}
	if !strings.Contains(out, "world\t1\n") {
		t.Errorf("world: want 1 value, got output:\n%s", out)
	}
	if !strings.Contains(out, "go\t1\n") {
		t.Errorf("go: want 1 value, got output:\n%s", out)
	}
}

func TestRunMapEmpty(t *testing.T) {
	var buf strings.Builder
	writers := []*bufio.Writer{bufio.NewWriter(&buf)}
	pairs, err := runMap(strings.NewReader(""), wordCountMap, 1, writers)
	if err != nil {
		t.Fatal(err)
	}
	if pairs != 0 {
		t.Errorf("empty input: want 0 keys, got %d", pairs)
	}
}

func TestRunMapEmitsValues(t *testing.T) {
	var buf strings.Builder
	writers := []*bufio.Writer{bufio.NewWriter(&buf)}
	_, err := runMap(strings.NewReader("a b c\n"), wordCountMap, 1, writers)
	if err != nil {
		t.Fatal(err)
	}
	writers[0].Flush()
	out := buf.String()
	for _, expected := range []string{"a\t1\n", "b\t1\n", "c\t1\n"} {
		if !strings.Contains(out, expected) {
			t.Errorf("want %q, got output:\n%s", expected, out)
		}
	}
}

func TestRunReduceSorted(t *testing.T) {
	path := writeTempSortedMap(t, "apple\t1\napple\t1\napple\t1\nmango\t1\nmango\t1\nzebra\t1\n")
	defer os.Remove(path)

	result, err := runReduce(path, wordCountReduce)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 3 {
		t.Fatalf("want 3 entries, got %d", len(result))
	}
	if result[0].Key != "apple" {
		t.Errorf("result[0].Key = %q, want 'apple'", result[0].Key)
	}
	if result[1].Key != "mango" {
		t.Errorf("result[1].Key = %q, want 'mango'", result[1].Key)
	}
	if result[2].Key != "zebra" {
		t.Errorf("result[2].Key = %q, want 'zebra'", result[2].Key)
	}
}

func TestRunReduceValues(t *testing.T) {
	path := writeTempSortedMap(t, "hello\t1\nhello\t1\nworld\t1\n")
	defer os.Remove(path)

	result, err := runReduce(path, wordCountReduce)
	if err != nil {
		t.Fatal(err)
	}

	counts := make(map[string]string)
	for _, kv := range result {
		counts[kv.Key] = kv.Value
	}
	if counts["hello"] != "2" {
		t.Errorf("hello: want '2', got %q", counts["hello"])
	}
	if counts["world"] != "1" {
		t.Errorf("world: want '1', got %q", counts["world"])
	}
}
