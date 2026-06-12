package main

import (
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
	result, err := runMap(strings.NewReader(data), wordCountMap)
	if err != nil {
		t.Fatal(err)
	}

	if len(result["hello"]) != 2 {
		t.Errorf("hello: want 2 values, got %d", len(result["hello"]))
	}
	if len(result["world"]) != 1 {
		t.Errorf("world: want 1 value, got %d", len(result["world"]))
	}
	if len(result["go"]) != 1 {
		t.Errorf("go: want 1 value, got %d", len(result["go"]))
	}
}

func TestRunMapEmpty(t *testing.T) {
	result, err := runMap(strings.NewReader(""), wordCountMap)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Errorf("empty input: want 0 keys, got %d", len(result))
	}
}

func TestRunMapEmitsValues(t *testing.T) {
	result, err := runMap(strings.NewReader("a b c\n"), wordCountMap)
	if err != nil {
		t.Fatal(err)
	}
	for key, vals := range result {
		for _, v := range vals {
			if v != "1" {
				t.Errorf("key %q: want value '1', got %q", key, v)
			}
		}
	}
}

func TestRunReduceSorted(t *testing.T) {
	intermediate := map[string][]string{
		"zebra": {"1"},
		"apple": {"1", "1", "1"},
		"mango": {"1", "1"},
	}
	result := runReduce(intermediate, wordCountReduce)

	if len(result) != 3 {
		t.Fatalf("want 3 entries, got %d", len(result))
	}
	// runReduce sorts by key
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
	intermediate := map[string][]string{
		"hello": {"1", "1"},
		"world": {"1"},
	}
	result := runReduce(intermediate, wordCountReduce)

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
