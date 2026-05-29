package main

import (
	"testing"
)

func TestWordCountMapTokenises(t *testing.T) {
	pairs := wordCountMap("0", "Hello, World! hello")

	counts := make(map[string]int)
	for _, kv := range pairs {
		if kv.Value != "1" {
			t.Errorf("expected value '1', got %q for key %q", kv.Value, kv.Key)
		}
		counts[kv.Key]++
	}

	if counts["hello"] != 2 {
		t.Errorf("hello: want 2, got %d", counts["hello"])
	}
	if counts["world"] != 1 {
		t.Errorf("world: want 1, got %d", counts["world"])
	}
}

func TestWordCountMapLowercases(t *testing.T) {
	pairs := wordCountMap("0", "GO Go go")
	counts := make(map[string]int)
	for _, kv := range pairs {
		counts[kv.Key]++
	}
	if counts["go"] != 3 {
		t.Errorf("go: want 3, got %d", counts["go"])
	}
	if counts["GO"] != 0 && counts["Go"] != 0 {
		t.Error("wordCountMap should lowercase all tokens")
	}
}

func TestWordCountMapSkipsPunctuation(t *testing.T) {
	pairs := wordCountMap("0", "hello, world! foo-bar")
	keys := make(map[string]bool)
	for _, kv := range pairs {
		keys[kv.Key] = true
	}
	for _, unexpected := range []string{",", "!", "-", "foo-bar"} {
		if keys[unexpected] {
			t.Errorf("unexpected token %q in output", unexpected)
		}
	}
}

func TestWordCountMapEmpty(t *testing.T) {
	if pairs := wordCountMap("0", ""); len(pairs) != 0 {
		t.Errorf("empty input: want 0 pairs, got %d", len(pairs))
	}
}

func TestWordCountMapOnlyPunctuation(t *testing.T) {
	if pairs := wordCountMap("0", "!!! ,,, ---"); len(pairs) != 0 {
		t.Errorf("punctuation-only input: want 0 pairs, got %d", len(pairs))
	}
}

func TestWordCountReduceCounts(t *testing.T) {
	tests := []struct {
		values []string
		want   string
	}{
		{[]string{"1", "1", "1"}, "3"},
		{[]string{"1"}, "1"},
		{[]string{}, "0"},
	}
	for _, tt := range tests {
		got := wordCountReduce("word", tt.values)
		if got != tt.want {
			t.Errorf("wordCountReduce(%v) = %q, want %q", tt.values, got, tt.want)
		}
	}
}
