package domainpop

import (
	"strconv"
	"testing"
)

func mapLine(t *testing.T, line string) map[string]int {
	t.Helper()
	out := make(map[string]int)
	for _, kv := range NewMapper().Map("0", line) {
		if kv.Value != "1" {
			t.Fatalf("mapper must emit \"1\" values, got %q", kv.Value)
		}
		out[kv.Key]++
	}
	return out
}

func TestMapWARCTargetURI(t *testing.T) {
	got := mapLine(t, "WARC-Target-URI: https://www.Example.COM:8080/some/page?q=1")
	if got["example.com"] != 1 || len(got) != 1 {
		t.Errorf("want example.com=1, got %v", got)
	}
}

func TestMapSkipsOtherWARCHeaders(t *testing.T) {
	for _, line := range []string{
		"WARC/1.0",
		"WARC-Type: conversion",
		"WARC-Date: 2026-01-01T00:00:00Z",
	} {
		if got := mapLine(t, line); len(got) != 0 {
			t.Errorf("line %q should emit nothing, got %v", line, got)
		}
	}
}

func TestMapFallbackURLsInText(t *testing.T) {
	got := mapLine(t, "see https://foo.org/a and http://bar.net/b, plus https://foo.org/c.")
	if got["foo.org"] != 2 || got["bar.net"] != 1 {
		t.Errorf("want foo.org=2 bar.net=1, got %v", got)
	}
}

func TestMapIgnoresPlainText(t *testing.T) {
	if got := mapLine(t, "no urls in this line at all"); len(got) != 0 {
		t.Errorf("plain text should emit nothing, got %v", got)
	}
}

func TestMapMalformedURI(t *testing.T) {
	if got := mapLine(t, "WARC-Target-URI: %%%not-a-url"); len(got) != 0 {
		t.Errorf("malformed URI should emit nothing, got %v", got)
	}
}

func TestMapEmpty(t *testing.T) {
	if got := mapLine(t, ""); len(got) != 0 {
		t.Errorf("empty line should emit nothing, got %v", got)
	}
}

func TestReduceCountsValues(t *testing.T) {
	got := NewReducer().Reduce("example.com", []string{"1", "1", "1", "1"})
	if got != "4" {
		t.Errorf("Reduce = %q, want \"4\"", got)
	}
	if _, err := strconv.Atoi(got); err != nil {
		t.Errorf("Reduce output %q is not an integer string", got)
	}
}
