package main

import (
	"testing"
)

func TestAssignChunksRoundRobin(t *testing.T) {
	slots := []*slot{{id: 0}, {id: 1}, {id: 2}}
	chunks := [][]byte{
		[]byte("a\n"),
		[]byte("b\n"),
		[]byte("c\n"),
		[]byte("d\n"),
		[]byte("e\n"),
	}
	assignChunksRoundRobin(slots, chunks)

	// slot 0: chunks 0,3 → "a\nd\n"; slot 1: chunks 1,4 → "b\ne\n"; slot 2: chunk 2 → "c\n"
	want := []string{"a\nd\n", "b\ne\n", "c\n"}
	for i, s := range slots {
		if string(s.chunk) != want[i] {
			t.Errorf("slot %d chunk = %q, want %q", i, s.chunk, want[i])
		}
	}
}

func TestAssignChunksRoundRobinInsertsNewline(t *testing.T) {
	slots := []*slot{{id: 0}}
	// Neither chunk ends in a newline; concatenation must not merge words.
	chunks := [][]byte{[]byte("foo"), []byte("bar")}
	assignChunksRoundRobin(slots, chunks)
	if got := string(slots[0].chunk); got != "foo\nbar\n" {
		t.Errorf("chunk = %q, want %q", got, "foo\nbar\n")
	}
}

func TestAssignChunksRoundRobinFewerChunksThanSlots(t *testing.T) {
	slots := []*slot{{id: 0}, {id: 1}, {id: 2}}
	chunks := [][]byte{[]byte("only\n")}
	assignChunksRoundRobin(slots, chunks)
	if string(slots[0].chunk) != "only\n" {
		t.Errorf("slot 0 chunk = %q, want %q", slots[0].chunk, "only\n")
	}
	if len(slots[1].chunk) != 0 || len(slots[2].chunk) != 0 {
		t.Errorf("expected empty payloads for slots 1 and 2, got %q / %q", slots[1].chunk, slots[2].chunk)
	}
}

func TestAssignURLsRoundRobin(t *testing.T) {
	slots := []*slot{{id: 0}, {id: 1}, {id: 2}}
	urls := []string{"u0", "u1", "u2", "u3", "u4"}
	assignURLsRoundRobin(slots, urls)

	want := [][]string{
		{"u0", "u3"},
		{"u1", "u4"},
		{"u2"},
	}
	for i, s := range slots {
		if len(s.urls) != len(want[i]) {
			t.Fatalf("slot %d got %d urls, want %d", i, len(s.urls), len(want[i]))
		}
		for j := range want[i] {
			if s.urls[j] != want[i][j] {
				t.Errorf("slot %d urls[%d] = %q, want %q", i, j, s.urls[j], want[i][j])
			}
		}
	}
}

func TestApplyCommonCrawlLimits(t *testing.T) {
	urls := []string{"u0", "u1", "u2", "u3"}
	got := applyCommonCrawlLimits(urls, 3, 2)
	if len(got) != 2 {
		t.Fatalf("got %d urls, want 2", len(got))
	}
	if got[0] != "u0" || got[1] != "u1" {
		t.Fatalf("got %v, want [u0 u1]", got)
	}
	if len(urls) != 4 {
		t.Fatalf("original slice should stay intact, got len=%d", len(urls))
	}
}
