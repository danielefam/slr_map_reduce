package docdensity

import (
	"strconv"
	"testing"
)

func mapLine(t *testing.T, line string) map[string]string {
	t.Helper()
	out := make(map[string]string)
	for _, kv := range NewMapper().Map("0", line) {
		if _, err := strconv.Atoi(kv.Value); err != nil {
			t.Fatalf("mapper must emit integer values, got %q for key %q", kv.Value, kv.Key)
		}
		out[kv.Key] = kv.Value
	}
	return out
}

func TestMapCountsCharacters(t *testing.T) {
	// "ab c!" -> total=5, alnum=3, other=2 (space + '!')
	got := mapLine(t, "ab c!")
	want := map[string]string{
		"stat:lines":           "1",
		"stat:chars.total":     "5",
		"stat:chars.alnum":     "3",
		"stat:chars.other":     "2",
		"hist:len.00000-00079": "1",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("key %s = %q, want %q (all: %v)", k, got[k], v, got)
		}
	}
}

func TestMapCountsRunesNotBytes(t *testing.T) {
	got := mapLine(t, "héllo") // 5 runes, all letters
	if got["stat:chars.total"] != "5" || got["stat:chars.alnum"] != "5" {
		t.Errorf("unicode counts wrong: %v", got)
	}
}

func TestMapSkipsBlankAndWARCHeaders(t *testing.T) {
	for _, line := range []string{
		"",
		"   ",
		"WARC/1.0",
		"WARC-Target-URI: http://example.com/",
		"Content-Type: text/plain",
		"Content-Length: 99",
	} {
		if got := NewMapper().Map("0", line); len(got) != 0 {
			t.Errorf("line %q should emit nothing, got %v", line, got)
		}
	}
}

func TestLengthBuckets(t *testing.T) {
	cases := map[int]string{
		0:     "00000-00079",
		79:    "00000-00079",
		80:    "00080-00199",
		499:   "00200-00499",
		999:   "00500-00999",
		4999:  "01000-04999",
		12345: "05000-plus",
	}
	for n, want := range cases {
		if got := lengthBucket(n); got != want {
			t.Errorf("lengthBucket(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestReduceSumsValues(t *testing.T) {
	got := NewReducer().Reduce("stat:chars.total", []string{"10", "20", "12"})
	if got != "42" {
		t.Errorf("Reduce = %q, want \"42\"", got)
	}
	if _, err := strconv.Atoi(got); err != nil {
		t.Errorf("Reduce output %q is not an integer string", got)
	}
}

func TestReduceIgnoresMalformedValues(t *testing.T) {
	got := NewReducer().Reduce("stat:lines", []string{"1", "oops", "2"})
	if got != "3" {
		t.Errorf("Reduce = %q, want \"3\"", got)
	}
}
