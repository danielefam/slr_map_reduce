package langdetect

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

func TestMapDetectsEnglish(t *testing.T) {
	got := mapLine(t, "the cat sat on the mat and it was happy")
	if got["lang:en"] != 1 {
		t.Errorf("want lang:en=1, got %v", got)
	}
}

func TestMapDetectsFrench(t *testing.T) {
	got := mapLine(t, "le chat est dans la maison avec les enfants")
	if got["lang:fr"] != 1 {
		t.Errorf("want lang:fr=1, got %v", got)
	}
}

func TestMapDetectsGerman(t *testing.T) {
	got := mapLine(t, "der Hund ist nicht mit der Katze auf dem Sofa")
	if got["lang:de"] != 1 {
		t.Errorf("want lang:de=1, got %v", got)
	}
}

func TestMapUnknownWhenNoStopWords(t *testing.T) {
	got := mapLine(t, "zxqv wkrt plmn vbgh")
	if got["lang:unknown"] != 1 {
		t.Errorf("want lang:unknown=1, got %v", got)
	}
}

func TestMapSkipsShortLines(t *testing.T) {
	if got := mapLine(t, "hello world"); len(got) != 0 {
		t.Errorf("short line should emit nothing, got %v", got)
	}
}

func TestMapSkipsBlankAndWARCHeaders(t *testing.T) {
	for _, line := range []string{
		"",
		"   ",
		"WARC/1.0",
		"WARC-Target-URI: http://example.com/",
		"Content-Type: text/plain",
		"Content-Length: 1234",
	} {
		if got := mapLine(t, line); len(got) != 0 {
			t.Errorf("line %q should emit nothing, got %v", line, got)
		}
	}
}

func TestReduceCountsValues(t *testing.T) {
	got := NewReducer().Reduce("lang:en", []string{"1", "1", "1"})
	if got != "3" {
		t.Errorf("Reduce = %q, want \"3\"", got)
	}
	if _, err := strconv.Atoi(got); err != nil {
		t.Errorf("Reduce output %q is not an integer string", got)
	}
}
