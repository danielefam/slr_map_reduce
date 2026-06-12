// Package docdensity computes document-size and character-density statistics.
//
// Per input line the mapper emits integer-valued aggregate counters:
//
//	stat:lines        — 1 per non-empty content line
//	stat:chars.total  — total rune count of the line
//	stat:chars.alnum  — alphanumeric rune count
//	stat:chars.other  — non-alphanumeric rune count
//	hist:len.<bucket> — 1, line-length histogram bucket
//
// The reducer SUMS the integer values per key (unlike wordcount, which counts
// occurrences), so every output value is an integer string and stays
// compatible with the orchestrator's integer-sum merge.
//
// Derived metrics are computed offline from the merged output:
//
//	average line length      = stat:chars.total / stat:lines
//	alphanumeric density     = stat:chars.alnum / stat:chars.total
//	length distribution      = the hist:len.* buckets
package docdensity

import (
	"strconv"
	"strings"
	"unicode"

	"scripts/mrjob"
)

// Mapper implements the document-density map phase.
type Mapper struct{}

// NewMapper returns the mapper implementation exported by this job package.
func NewMapper() mrjob.Mapper {
	return Mapper{}
}

// Map emits the aggregate counters for one line of text.
// WET/WARC metadata lines and blank lines are excluded so the statistics
// describe actual document content.
func (Mapper) Map(_ string, text string) []mrjob.KeyValue {
	line := strings.TrimSpace(text)
	if line == "" || isWARCHeader(line) {
		return nil
	}

	total, alnum := 0, 0
	for _, r := range line {
		total++
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			alnum++
		}
	}

	return []mrjob.KeyValue{
		{Key: "stat:lines", Value: "1"},
		{Key: "stat:chars.total", Value: strconv.Itoa(total)},
		{Key: "stat:chars.alnum", Value: strconv.Itoa(alnum)},
		{Key: "stat:chars.other", Value: strconv.Itoa(total - alnum)},
		{Key: "hist:len." + lengthBucket(total), Value: "1"},
	}
}

// isWARCHeader reports whether the line belongs to WET/WARC record metadata.
func isWARCHeader(line string) bool {
	return strings.HasPrefix(line, "WARC/") ||
		strings.HasPrefix(line, "WARC-") ||
		strings.HasPrefix(line, "Content-Type:") ||
		strings.HasPrefix(line, "Content-Length:")
}

// lengthBucket maps a line length to a fixed histogram bucket label.
// Labels are zero-padded so the lexicographic key order matches the
// numeric bucket order in the final sorted output.
func lengthBucket(n int) string {
	switch {
	case n < 80:
		return "00000-00079"
	case n < 200:
		return "00080-00199"
	case n < 500:
		return "00200-00499"
	case n < 1000:
		return "00500-00999"
	case n < 5000:
		return "01000-04999"
	default:
		return "05000-plus"
	}
}

// Reducer implements the document-density reduce phase.
type Reducer struct{}

// NewReducer returns the reducer implementation exported by this job package.
func NewReducer() mrjob.Reducer {
	return Reducer{}
}

// Reduce sums the integer values emitted for a counter key.
// Non-numeric values are ignored defensively.
// The output is an integer string, as required by the orchestrator merge.
func (Reducer) Reduce(_ string, values []string) string {
	sum := 0
	for _, v := range values {
		n, err := strconv.Atoi(v)
		if err != nil {
			continue
		}
		sum += n
	}
	return strconv.Itoa(sum)
}
