package wordcount

import (
	"strconv"
	"strings"
	"unicode"

	"scripts/mrjob"
)

// Mapper implements a word-count map phase.
type Mapper struct{}

// NewMapper returns the mapper implementation exported by this job package.
func NewMapper() mrjob.Mapper {
	return Mapper{}
}

// Map emits (word, "1") for every alphanumeric token in text.
func (Mapper) Map(_ string, text string) []mrjob.KeyValue {
	words := strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	out := make([]mrjob.KeyValue, 0, len(words))
	for _, w := range words {
		if w != "" {
			out = append(out, mrjob.KeyValue{Key: strings.ToLower(w), Value: "1"})
		}
	}
	return out
}

// Reducer implements the word-count reduce phase.
type Reducer struct{}

// NewReducer returns the reducer implementation exported by this job package.
func NewReducer() mrjob.Reducer {
	return Reducer{}
}

// Reduce sums the numeric values emitted for a word.
func (Reducer) Reduce(_ string, values []string) string {
	sum := 0
	for _, v := range values {
		if c, err := strconv.Atoi(v); err == nil {
			sum += c
		}
	}
	return strconv.Itoa(sum)
}
