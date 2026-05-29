package main

import (
	"strconv"
	"strings"
	"unicode"
)

// wordCountMap emits (word, "1") for every alphanumeric token in text.
func wordCountMap(_ string, text string) []KeyValue {
	words := strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	out := make([]KeyValue, 0, len(words))
	for _, w := range words {
		if w != "" {
			out = append(out, KeyValue{Key: strings.ToLower(w), Value: "1"})
		}
	}
	return out
}

// wordCountReduce counts the number of values emitted for a word.
func wordCountReduce(_ string, values []string) string {
	return strconv.Itoa(len(values))
}
