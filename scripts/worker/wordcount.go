package main

import (
	"scripts/jobs/wordcount"
)

var (
	wordCountMapper  = wordcount.NewMapper()
	wordCountReducer = wordcount.NewReducer()
)

// wordCountMap adapts the sample word-count job for existing worker tests.
func wordCountMap(docID, text string) []KeyValue {
	return wordCountMapper.Map(docID, text)
}

// wordCountReduce adapts the sample word-count job for existing worker tests.
func wordCountReduce(key string, values []string) string {
	return wordCountReducer.Reduce(key, values)
}
