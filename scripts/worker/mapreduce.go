// Package main defines the core MapReduce types and logic used by the worker.
package main

import (
	"bufio"
	"hash/fnv"
	"io"
	"sort"
	"strconv"

	"scripts/mrjob"
)

// KeyValue is a single intermediate or output key-value pair.
type KeyValue = mrjob.KeyValue

// MapFunc maps a document (identified by docID) to a list of intermediate KeyValue pairs.
type MapFunc func(docID, text string) []KeyValue

// ReduceFunc reduces all values for a key into a single output string.
type ReduceFunc func(key string, values []string) string

func mapFuncFrom(mapper mrjob.Mapper) MapFunc {
	return mapper.Map
}

func reduceFuncFrom(reducer mrjob.Reducer) ReduceFunc {
	return reducer.Reduce
}

// runMap applies fn to every line in r, using the line number as docID.
// Returns intermediate KV pairs grouped by key.
func runMap(r io.Reader, fn MapFunc) (map[string][]string, error) {
	out := make(map[string][]string)
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024*1024), 64*1024*1024)
	lineNo := 0
	for scanner.Scan() {
		pairs := fn(strconv.Itoa(lineNo), scanner.Text())
		for _, kv := range pairs {
			out[kv.Key] = append(out[kv.Key], kv.Value)
		}
		lineNo++
	}
	return out, scanner.Err()
}

// runReduce applies fn to every key in intermediate, returning the final sorted KV list.
func runReduce(intermediate map[string][]string, fn ReduceFunc) []KeyValue {
	keys := make([]string, 0, len(intermediate))
	for k := range intermediate {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	result := make([]KeyValue, 0, len(keys))
	for _, k := range keys {
		result = append(result, KeyValue{Key: k, Value: fn(k, intermediate[k])})
	}
	return result
}

// targetNode returns the index of the reducer node responsible for key,
// given n total nodes.
func targetNode(key string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32()) % n
}
