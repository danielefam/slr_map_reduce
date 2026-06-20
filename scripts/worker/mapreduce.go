// Package main defines the core MapReduce types and logic used by the worker.
package main

import (
	"bufio"
	"hash/fnv"
	"io"
	"os"
	"strconv"
	"strings"

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
// It streams the emitted KV pairs directly to the appropriate partition writer.
func runMap(r io.Reader, fn MapFunc, n int, writers []*bufio.Writer) (int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024*1024), 64*1024*1024)
	lineNo := 0
	pairsCount := 0
	for scanner.Scan() {
		pairs := fn(strconv.Itoa(lineNo), scanner.Text())
		for _, kv := range pairs {
			idx := targetNode(kv.Key, n)
			if _, err := writers[idx].WriteString(kv.Key + "\t" + kv.Value + "\n"); err != nil {
				return pairsCount, err
			}
			pairsCount++
		}
		lineNo++
	}
	return pairsCount, scanner.Err()
}

// runReduce applies fn streamingly over a sorted TSV file, returning the final sorted KV list.
func runReduce(sortedFile string, fn ReduceFunc) ([]KeyValue, error) {
	f, err := os.Open(sortedFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024*1024), 64*1024*1024)

	var result []KeyValue
	var currentKey string
	var currentValues []string

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			continue
		}
		k, v := parts[0], parts[1]

		if currentKey == "" {
			currentKey = k
		} else if k != currentKey {
			result = append(result, KeyValue{Key: currentKey, Value: fn(currentKey, currentValues)})
			currentKey = k
			currentValues = currentValues[:0] // reuse slice
		}
		currentValues = append(currentValues, v)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if currentKey != "" {
		result = append(result, KeyValue{Key: currentKey, Value: fn(currentKey, currentValues)})
	}
	return result, nil
}

// targetNode returns the index of the reducer node responsible for key,
// given n total nodes.
func targetNode(key string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32()) % n
}
