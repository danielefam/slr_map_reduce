// Package domainpop counts the popularity of URL domains in the dataset.
//
// The mapper extracts the host name from Common Crawl WET record headers
// ("WARC-Target-URI: <url>") and, as a fallback for plain-text inputs, from
// any token that looks like an absolute http(s) URL. It emits (domain, "1")
// per occurrence.
//
// The reducer counts occurrences per domain, so all output values are
// integer strings and remain compatible with the orchestrator's integer-sum
// merge (the final output is therefore a global "domain popularity" ranking,
// already sorted by descending count by the orchestrator).
package domainpop

import (
	"net/url"
	"strconv"
	"strings"

	"scripts/mrjob"
)

const targetURIPrefix = "WARC-Target-URI:"

// Mapper implements the domain-popularity map phase.
type Mapper struct{}

// NewMapper returns the mapper implementation exported by this job package.
func NewMapper() mrjob.Mapper {
	return Mapper{}
}

// Map emits (domain, "1") for every URL found in the line.
//
// WET metadata lines ("WARC-Target-URI: http://…") are the primary source:
// each WET record has exactly one such header, so counting them yields the
// number of crawled pages per domain. For non-WET inputs, absolute http(s)
// URLs embedded in regular text are extracted as a fallback.
func (Mapper) Map(_ string, text string) []mrjob.KeyValue {
	line := strings.TrimSpace(text)
	if line == "" {
		return nil
	}

	// Primary path: WET record header.
	if strings.HasPrefix(line, targetURIPrefix) {
		raw := strings.TrimSpace(line[len(targetURIPrefix):])
		if d, ok := domainOf(raw); ok {
			return []mrjob.KeyValue{{Key: d, Value: "1"}}
		}
		return nil
	}
	// Skip the rest of the WARC metadata; record bodies rarely contain
	// absolute URLs in WET extracts, but plain-text test inputs might.
	if strings.HasPrefix(line, "WARC/") || strings.HasPrefix(line, "WARC-") {
		return nil
	}

	// Fallback path: absolute URLs inside free text.
	var out []mrjob.KeyValue
	for _, tok := range strings.Fields(line) {
		if !strings.HasPrefix(tok, "http://") && !strings.HasPrefix(tok, "https://") {
			continue
		}
		if d, ok := domainOf(strings.TrimRight(tok, ".,;:!?)('\"")); ok {
			out = append(out, mrjob.KeyValue{Key: d, Value: "1"})
		}
	}
	return out
}

// domainOf parses rawURL and returns its normalised host name:
// lower-cased, port stripped, leading "www." removed.
func domainOf(rawURL string) (string, bool) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		return "", false
	}
	host := strings.ToLower(u.Hostname())
	host = strings.TrimPrefix(host, "www.")
	if host == "" {
		return "", false
	}
	return host, true
}

// Reducer implements the domain-popularity reduce phase.
type Reducer struct{}

// NewReducer returns the reducer implementation exported by this job package.
func NewReducer() mrjob.Reducer {
	return Reducer{}
}

// Reduce counts the occurrences of a domain.
// The output is an integer string, as required by the orchestrator merge.
func (Reducer) Reduce(_ string, values []string) string {
	return strconv.Itoa(len(values))
}
