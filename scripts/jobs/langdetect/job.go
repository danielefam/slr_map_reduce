// Package langdetect estimates the language distribution of the dataset.
//
// The mapper scores every input line against small stop-word sets for five
// European languages and emits ("lang:<code>", "1") for the best-scoring
// language. Lines with no stop-word hits (or too few tokens to judge) are
// attributed to "lang:unknown".
//
// The reducer counts lines per language, so all output values are integer
// strings and remain compatible with the orchestrator's integer-sum merge.
package langdetect

import (
	"strconv"
	"strings"
	"unicode"

	"scripts/mrjob"
)

// stopWords maps an ISO-639-1 language code to a set of very frequent,
// highly discriminative function words for that language.
var stopWords = map[string]map[string]struct{}{
	"en": toSet("the", "and", "of", "to", "in", "is", "that", "it", "for", "was",
		"with", "are", "this", "have", "not", "they", "from", "you", "which"),
	"fr": toSet("le", "la", "les", "de", "des", "du", "et", "est", "que", "qui",
		"dans", "pour", "une", "sur", "pas", "avec", "sont", "mais", "nous"),
	"de": toSet("der", "die", "das", "und", "ist", "von", "den", "nicht", "mit",
		"sich", "auf", "ein", "eine", "auch", "als", "wird", "sind", "dem"),
	"es": toSet("el", "la", "los", "las", "de", "que", "y", "en", "es", "por",
		"una", "con", "para", "del", "se", "su", "como", "más", "pero"),
	"it": toSet("il", "la", "di", "che", "e", "è", "per", "un", "una", "del",
		"della", "con", "non", "sono", "anche", "come", "alla", "gli", "nel"),
}

func toSet(words ...string) map[string]struct{} {
	s := make(map[string]struct{}, len(words))
	for _, w := range words {
		s[w] = struct{}{}
	}
	return s
}

// minTokens is the minimum number of tokens needed before we attempt to
// classify a line; shorter lines are too noisy to judge.
const minTokens = 3

// Mapper implements the language-detection map phase.
type Mapper struct{}

// NewMapper returns the mapper implementation exported by this job package.
func NewMapper() mrjob.Mapper {
	return Mapper{}
}

// Map classifies one line of text and emits ("lang:<code>", "1").
// WARC/WET header lines are skipped so that Common Crawl metadata does not
// skew the distribution towards English.
func (Mapper) Map(_ string, text string) []mrjob.KeyValue {
	line := strings.TrimSpace(text)
	if line == "" || isWARCHeader(line) {
		return nil
	}

	tokens := tokenize(line)
	if len(tokens) < minTokens {
		return nil
	}

	best, bestScore := "unknown", 0
	// Iterate deterministically so ties resolve the same way on every worker.
	for _, lang := range []string{"de", "en", "es", "fr", "it"} {
		score := 0
		for _, tok := range tokens {
			if _, ok := stopWords[lang][tok]; ok {
				score++
			}
		}
		if score > bestScore {
			best, bestScore = lang, score
		}
	}
	return []mrjob.KeyValue{{Key: "lang:" + best, Value: "1"}}
}

// isWARCHeader reports whether the line belongs to WET/WARC record metadata.
func isWARCHeader(line string) bool {
	return strings.HasPrefix(line, "WARC/") ||
		strings.HasPrefix(line, "WARC-") ||
		strings.HasPrefix(line, "Content-Type:") ||
		strings.HasPrefix(line, "Content-Length:")
}

// tokenize lower-cases and splits a line on non-letter runes.
func tokenize(line string) []string {
	return strings.FieldsFunc(strings.ToLower(line), func(r rune) bool {
		return !unicode.IsLetter(r)
	})
}

// Reducer implements the language-detection reduce phase.
type Reducer struct{}

// NewReducer returns the reducer implementation exported by this job package.
func NewReducer() mrjob.Reducer {
	return Reducer{}
}

// Reduce counts the number of lines attributed to a language.
// The output is an integer string, as required by the orchestrator merge.
func (Reducer) Reduce(_ string, values []string) string {
	return strconv.Itoa(len(values))
}
