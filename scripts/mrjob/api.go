package mrjob

// KeyValue is a single intermediate or output key-value pair.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Mapper emits intermediate key/value pairs for one input document.
type Mapper interface {
	Map(docID, text string) []KeyValue
}

// Reducer combines all values for a given key into the final output value.
type Reducer interface {
	Reduce(key string, values []string) string
}
