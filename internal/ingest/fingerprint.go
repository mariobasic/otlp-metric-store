package ingest

import (
	"sort"
	"strings"

	"github.com/cespare/xxhash/v2"
)

// computeSeriesID returns a deterministic 64-bit fingerprint of the identifying
// fields of a metric series. Two datapoints with identical inputs produce the
// same ID; any change in metric name/type, resource attrs, scope, schema URL,
// or datapoint attrs produces a different ID.
//
// Map keys are sorted before hashing because Go map iteration order is
// non-deterministic. A null byte separates fields and key/value pairs to keep
// concatenated boundaries unambiguous (e.g. "ab"+"c" != "a"+"bc" after hashing).
func computeSeriesID(
	metricName, metricType string,
	resourceAttrs map[string]string,
	resourceSchemaURL string,
	scopeName, scopeVersion string,
	dpAttrs map[string]string,
) uint64 {
	var b strings.Builder
	b.WriteString(metricName)
	b.WriteByte(0)
	b.WriteString(metricType)
	b.WriteByte(0)
	b.WriteString(resourceSchemaURL)
	b.WriteByte(0)
	b.WriteString(scopeName)
	b.WriteByte(0)
	b.WriteString(scopeVersion)
	b.WriteByte(0)

	for _, k := range sortedKeys(resourceAttrs) {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(resourceAttrs[k])
		b.WriteByte(0)
	}
	b.WriteByte(0) // separator between resourceAttrs and dpAttrs sections
	for _, k := range sortedKeys(dpAttrs) {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(dpAttrs[k])
		b.WriteByte(0)
	}
	return xxhash.Sum64String(b.String())
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}