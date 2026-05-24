package ingest

import lru "github.com/hashicorp/golang-lru/v2"

// SeriesCache deduplicates lookup-table inserts within a single process.
// It tracks SeriesIDs the service has already written, so repeat datapoints
// for an established series skip the catalogue write path entirely.
//
// Bounded by a fixed LRU size — evicted entries trigger one harmless duplicate
// insert, which ReplacingMergeTree(LastSeen) collapses during background
// merges. Cross-instance dedup is also handled by the merge engine; see
// design considerations in README.
type SeriesCache struct {
	cache *lru.Cache[uint64, struct{}]
}

// NewSeriesCache creates an LRU-backed cache of the given size. Returns an
// error if size <= 0 (lru.New constraint).
func NewSeriesCache(size int) (*SeriesCache, error) {
	c, err := lru.New[uint64, struct{}](size)
	if err != nil {
		return nil, err
	}
	return &SeriesCache{cache: c}, nil
}

// MarkIfNew atomically records the series ID and reports whether it was
// previously absent. Returns true the first time a given ID is seen (and
// after eviction), false otherwise. Single lock acquisition per call via
// the underlying lru.Cache's ContainsOrAdd.
func (c *SeriesCache) MarkIfNew(id uint64) bool {
	contains, _ := c.cache.ContainsOrAdd(id, struct{}{})
	return !contains
}

// Len returns the number of entries currently cached. Useful for tests and
// the `series_cache_size` instrument (Phase 5).
func (c *SeriesCache) Len() int {
	return c.cache.Len()
}