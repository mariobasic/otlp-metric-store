package ingest

import "testing"

func TestSeriesCache_MarkIfNew(t *testing.T) {
	c, err := NewSeriesCache(100)
	if err != nil {
		t.Fatalf("NewSeriesCache: %v", err)
	}

	if !c.MarkIfNew(42) {
		t.Fatal("first MarkIfNew(42) should return true")
	}
	if c.MarkIfNew(42) {
		t.Fatal("second MarkIfNew(42) should return false (already cached)")
	}
	if !c.MarkIfNew(43) {
		t.Fatal("MarkIfNew(43) should return true (different ID)")
	}
	if c.Len() != 2 {
		t.Fatalf("Len: want 2 got %d", c.Len())
	}
}

func TestSeriesCache_LRUEviction(t *testing.T) {
	c, err := NewSeriesCache(2)
	if err != nil {
		t.Fatalf("NewSeriesCache: %v", err)
	}
	c.MarkIfNew(1)
	c.MarkIfNew(2)
	c.MarkIfNew(3) // evicts 1 (least recently added)

	if c.Len() != 2 {
		t.Fatalf("Len: want 2 got %d", c.Len())
	}
	if !c.MarkIfNew(1) {
		t.Fatal("after eviction, MarkIfNew(1) should return true again")
	}
}

func TestNewSeriesCache_InvalidSize(t *testing.T) {
	if _, err := NewSeriesCache(0); err == nil {
		t.Fatal("NewSeriesCache(0) should error")
	}
}