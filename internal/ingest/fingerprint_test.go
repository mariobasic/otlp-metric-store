package ingest

import "testing"

func TestComputeSeriesID(t *testing.T) {
	base := func() (string, string, map[string]string, string, string, string, map[string]string) {
		return "cpu.utilization",
			"Gauge",
			map[string]string{"service.name": "checkout", "host.name": "h1"},
			"https://opentelemetry.io/schemas/1.4.0",
			"runtime",
			"1.0.0",
			map[string]string{"cpu": "0", "state": "user"}
	}

	t.Run("determinism", func(t *testing.T) {
		a := computeSeriesID(base())
		b := computeSeriesID(base())
		if a != b {
			t.Fatalf("same inputs produced different IDs: %d vs %d", a, b)
		}
	})

	t.Run("different metric name", func(t *testing.T) {
		a := computeSeriesID(base())
		mn, mt, ra, su, sn, sv, da := base()
		mn = "memory.utilization"
		b := computeSeriesID(mn, mt, ra, su, sn, sv, da)
		if a == b {
			t.Fatalf("different MetricName must produce different IDs, got %d", a)
		}
	})

	t.Run("different metric type", func(t *testing.T) {
		mn, _, ra, su, sn, sv, da := base()
		gauge := computeSeriesID(mn, "Gauge", ra, su, sn, sv, da)
		sum := computeSeriesID(mn, "Sum", ra, su, sn, sv, da)
		if gauge == sum {
			t.Fatalf("Gauge vs Sum with same name must differ, got %d", gauge)
		}
	})

	t.Run("different resource attributes", func(t *testing.T) {
		a := computeSeriesID(base())
		mn, mt, _, su, sn, sv, da := base()
		ra := map[string]string{"service.name": "checkout", "host.name": "h2"}
		b := computeSeriesID(mn, mt, ra, su, sn, sv, da)
		if a == b {
			t.Fatalf("different ResourceAttributes must produce different IDs, got %d", a)
		}
	})

	t.Run("different datapoint attributes", func(t *testing.T) {
		a := computeSeriesID(base())
		mn, mt, ra, su, sn, sv, _ := base()
		da := map[string]string{"cpu": "1", "state": "user"}
		b := computeSeriesID(mn, mt, ra, su, sn, sv, da)
		if a == b {
			t.Fatalf("different datapoint Attributes must produce different IDs, got %d", a)
		}
	})

	t.Run("attribute order does not matter", func(t *testing.T) {
		mn, mt, _, su, sn, sv, _ := base()
		ra1 := map[string]string{"a": "1", "b": "2"}
		ra2 := map[string]string{"b": "2", "a": "1"}
		da1 := map[string]string{"x": "10", "y": "20"}
		da2 := map[string]string{"y": "20", "x": "10"}
		a := computeSeriesID(mn, mt, ra1, su, sn, sv, da1)
		b := computeSeriesID(mn, mt, ra2, su, sn, sv, da2)
		if a != b {
			t.Fatalf("map order changed the ID: %d vs %d", a, b)
		}
	})

	t.Run("empty attributes produce non-zero ID", func(t *testing.T) {
		id := computeSeriesID("m", "Gauge", nil, "", "scope", "1.0", nil)
		if id == 0 {
			t.Fatalf("empty attributes must still produce a non-zero ID")
		}
	})

	t.Run("resource vs datapoint attrs are not interchangeable", func(t *testing.T) {
		// Same total set of {k:v} pairs split differently across resource vs
		// datapoint attrs must produce different IDs — otherwise two semantically
		// different series would collide.
		mn, mt, _, su, sn, sv, _ := base()
		a := computeSeriesID(mn, mt, map[string]string{"k": "v"}, su, sn, sv, nil)
		b := computeSeriesID(mn, mt, nil, su, sn, sv, map[string]string{"k": "v"})
		if a == b {
			t.Fatalf("resource attr {k:v} collided with datapoint attr {k:v}: %d", a)
		}
	})
}