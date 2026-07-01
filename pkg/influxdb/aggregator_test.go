package influxdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/metrics"
)

func TestQuantile(t *testing.T) {
	t.Parallel()

	sorted := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	assert.InDelta(t, 1.0, quantile(sorted, 0), 1e-9)
	assert.InDelta(t, 10.0, quantile(sorted, 100), 1e-9)
	assert.InDelta(t, 5.5, quantile(sorted, 50), 1e-9)
	// p90 on a 10-element slice with linear interpolation: rank = 0.9*9 = 8.1 -> 9.1
	assert.InDelta(t, 9.1, quantile(sorted, 90), 1e-9)

	assert.Equal(t, 0.0, quantile(nil, 95))
	assert.Equal(t, 42.0, quantile([]float64{42}, 95))
}

func TestPercentileField(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "p90", percentileField(90))
	assert.Equal(t, "p95", percentileField(95))
	assert.Equal(t, "p99_9", percentileField(99.9))
}

func TestJMeterPercentileField(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "pct90.0", jmeterPercentileField(90))
	assert.Equal(t, "pct95.0", jmeterPercentileField(95))
	assert.Equal(t, "pct99.9", jmeterPercentileField(99.9))
}

func TestStatsFields(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		f := statsFields(nil, []float64{95}, percentileField)
		assert.Equal(t, int64(0), f["count"])
		_, hasAvg := f["avg"]
		assert.False(t, hasAvg)
	})

	t.Run("values", func(t *testing.T) {
		t.Parallel()
		f := statsFields([]float64{10, 20, 30, 40}, []float64{90, 99}, percentileField)
		assert.Equal(t, int64(4), f["count"])
		assert.InDelta(t, 10.0, f["min"], 1e-9)
		assert.InDelta(t, 40.0, f["max"], 1e-9)
		assert.InDelta(t, 25.0, f["avg"], 1e-9)
		assert.Contains(t, f, "p90")
		assert.Contains(t, f, "p99")
	})

	t.Run("jmeter field names", func(t *testing.T) {
		t.Parallel()
		f := statsFields([]float64{10, 20, 30, 40}, []float64{90, 99}, jmeterPercentileField)
		assert.Contains(t, f, "pct90.0")
		assert.Contains(t, f, "pct99.0")
	})
}

func TestGroupTagsAndKey(t *testing.T) {
	t.Parallel()

	a := newAggregator(NewConfig()) // default dropTags: vu, iter, url

	// Custom tags (testid) are kept automatically; high-cardinality vu/url and
	// internal tags (expected_response/status) are dropped.
	t1 := a.groupTags(map[string]string{
		"name": "GET /x", "group": "g1", "testid": "t1",
		"vu": "1", "url": "http://a/x", "expected_response": "true", "status": "200",
	})
	assert.Equal(t, "GET /x", t1["name"])
	assert.Equal(t, "g1", t1["group"])
	assert.Equal(t, "t1", t1["testid"], "custom tag kept with no extra config")
	assert.NotContains(t, t1, "vu")
	assert.NotContains(t, t1, "url")
	assert.NotContains(t, t1, "expected_response")
	assert.NotContains(t, t1, "status")

	// Same grouping tags but different vu/url => same key.
	t2 := a.groupTags(map[string]string{
		"name": "GET /x", "group": "g1", "testid": "t1", "vu": "99", "url": "http://a/x?z=2",
	})
	assert.Equal(t, groupKey(t1), groupKey(t2), "vu/url must not affect the group key")

	// Different custom tag value => different key.
	t3 := a.groupTags(map[string]string{"name": "GET /x", "group": "g1", "testid": "t2"})
	assert.NotEqual(t, groupKey(t1), groupKey(t3))
}

func TestAggregatorIngest(t *testing.T) {
	t.Parallel()

	registry := metrics.NewRegistry()
	dur, err := registry.NewMetric(metricHTTPReqDuration, metrics.Trend)
	require.NoError(t, err)
	sent, err := registry.NewMetric(metricDataSent, metrics.Counter)
	require.NoError(t, err)
	vus, err := registry.NewMetric(metricVUs, metrics.Gauge)
	require.NoError(t, err)

	mkSample := func(m *metrics.Metric, v float64, tags map[string]string) metrics.Sample {
		return metrics.Sample{
			TimeSeries: metrics.TimeSeries{
				Metric: m,
				Tags:   registry.RootTagSet().WithTagsFromMap(tags),
			},
			Value: v,
		}
	}

	a := newAggregator(Config{Aggregation: AggregationConfig{Percentiles: []float64{95}}})

	okTags := map[string]string{"name": "GET /x", "group": "g", "expected_response": "true", "status": "200"}
	koTags := map[string]string{
		"name": "GET /x", "group": "g", "expected_response": "false", "status": "500", "error": "boom",
	}

	a.ingest([]metrics.SampleContainer{metrics.Samples{
		mkSample(dur, 100, okTags),
		mkSample(dur, 200, okTags),
		mkSample(dur, 500, koTags),
		mkSample(sent, 1024, okTags),
		mkSample(vus, 5, nil),
		mkSample(vus, 9, nil),
	}})

	require.Len(t, a.groups, 1)
	var m *samplerMetric
	for _, g := range a.groups {
		m = g
	}
	assert.Equal(t, int64(2), m.successes)
	assert.Equal(t, int64(1), m.failures)
	assert.Equal(t, int64(3), m.hits)
	assert.Len(t, m.allValues, 3)
	assert.Len(t, m.okValues, 2)
	assert.Len(t, m.koValues, 1)
	// Per-(status,message) counters track every response, including successful 2xx,
	// with a synthetic "OK" message for successes (mirroring JMeter's own rows).
	assert.Equal(t, int64(2), m.statusCounts[errKey{status: "200", msg: okMessage}])
	assert.Equal(t, int64(1), m.statusCounts[errKey{status: "500", msg: "boom"}])
	// data_sent is tracked as a global total, not per-group, and must not create a group.
	assert.Equal(t, 1024.0, a.totalSent)
	require.Len(t, a.groups, 1)

	assert.Equal(t, 5.0, a.users.min)
	assert.Equal(t, 9.0, a.users.max)
	assert.Equal(t, int64(2), a.users.count)

	// drain resets state.
	snap := a.drain()
	assert.Len(t, snap.groups, 1)
	assert.Empty(t, a.groups)
	assert.Equal(t, int64(0), a.users.count)
}
