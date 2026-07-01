package influxdb

import (
	"sort"
	"strconv"
	"strings"
	"sync"

	"go.k6.io/k6/v2/metrics"
)

// Aggregation status values, mirroring JMeter's ok/ko/all transaction split.
const (
	statusOK  = "ok"
	statusKO  = "ko"
	statusAll = "all"

	// okMessage is the synthetic responseMessage used for successful samples,
	// mirroring JMeter's own "OK" message on its per-response-code breakdown rows.
	okMessage = "OK"
)

// Aggregation schema modes, selected by Config.Aggregation.Schema.
const (
	schemaK6     = "k6"
	schemaJMeter = "jmeter"
)

// k6 metric names ingested by the aggregator. Response time is modeled on
// http_req_duration, exactly like JMeter models it on the sampler elapsed time.
const (
	metricHTTPReqDuration = "http_req_duration"
	metricDataSent        = "data_sent"
	metricDataReceived    = "data_received"
	metricVUs             = "vus"
)

// k6 sample tag keys read by the aggregator. These describe the *input* (raw k6
// sample tags) and never change with the output schema; what varies by schema is
// how they get renamed/combined/dropped on the way out -- see schemaDef below.
const (
	tagName             = "name"
	tagGroup            = "group"
	tagStatus           = "status"
	tagError            = "error"
	tagErrorCode        = "error_code"
	tagExpectedResponse = "expected_response"
)

// tagResult is the tag key both schemas use for the synthetic ok/ko/all split,
// under JMeter's own name ("statut"). Deliberately distinct from k6's built-in
// "status" (HTTP status code) tag to avoid a collision.
const tagResult = "statut"

// internalTags are read by the aggregator itself (for the ok/ko split and the
// per-error breakdown) and are never used as grouping dimensions.
var internalTags = map[string]bool{
	tagExpectedResponse: true,
	tagStatus:           true,
	tagError:            true,
	tagErrorCode:        true,
}

// schemaDef fully describes one aggregation output schema's tag/field naming.
// aggregateBatch and baseTags never branch on which schema is active -- they
// look values up on the active schemaDef instead. Every schema-dependent
// decision lives here, in exactly one of the two schemaDef values below.
type schemaDef struct {
	// hitField is the field name for the per-window hit/request count.
	hitField string
	// countErrorField is the field name for the cumulative error count, set only
	// on the "all" rollup row.
	countErrorField string
	// percentileField renders a percentile (e.g. 90) as its field name.
	percentileField func(p float64) string
	// combineTransaction merges tagName+tagGroup into a single transactionTag
	// when true (JMeter has no separate group concept); when false, tagName and
	// tagGroup stay separate tags and transactionTag is unused.
	combineTransaction bool
	transactionTag     string
	// responseCodeTag/responseMessageTag are the tag keys used on the
	// per-response-code breakdown rows.
	responseCodeTag    string
	responseMessageTag string
	// omitSuccessMessage suppresses responseMessageTag on successful rows. Set
	// for k6 schema so it doesn't invent a new "error" tag value that never
	// existed on 2xx/3xx rows before this feature added response-code breakdown
	// rows; JMeter's own schema always sets it (including "OK" on success).
	omitSuccessMessage bool
	// includeConnectionTotals reports data_sent/data_received on the cumulative
	// row. k6 measures these at the connection level, with no per-transaction or
	// JMeter equivalent.
	includeConnectionTotals bool
	// droppedTags are tag keys with no equivalent in this schema, excluded at
	// both grouping time (see newAggregator) and emission time (see baseTags).
	droppedTags map[string]bool
	// minActiveField/maxActiveField/meanActiveField are the active-thread (VU)
	// field names on the separate "internal" row.
	minActiveField  string
	maxActiveField  string
	meanActiveField string
}

// k6Schema is this output's own naming (Aggregation.Schema == "k6", the
// default): unchanged from before this feature added a second schema.
var k6Schema = schemaDef{
	hitField:                "hits",
	countErrorField:         "countError",
	percentileField:         percentileField,
	combineTransaction:      false,
	responseCodeTag:         tagStatus,
	responseMessageTag:      tagError,
	omitSuccessMessage:      true,
	includeConnectionTotals: true,
	minActiveField:          "minAT",
	maxActiveField:          "maxAT",
	meanActiveField:         "meanAT",
}

// jmeterSchema mirrors a real JMeter InfluxDB backend listener's own schema
// (verified against a live JMeter-written bucket via the InfluxDB MCP during
// design), so existing JMeter dashboards/queries can be reused against k6 data.
var jmeterSchema = schemaDef{
	hitField:                "hit",
	countErrorField:         "countError",
	percentileField:         jmeterPercentileField,
	combineTransaction:      true,
	transactionTag:          "transaction",
	responseCodeTag:         "responseCode",
	responseMessageTag:      "responseMessage",
	includeConnectionTotals: false,
	droppedTags: map[string]bool{
		"method":      true,
		"proto":       true,
		"tls_version": true,
		"scenario":    true,
	},
	minActiveField:  "minAT",
	maxActiveField:  "maxAT",
	meanActiveField: "meanAT",
}

// schemaFor returns the schemaDef for a Config.Aggregation.Schema value,
// defaulting to k6Schema for "" (unset) or "k6".
func schemaFor(name string) schemaDef {
	if name == schemaJMeter {
		return jmeterSchema
	}
	return k6Schema
}

// k6LifecycleGroups are the raw group tag values k6 assigns to requests made
// during its own setup/teardown lifecycle phases (not user-defined groups).
// Samples from these phases are skipped so they don't pollute the transaction list.
var k6LifecycleGroups = map[string]bool{
	"::setup":    true,
	"::teardown": true,
}

// groupSeparator is an ASCII unit separator used to build collision-safe group
// keys (it cannot appear in normal tag values).
const groupSeparator = "\x1f"

// errKey identifies a unique (response code, message) bucket, like JMeter's own
// per-response-code breakdown rows.
type errKey struct {
	status string
	msg    string
}

// samplerMetric accumulates response-time samples for one (name, group, extraTags)
// group over a single flush window. Mirrors JMeter's SamplerMetric.
type samplerMetric struct {
	// tags is the snapshot of grouping tag values (name, group, extra tags) used
	// when emitting points.
	tags map[string]string

	okValues  []float64
	koValues  []float64
	allValues []float64

	successes int64
	failures  int64
	hits      int64

	// statusCounts holds the per-(status, message) request count (e.g. ("200","OK")
	// -> 9, ("500","boom") -> 1), covering every distinct response, not just
	// failures, so a full response-code distribution can be reported -- mirroring
	// JMeter's own per-response-code rows, which carry no ok/ko/all split.
	statusCounts map[errKey]int64
}

func newSamplerMetric(tags map[string]string) *samplerMetric {
	return &samplerMetric{
		tags:         tags,
		statusCounts: make(map[errKey]int64),
	}
}

// addDuration records a single response-time observation, routing it into the
// ok/ko/all buckets just like JMeter does, and counting its (status, message) pair.
func (m *samplerMetric) addDuration(v float64, ok bool, status, errMsg string) {
	m.allValues = append(m.allValues, v)
	m.hits++
	// k6 reports status "0" for transport-level failures (connection refused,
	// timeout, DNS); fall back to "0" if the tag is somehow absent.
	if status == "" {
		status = "0"
	}
	msg := errMsg
	switch {
	case ok:
		msg = okMessage
	case msg == "":
		msg = "KO"
	}
	m.statusCounts[errKey{status: status, msg: msg}]++
	if ok {
		m.okValues = append(m.okValues, v)
		m.successes++
	} else {
		m.koValues = append(m.koValues, v)
		m.failures++
	}
}

// userMetric tracks active-VU (thread) counts over a flush window. Mirrors the
// thread metrics from JMeter's UserMetric (min/max/mean active threads).
type userMetric struct {
	min   float64
	max   float64
	sum   float64
	count int64
}

func (u *userMetric) add(v float64) {
	if u.count == 0 || v < u.min {
		u.min = v
	}
	if u.count == 0 || v > u.max {
		u.max = v
	}
	u.sum += v
	u.count++
}

// aggregator holds the cross-interval aggregation state. It survives the raw
// sample-buffer drains (which happen every PushInterval) and is itself drained
// and reset every aggregation FlushInterval.
type aggregator struct {
	mu sync.Mutex

	dropTags    map[string]bool
	percentiles []float64
	measurement string
	schema      string

	groups map[string]*samplerMetric
	users  userMetric

	// data_sent/data_received are measured by k6 at the connection level and
	// cannot be attributed to individual requests, so they are tracked as global
	// totals and reported on the cumulative row.
	totalSent     float64
	totalReceived float64
}

func newAggregator(c Config) *aggregator {
	drop := make(map[string]bool, len(c.Aggregation.DropTags))
	for _, t := range c.Aggregation.DropTags {
		drop[t] = true
	}
	schema := c.Aggregation.Schema.String
	if schema == "" {
		schema = schemaK6
	}
	// Drop the active schema's droppedTags at grouping time too, not just at
	// emission time in baseTags: otherwise two groups differing only by one of
	// these tags would still be grouped separately but emit points with an
	// identical schema tag set and timestamp, silently overwriting each other in
	// InfluxDB instead of being counted together.
	for t := range schemaFor(schema).droppedTags {
		drop[t] = true
	}
	return &aggregator{
		dropTags:    drop,
		percentiles: c.Aggregation.Percentiles,
		measurement: c.Aggregation.Measurement.String,
		schema:      schema,
		groups:      make(map[string]*samplerMetric),
	}
}

// groupTags keeps every tag on the sample (built-in or custom) except the
// high-cardinality denylist and the internally-consumed tags. This way custom
// tags flow into the aggregated output automatically, with no extra config.
func (a *aggregator) groupTags(tagMap map[string]string) map[string]string {
	tags := make(map[string]string, len(tagMap))
	for k, v := range tagMap {
		if v == "" || internalTags[k] || a.dropTags[k] {
			continue
		}
		// k6 stores group paths as "::root::child"; strip the leading "::" so
		// the tag value is human-readable (e.g. "DCPAN-TR-01-homepage").
		if k == tagGroup {
			v = strings.TrimPrefix(v, "::")
		}
		tags[k] = v
	}
	return tags
}

// groupKey builds a stable, collision-safe key from a sorted snapshot of the
// grouping tags.
func groupKey(tags map[string]string) string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(tags[k])
		b.WriteString(groupSeparator)
	}
	return b.String()
}

// metricFor returns (creating if needed) the samplerMetric for a sample's group.
// Callers must hold a.mu.
func (a *aggregator) metricFor(tagMap map[string]string) *samplerMetric {
	tags := a.groupTags(tagMap)
	key := groupKey(tags)
	m := a.groups[key]
	if m == nil {
		m = newSamplerMetric(tags)
		a.groups[key] = m
	}
	return m
}

// ingest folds a batch of samples into the rolling aggregation state. Only the
// metrics JMeter models are considered; everything else is ignored.
func (a *aggregator) ingest(containers []metrics.SampleContainer) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, container := range containers {
		for _, s := range container.GetSamples() {
			switch s.Metric.Name {
			case metricHTTPReqDuration:
				tagMap := s.Tags.Map()
				if k6LifecycleGroups[tagMap[tagGroup]] {
					continue
				}
				m := a.metricFor(tagMap)
				ok := tagMap[tagExpectedResponse] != "false"
				m.addDuration(s.Value, ok, tagMap[tagStatus], tagMap[tagError])
			case metricDataSent:
				a.totalSent += s.Value
			case metricDataReceived:
				a.totalReceived += s.Value
			case metricVUs:
				a.users.add(s.Value)
			}
		}
	}
}

// aggSnapshot is a drained window of aggregation state, ready to be turned into
// points outside of the aggregator lock.
type aggSnapshot struct {
	groups        map[string]*samplerMetric
	users         userMetric
	totalSent     float64
	totalReceived float64
	percentiles   []float64
	measurement   string
	schema        string
}

// drain atomically swaps out the current window for a fresh empty one and returns
// the old state. This is the per-interval reset, equivalent to JMeter's
// resetForTimeInterval.
func (a *aggregator) drain() aggSnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()

	snap := aggSnapshot{
		groups:        a.groups,
		users:         a.users,
		totalSent:     a.totalSent,
		totalReceived: a.totalReceived,
		percentiles:   a.percentiles,
		measurement:   a.measurement,
		schema:        a.schema,
	}
	a.groups = make(map[string]*samplerMetric)
	a.users = userMetric{}
	a.totalSent = 0
	a.totalReceived = 0
	return snap
}

// statsFields computes the JMeter-style summary fields (count/min/max/avg + the
// configured percentiles) for a slice of response-time values. percentileFieldFn
// renders each percentile's field name, and differs by aggregation schema.
func statsFields(values []float64, percentiles []float64, percentileFieldFn func(float64) string) map[string]any {
	count := int64(len(values))
	fields := map[string]any{"count": count}
	if count == 0 {
		return fields
	}

	min, max, sum := values[0], values[0], 0.0
	for _, v := range values {
		sum += v
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	fields["min"] = min
	fields["max"] = max
	fields["avg"] = sum / float64(count)

	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	for _, p := range percentiles {
		fields[percentileFieldFn(p)] = quantile(sorted, p)
	}
	return fields
}

// quantile returns the p-th percentile (0..100) of a sorted slice using linear
// interpolation, matching Apache Commons Math DescriptiveStatistics.getPercentile
// as used by JMeter.
func quantile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sorted[0]
	}
	rank := (p / 100) * float64(n-1)
	lo := int(rank)
	if lo >= n-1 {
		return sorted[n-1]
	}
	frac := rank - float64(lo)
	return sorted[lo] + frac*(sorted[lo+1]-sorted[lo])
}

// percentileField renders a percentile as a k6-schema InfluxDB field name, e.g.
// 90 -> "p90", 99.9 -> "p99_9".
func percentileField(p float64) string {
	s := strconv.FormatFloat(p, 'f', -1, 64)
	s = strings.ReplaceAll(s, ".", "_")
	return "p" + s
}

// jmeterPercentileField renders a percentile as JMeter's own field name, e.g.
// 90 -> "pct90.0", 99.9 -> "pct99.9". JMeter always includes a decimal point.
func jmeterPercentileField(p float64) string {
	s := strconv.FormatFloat(p, 'f', -1, 64)
	if !strings.Contains(s, ".") {
		s += ".0"
	}
	return "pct" + s
}

// copyTags returns a shallow copy of a tag map so callers can add per-point tags
// (like status) without mutating the shared group snapshot.
func copyTags(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src)+1)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
