// Package influxdb is a k6 output that sends metrics to an InfluxDB v2 database.
package influxdb

import (
	"context"
	"crypto/tls"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"sync"
	"time"

	influxdbclient "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	influxdblog "github.com/influxdata/influxdb-client-go/v2/log"
	"github.com/sirupsen/logrus"
	"go.k6.io/k6/v2/metrics"
	"go.k6.io/k6/v2/output"
)

func init() {
	// disable the internal influxdb log
	influxdblog.Log = nil
}

// FieldKind defines Enum for tag-to-field type conversion
type FieldKind int

const (
	// String denotes string datatype
	String FieldKind = iota
	// Int denotes integer datatype
	Int
	// Float denotes float datatype
	Float
	// Bool denotes a boolean datatype
	Bool
)

var _ output.Output = new(Output)

// Output is the influxdb Output struct
type Output struct {
	output.SampleBuffer

	client influxdbclient.Client
	config Config

	params          output.Params
	periodicFlusher *output.PeriodicFlusher
	logger          logrus.FieldLogger
	fieldKinds      map[string]FieldKind
	pointWriter     api.WriteAPIBlocking
	semaphoreCh     chan struct{}
	wg              sync.WaitGroup

	// aggregator and aggFlusher are non-nil only when aggregation is enabled.
	// When enabled, the raw per-sample path is replaced by JMeter-style aggregation.
	aggregator *aggregator
	aggFlusher *output.PeriodicFlusher
}

// New returns new InfluxDB Output
func New(params output.Params) (*Output, error) {
	logger := params.Logger.WithFields(logrus.Fields{"output": "InfluxDBv2"})

	conf, err := GetConsolidatedConfig(params.JSONConfig, params.Environment, params.ConfigArgument)
	if err != nil {
		return nil, err
	}
	if conf.Bucket.String == "" {
		return nil, fmt.Errorf("the Bucket option is required")
	}
	if conf.ConcurrentWrites.Int64 <= 0 {
		return nil, fmt.Errorf("the ConcurrentWrites option must be a positive number")
	}
	opts := influxdbclient.DefaultOptions().
		SetTLSConfig(&tls.Config{
			InsecureSkipVerify: conf.InsecureSkipTLSVerify.Bool, //nolint:gosec
		})
	if conf.Precision.Valid {
		opts.SetPrecision(time.Duration(conf.Precision.Duration))
	}
	cl := influxdbclient.NewClientWithOptions(conf.Addr.String, conf.Token.String, opts)
	fldKinds, err := makeFieldKinds(conf)
	if err != nil {
		return nil, err
	}
	o := &Output{
		params:      params,
		logger:      logger,
		client:      cl,
		config:      conf,
		fieldKinds:  fldKinds,
		pointWriter: cl.WriteAPIBlocking(conf.Organization.String, conf.Bucket.String),
		semaphoreCh: make(chan struct{}, conf.ConcurrentWrites.Int64),
		wg:          sync.WaitGroup{},
	}
	if conf.Aggregation.Enabled.Bool {
		if err := validateAggregation(conf); err != nil {
			return nil, err
		}
		o.aggregator = newAggregator(conf)
		logger.WithField("flushInterval", time.Duration(conf.Aggregation.FlushInterval.Duration)).
			Info("InfluxDB aggregation enabled: sending JMeter-style aggregated metrics")
	}
	return o, nil
}

// validateAggregation checks aggregation options. It only runs when aggregation
// is enabled, so the default raw path gains no new failure modes.
func validateAggregation(conf Config) error {
	for _, p := range conf.Aggregation.Percentiles {
		if p <= 0 || p >= 100 {
			return fmt.Errorf("the aggregation percentile %v must be between 0 and 100 (exclusive)", p)
		}
	}
	switch conf.Aggregation.Schema.String {
	case "", schemaK6, schemaJMeter:
	default:
		return fmt.Errorf("the aggregation schema %q must be %q or %q",
			conf.Aggregation.Schema.String, schemaK6, schemaJMeter)
	}
	return nil
}

// Description returns a human-readable description of the output.
func (o *Output) Description() string {
	return fmt.Sprintf("InfluxDBv2 (%s)", o.config.Addr.String)
}

// Start initializes the SampleBuffer for collect samples.
func (o *Output) Start() error {
	o.logger.Debug("Starting...")
	pf, err := output.NewPeriodicFlusher(time.Duration(o.config.PushInterval.Duration), o.flushMetrics)
	if err != nil {
		return err
	}
	o.periodicFlusher = pf

	// When aggregation is enabled, flushMetrics only drains the buffer into the
	// aggregator; a separate, slower flusher emits the aggregated summaries.
	if o.aggregator != nil {
		apf, err := output.NewPeriodicFlusher(
			time.Duration(o.config.Aggregation.FlushInterval.Duration), o.flushAggregated)
		if err != nil {
			// Don't leave the buffer flusher running if the aggregation flusher
			// failed to start.
			o.periodicFlusher.Stop()
			return err
		}
		o.aggFlusher = apf
	}

	o.logger.Debug("Started")
	return nil
}

// Stop flushes any remaining metrics and stops the goroutines.
func (o *Output) Stop() error {
	o.logger.Debug("Stopping...")
	// Stop the buffer flusher first so no new samples are ingested, then flush
	// the final aggregation window (PeriodicFlusher.Stop runs the callback once more).
	o.periodicFlusher.Stop()
	if o.aggFlusher != nil {
		o.aggFlusher.Stop()
	}
	// Wait for every in-flight write to finish before closing the client, so the
	// final flush (raw or aggregated) is never cut off mid-request.
	o.wg.Wait()
	o.client.Close()
	o.logger.Debug("Stopped")
	return nil
}

func (o *Output) extractTagsToValues(tags map[string]string, values map[string]any) map[string]any {
	for tag, kind := range o.fieldKinds {
		if val, ok := tags[tag]; ok {
			var v any
			var err error
			switch kind {
			case String:
				v = val
			case Bool:
				v, err = strconv.ParseBool(val)
			case Float:
				v, err = strconv.ParseFloat(val, 64)
			case Int:
				v, err = strconv.ParseInt(val, 10, 64)
			}
			if err == nil {
				values[tag] = v
			} else {
				values[tag] = val
			}
			delete(tags, tag)
		}
	}
	return values
}

func (o *Output) batchFromSamples(containers []metrics.SampleContainer) []*write.Point {
	type cacheItem struct {
		tags   map[string]string
		values map[string]any
	}
	cache := map[*metrics.TagSet]cacheItem{}

	var points []*write.Point
	for _, container := range containers {
		samples := container.GetSamples()
		for _, sample := range samples {
			var tags map[string]string
			values := make(map[string]any)
			if cached, ok := cache[sample.Tags]; ok {
				tags = cached.tags
				maps.Copy(values, cached.values)
			} else {
				tags = sample.Tags.Map()
				o.extractTagsToValues(tags, values)
				cache[sample.Tags] = cacheItem{tags, values}
			}
			values["value"] = sample.Value
			p := influxdbclient.NewPoint(
				sample.Metric.Name,
				tags,
				values,
				sample.Time,
			)
			points = append(points, p)
		}
	}

	return points
}

func (o *Output) flushMetrics() {
	samples := o.GetBufferedSamples()
	if len(samples) == 0 {
		return
	}

	// Aggregation path: fold samples into the rolling state in memory (no network
	// I/O here). The aggregated summaries are written by flushAggregated.
	if o.aggregator != nil {
		o.aggregator.ingest(samples)
		return
	}

	o.wg.Add(1)
	o.semaphoreCh <- struct{}{}
	go func() {
		defer func() {
			<-o.semaphoreCh
			o.wg.Done()
		}()

		start := time.Now()
		batch := o.batchFromSamples(samples)

		o.logger.WithField("samples", len(samples)).WithField("points", len(batch)).Debug("Sending metrics points...")
		if err := o.pointWriter.WritePoint(context.Background(), batch...); err != nil {
			o.logger.WithError(err).
				WithField("elapsed", time.Since(start)).
				WithField("points", len(batch)).
				Error("Couldn't send metrics points")
			return
		}

		d := time.Since(start)
		o.logger.WithField("elapsed", d).Debug("Metrics points have been sent")
		if d > time.Duration(o.config.PushInterval.Duration) {
			msg := "The flush operation took higher than the expected set push interval. " +
				"If you see this message multiple times then the setup or configuration " +
				"need to be adjusted to achieve a sustainable rate."
			o.logger.WithField("t", d).Warn(msg)
		}
	}()
}

// flushAggregated drains the current aggregation window and writes the JMeter-style
// summary points. It mirrors flushMetrics' goroutine/semaphore structure.
func (o *Output) flushAggregated() {
	snap := o.aggregator.drain()
	if len(snap.groups) == 0 && snap.users.count == 0 && snap.totalSent == 0 && snap.totalReceived == 0 {
		return
	}

	o.wg.Add(1)
	o.semaphoreCh <- struct{}{}
	go func() {
		defer func() {
			<-o.semaphoreCh
			o.wg.Done()
		}()

		start := time.Now()
		batch := aggregateBatch(snap, start)

		o.logger.WithField("points", len(batch)).Debug("Sending aggregated points...")
		if err := o.pointWriter.WritePoint(context.Background(), batch...); err != nil {
			o.logger.WithError(err).
				WithField("elapsed", time.Since(start)).
				WithField("points", len(batch)).
				Error("Couldn't send aggregated points")
			return
		}
		o.logger.WithField("elapsed", time.Since(start)).Debug("Aggregated points have been sent")
	}()
}

// baseTags returns schema's tag map for one sampler group's raw grouping tags
// (name, group, and any pass-through custom tags), per schema.combineTransaction/
// transactionTag/droppedTags.
func baseTags(schema schemaDef, tags map[string]string) map[string]string {
	if !schema.combineTransaction {
		return copyTags(tags)
	}
	out := make(map[string]string, len(tags))
	for k, v := range tags {
		if k == tagName || k == tagGroup || schema.droppedTags[k] {
			continue
		}
		out[k] = v
	}
	name, group := tags[tagName], tags[tagGroup]
	switch {
	case group != "" && name != "":
		out[schema.transactionTag] = group + "::" + name
	case group != "":
		out[schema.transactionTag] = group
	default:
		out[schema.transactionTag] = name
	}
	return out
}

// sentinelTransactionTags returns schema's tag map for a synthetic,
// non-sample-derived row (the cumulative "all" rollup or the "internal"
// thread-metrics row).
func sentinelTransactionTags(schema schemaDef, name string) map[string]string {
	if schema.combineTransaction {
		return map[string]string{schema.transactionTag: name}
	}
	return map[string]string{tagName: name, tagGroup: name}
}

// aggregateBatch turns a drained aggregation window into InfluxDB points, per
// the active schema: per transaction an ok/ko/all triple, per-response-code
// count rows, a cumulative "all" rollup, and an "internal" thread-metrics row.
// rb/sb (per-request byte counts) and startedT/endedT (threads started/ended
// per interval) are never emitted -- k6 has no equivalent per-request data.
func aggregateBatch(snap aggSnapshot, ts time.Time) []*write.Point {
	schema := schemaFor(snap.schema)

	var points []*write.Point
	var totalErrors int64
	// globalValues collects every response time across all groups so the cumulative
	// "all" row can report exact run-wide stats/percentiles (averaging per-group
	// percentiles would be statistically wrong).
	var globalValues []float64

	for _, m := range snap.groups {
		resultValues := []struct {
			result string
			values []float64
		}{
			{statusOK, m.okValues},
			{statusKO, m.koValues},
			{statusAll, m.allValues},
		}
		for _, rv := range resultValues {
			// Always emit the "all" row; skip empty ok/ko rows.
			if len(rv.values) == 0 && rv.result != statusAll {
				continue
			}
			fields := statsFields(rv.values, snap.percentiles, schema.percentileField)
			fields[schema.hitField] = m.hits

			tags := baseTags(schema, m.tags)
			tags[tagResult] = rv.result
			points = append(points, influxdbclient.NewPoint(snap.measurement, tags, fields, ts))
		}

		// Per-response-code counts, including successful 2xx -- mirrors JMeter's
		// own per-response-code rows, which carry no ok/ko/all (statut) tag.
		// Count-only rows: counts are additive across windows, so a response-code
		// distribution charts correctly at any time range (unlike percentiles).
		for ek, c := range m.statusCounts {
			tags := baseTags(schema, m.tags)
			tags[schema.responseCodeTag] = ek.status
			if !schema.omitSuccessMessage || ek.msg != okMessage {
				tags[schema.responseMessageTag] = ek.msg
			}
			points = append(points, influxdbclient.NewPoint(
				snap.measurement, tags, map[string]any{"count": c}, ts))
		}

		globalValues = append(globalValues, m.allValues...)
		totalErrors += m.failures
	}

	// Cumulative "all" transaction across every group: exact run-wide
	// count/avg/min/max/percentiles plus errors. data_sent/data_received are
	// reported here as global totals since k6 measures them at the connection
	// level and cannot be attributed to individual requests; they have no JMeter
	// equivalent, so schema.includeConnectionTotals is false in jmeter schema.
	cumulative := statsFields(globalValues, snap.percentiles, schema.percentileField)
	cumulative[schema.hitField] = int64(len(globalValues))
	cumulative[schema.countErrorField] = totalErrors
	if schema.includeConnectionTotals {
		cumulative["data_sent"] = snap.totalSent
		cumulative["data_received"] = snap.totalReceived
	}
	cumulativeTags := sentinelTransactionTags(schema, statusAll)
	cumulativeTags[tagResult] = statusAll
	points = append(points, influxdbclient.NewPoint(snap.measurement, cumulativeTags, cumulative, ts))

	// Thread / active-VU metrics, like JMeter's "internal" transaction row.
	if snap.users.count > 0 {
		points = append(points, influxdbclient.NewPoint(
			snap.measurement,
			sentinelTransactionTags(schema, "internal"),
			map[string]any{
				schema.minActiveField:  snap.users.min,
				schema.maxActiveField:  snap.users.max,
				schema.meanActiveField: snap.users.sum / float64(snap.users.count),
			},
			ts,
		))
	}

	return points
}

// MakeFieldKinds reads the Config and returns a lookup map of tag names to
// the field type their values should be converted to.
func makeFieldKinds(conf Config) (map[string]FieldKind, error) {
	fieldKinds := make(map[string]FieldKind)
	for _, tag := range conf.TagsAsFields {
		var fieldName, fieldType string
		s := strings.SplitN(tag, ":", 2)
		if len(s) == 1 {
			fieldName, fieldType = s[0], "string"
		} else {
			fieldName, fieldType = s[0], s[1]
		}

		err := checkDuplicatedTypeDefinitions(fieldKinds, fieldName)
		if err != nil {
			return nil, err
		}

		switch fieldType {
		case "string":
			fieldKinds[fieldName] = String
		case "bool":
			fieldKinds[fieldName] = Bool
		case "float":
			fieldKinds[fieldName] = Float
		case "int":
			fieldKinds[fieldName] = Int
		default:
			return nil, fmt.Errorf("an invalid type (%s) is specified for an InfluxDB field (%s)",
				fieldType, fieldName)
		}
	}

	return fieldKinds, nil
}

func checkDuplicatedTypeDefinitions(fieldKinds map[string]FieldKind, tag string) error {
	if _, found := fieldKinds[tag]; found {
		return fmt.Errorf("a tag name (%s) shows up more than once in InfluxDB field type configurations", tag)
	}
	return nil
}
