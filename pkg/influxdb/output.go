package influxdb

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	influxdbclient "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	influxdblog "github.com/influxdata/influxdb-client-go/v2/log"
	"github.com/sirupsen/logrus"
	"go.k6.io/k6/output"
	"go.k6.io/k6/stats"
)

func init() {
	// disable the internal influxdb log
	influxdblog.Log = nil
}

// FieldKind defines Enum for tag-to-field type conversion
type FieldKind int

const (
	String FieldKind = iota
	Int
	Float
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
	return &Output{
		params:      params,
		logger:      logger,
		client:      cl,
		config:      conf,
		fieldKinds:  fldKinds,
		pointWriter: cl.WriteAPIBlocking(conf.Organization.String, conf.Bucket.String),
		semaphoreCh: make(chan struct{}, conf.ConcurrentWrites.Int64),
		wg:          sync.WaitGroup{},
	}, nil
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
	o.logger.Debug("Started")
	o.periodicFlusher = pf
	return nil
}

// Stop flushes any remaining metrics and stops the goroutine.
func (o *Output) Stop() error {
	o.logger.Debug("Stopping...")
	o.periodicFlusher.Stop()
	o.client.Close()
	o.wg.Wait()
	o.logger.Debug("Stopped")
	return nil
}

func (o *Output) extractTagsToValues(tags map[string]string, values map[string]interface{}) map[string]interface{} {
	for tag, kind := range o.fieldKinds {
		if val, ok := tags[tag]; ok {
			var v interface{}
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

func (o *Output) batchFromSamples(containers []stats.SampleContainer) []*write.Point {
	type cacheItem struct {
		tags   map[string]string
		values map[string]interface{}
	}
	cache := map[*stats.SampleTags]cacheItem{}

	var points []*write.Point
	for _, container := range containers {
		samples := container.GetSamples()
		for _, sample := range samples {
			var tags map[string]string
			values := make(map[string]interface{})
			if cached, ok := cache[sample.Tags]; ok {
				tags = cached.tags
				for k, v := range cached.values {
					values[k] = v
				}
			} else {
				tags = sample.Tags.CloneTags()
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
			msg := "The flush operation took higher than the expected set push interval. If you see this message multiple times then the setup or configuration need to be adjusted to achieve a sustainable rate."
			o.logger.WithField("t", d).Warn(msg)
		}
	}()
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
