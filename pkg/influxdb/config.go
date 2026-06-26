package influxdb

import (
	"encoding/json"
	"net/url"
	"strings"
	"time"

	"github.com/mstoykov/envconfig"
	"go.k6.io/k6/v2/lib/types"
	"gopkg.in/guregu/null.v3"
)

// Config contains the configuration for the Output.
type Config struct {
	Addr                  null.String        `json:"addr" envconfig:"K6_INFLUXDB_ADDR"`
	Organization          null.String        `json:"organization" envconfig:"K6_INFLUXDB_ORGANIZATION"`
	Bucket                null.String        `json:"bucket" envconfig:"K6_INFLUXDB_BUCKET"`
	Token                 null.String        `json:"token" envconfig:"K6_INFLUXDB_TOKEN"`
	InsecureSkipTLSVerify null.Bool          `json:"insecureSkipTLSVerify" envconfig:"K6_INFLUXDB_INSECURE"`
	PushInterval          types.NullDuration `json:"pushInterval" envconfig:"K6_INFLUXDB_PUSH_INTERVAL"`
	ConcurrentWrites      null.Int           `json:"concurrentWrites" envconfig:"K6_INFLUXDB_CONCURRENT_WRITES"`
	Precision             types.NullDuration `json:"precision" envconfig:"K6_INFLUXDB_PRECISION"`
	TagsAsFields          []string           `json:"tagsAsFields,omitempty" envconfig:"K6_INFLUXDB_TAGS_AS_FIELDS"`

	// Aggregation holds the optional JMeter-style aggregation settings. When
	// disabled (the default) the output sends raw per-sample points unchanged.
	Aggregation AggregationConfig `json:"aggregation"`
}

// AggregationConfig contains the optional JMeter-style metric aggregation
// settings. When Enabled is false (the default), the output keeps its original
// raw per-sample behavior. When true, samples are aggregated over a time window
// and only summary points are written, drastically reducing the load on InfluxDB.
type AggregationConfig struct {
	// Enabled turns aggregation on. Default false (raw per-sample output).
	Enabled null.Bool `json:"enabled" envconfig:"K6_INFLUXDB_AGGREGATION_ENABLED"`
	// FlushInterval is how often aggregated summaries are written. Default 5s.
	FlushInterval types.NullDuration `json:"flushInterval" envconfig:"K6_INFLUXDB_AGGREGATION_FLUSH_INTERVAL"`
	// Measurement is the InfluxDB measurement name for aggregated points.
	Measurement null.String `json:"measurement" envconfig:"K6_INFLUXDB_AGGREGATION_MEASUREMENT"`
	// Percentiles is the list of response-time percentiles to compute (e.g. 90, 95, 99).
	Percentiles []float64 `json:"percentiles,omitempty" envconfig:"K6_INFLUXDB_AGGREGATION_PERCENTILES"`
	// DropTags is the denylist of high-cardinality tags that are excluded from the
	// aggregation grouping. Every other tag (built-in or custom) is kept automatically.
	// Defaults to vu, iter and url.
	DropTags []string `json:"dropTags,omitempty" envconfig:"K6_INFLUXDB_AGGREGATION_DROP_TAGS"`
}

// NewConfig creates a new InfluxDB output config with some default values.
func NewConfig() Config {
	c := Config{
		Addr:             null.NewString("http://localhost:8086", false),
		TagsAsFields:     []string{"vu:int", "iter:int", "url"},
		ConcurrentWrites: null.NewInt(4, false),
		PushInterval:     types.NewNullDuration(time.Second, false),
		Aggregation: AggregationConfig{
			Enabled:       null.NewBool(false, false),
			FlushInterval: types.NewNullDuration(5*time.Second, false),
			Measurement:   null.NewString("k6_aggregated", false),
			Percentiles:   []float64{90, 95, 99},
			DropTags:      []string{"vu", "iter", "url"},
		},
	}
	return c
}

// Apply overrides internal configuration with received values.
func (c Config) Apply(cfg Config) Config {
	if cfg.Addr.Valid {
		c.Addr = cfg.Addr
	}
	if cfg.Organization.Valid {
		c.Organization = cfg.Organization
	}
	if cfg.Bucket.Valid {
		c.Bucket = cfg.Bucket
	}
	if cfg.Token.Valid {
		c.Token = cfg.Token
	}
	if cfg.InsecureSkipTLSVerify.Valid {
		c.InsecureSkipTLSVerify = cfg.InsecureSkipTLSVerify
	}
	if len(cfg.TagsAsFields) > 0 {
		c.TagsAsFields = cfg.TagsAsFields
	}
	if cfg.PushInterval.Valid {
		c.PushInterval = cfg.PushInterval
	}
	if cfg.ConcurrentWrites.Valid {
		c.ConcurrentWrites = cfg.ConcurrentWrites
	}
	if cfg.Precision.Valid {
		c.Precision = cfg.Precision
	}
	if cfg.Aggregation.Enabled.Valid {
		c.Aggregation.Enabled = cfg.Aggregation.Enabled
	}
	if cfg.Aggregation.FlushInterval.Valid {
		c.Aggregation.FlushInterval = cfg.Aggregation.FlushInterval
	}
	if cfg.Aggregation.Measurement.Valid {
		c.Aggregation.Measurement = cfg.Aggregation.Measurement
	}
	if len(cfg.Aggregation.Percentiles) > 0 {
		c.Aggregation.Percentiles = cfg.Aggregation.Percentiles
	}
	if len(cfg.Aggregation.DropTags) > 0 {
		c.Aggregation.DropTags = cfg.Aggregation.DropTags
	}
	return c
}

// parseJSON parses the supplied JSON into a Config.
func parseJSON(data json.RawMessage) (Config, error) {
	conf := Config{}
	err := json.Unmarshal(data, &conf)
	return conf, err
}

// parseURL parses the supplied URL into a Config.
func parseURL(text string) (Config, error) {
	c := Config{}
	u, err := url.Parse(text)
	if err != nil {
		return c, err
	}
	if u.Host != "" {
		c.Addr = null.StringFrom(u.Scheme + "://" + u.Host)
	}
	if bucket := strings.TrimPrefix(u.Path, "/"); bucket != "" {
		c.Bucket = null.StringFrom(bucket)
	}
	return c, err
}

// GetConsolidatedConfig combines {default config values + JSON config +
// environment vars + URL config values}, and returns the final result.
func GetConsolidatedConfig(
	jsonRawConf json.RawMessage, env map[string]string, url string,
) (Config, error) {
	result := NewConfig()
	if jsonRawConf != nil {
		jsonConf, err := parseJSON(jsonRawConf)
		if err != nil {
			return result, err
		}
		result = result.Apply(jsonConf)
	}

	envConfig := Config{}
	if err := envconfig.Process("", &envConfig, func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}); err != nil {
		return result, err
	}
	result = result.Apply(envConfig)

	if url != "" {
		urlConf, err := parseURL(url)
		if err != nil {
			return result, err
		}
		result = result.Apply(urlConf)
	}

	return result, nil
}
