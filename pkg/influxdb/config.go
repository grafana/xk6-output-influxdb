package influxdb

import (
	"encoding/json"
	"net/url"
	"strings"
	"time"

	"github.com/mstoykov/envconfig"
	"go.k6.io/k6/lib/types"
	"gopkg.in/guregu/null.v3"
)

// Config contains the configuration for the Output.
type Config struct {
	Addr                  null.String        `json:"addr" envconfig:"K6_INFLUXDB_ADDR"`
	Organization          null.String        `json:"organization" envconfig:"K6_INFLUXDB_ORGANIZATION"`
	Bucket                null.String        `json:"bucket" envconfig:"K6_INFLUXDB_BUCKET"`
	Token                 null.String        `json:"token" envconfig:"K6_INFLUXDB_TOKEN"`
	InsecureSkipTLSVerify null.Bool          `json:"insecureSkipTLSVerify,omitempty" envconfig:"K6_INFLUXDB_INSECURE"`
	PushInterval          types.NullDuration `json:"pushInterval,omitempty" envconfig:"K6_INFLUXDB_PUSH_INTERVAL"`
	ConcurrentWrites      null.Int           `json:"concurrentWrites,omitempty" envconfig:"K6_INFLUXDB_CONCURRENT_WRITES"`
	Precision             types.NullDuration `json:"precision,omitempty" envconfig:"K6_INFLUXDB_PRECISION"`
	TagsAsFields          []string           `json:"tagsAsFields,omitempty" envconfig:"K6_INFLUXDB_TAGS_AS_FIELDS"`
	EnableUniqueTag       null.Bool          `json:"enableUniqueTag,omitempty" envconfig:"K6_INFLUXDB_ENABLE_UNIQUE_TAG"`
	UniqueTagName         null.String        `json:"uniqueTagName,omitempty" envconfig:"K6_INFLUXDB_UNIQUE_TAG_NAME"`
}

// NewConfig creates a new InfluxDB output config with some default values.
func NewConfig() Config {
	c := Config{
		Addr:             null.NewString("http://localhost:8086", false),
		TagsAsFields:     []string{"vu:int", "iter:int", "url"},
		ConcurrentWrites: null.NewInt(4, false),
		PushInterval:     types.NewNullDuration(time.Second, false),
		EnableUniqueTag:  null.NewBool(false, false),
		UniqueTagName:    null.NewString("uniqueId", false),
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
	if cfg.EnableUniqueTag.Valid {
		c.EnableUniqueTag = cfg.EnableUniqueTag
	}
	if cfg.UniqueTagName.Valid {
		c.UniqueTagName = cfg.UniqueTagName
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
