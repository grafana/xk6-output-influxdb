/*
 *
 * k6 - a next-generation load testing tool
 * Copyright (C) 2016 Load Impact
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package influxdb

import (
	"encoding/json"
	"net/url"
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
	"go.k6.io/k6/lib/types"
	"gopkg.in/guregu/null.v3"
)

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
}

// NewConfig creates a new InfluxDB output config with some default values.
func NewConfig() Config {
	c := Config{
		Addr:             null.NewString("http://localhost:8086", false),
		TagsAsFields:     []string{"vu", "iter", "url"},
		ConcurrentWrites: null.NewInt(4, false),
		PushInterval:     types.NewNullDuration(time.Second, false),
	}
	return c
}

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
	jsonRawConf json.RawMessage, env map[string]string, url string) (Config, error) {
	result := NewConfig()
	if jsonRawConf != nil {
		jsonConf, err := parseJSON(jsonRawConf)
		if err != nil {
			return result, err
		}
		result = result.Apply(jsonConf)
	}

	envConfig := Config{}
	if err := envconfig.Process("", &envConfig); err != nil {
		// TODO: get rid of envconfig and actually use the env parameter...
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
