package influxdb

import (
	"testing"
	"time"

	"go.k6.io/k6/lib/types"

	"github.com/stretchr/testify/assert"
	"gopkg.in/guregu/null.v3"
)

func TestParseURL(t *testing.T) {
	t.Parallel()
	testdata := map[string]Config{
		"":                                 {Bucket: null.NewString("", false)},
		"bucketname":                       {Bucket: null.StringFrom("bucketname")},
		"/bucketname":                      {Bucket: null.StringFrom("bucketname")},
		"/dbname/retention":                {Bucket: null.StringFrom("dbname/retention")}, // 1.8+ API compatibility
		"http://localhost:8086":            {Addr: null.StringFrom("http://localhost:8086")},
		"http://localhost:8086/bucketname": {Addr: null.StringFrom("http://localhost:8086"), Bucket: null.StringFrom("bucketname")},
	}
	for str, data := range testdata {
		str, data := str, data
		t.Run(str, func(t *testing.T) {
			t.Parallel()
			config, err := parseURL(str)
			assert.NoError(t, err)
			assert.Equal(t, data, config)
		})
	}
}

func TestGetConsolidatedConfig(t *testing.T) {
	t.Parallel()
	duration999s, _ := time.ParseDuration("999s")
	testdata := map[string]string{
		"K6_INFLUXDB_ADDR":              "http://test-url",
		"K6_INFLUXDB_ORGANIZATION":      "test-org",
		"K6_INFLUXDB_BUCKET":            "test-bucket",
		"K6_INFLUXDB_TOKEN":             "test-token",
		"K6_INFLUXDB_INSECURE":          "true",
		"K6_INFLUXDB_PUSH_INTERVAL":     duration999s.String(),
		"K6_INFLUXDB_CONCURRENT_WRITES": "999",
		"K6_INFLUXDB_PRECISION":         duration999s.String(),
		"K6_INFLUXDB_TAGS_AS_FIELDS":    "test-tag-1,test-tag-2,test-tag-3",
	}

	check, err := GetConsolidatedConfig(nil, testdata, "http://test-url-override/test-bucket-override")
	assert.NoError(t, err)

	assert.Equal(t, null.StringFrom("http://test-url-override"), check.Addr)
	assert.Equal(t, null.StringFrom("test-org"), check.Organization)
	assert.Equal(t, null.StringFrom("test-bucket-override"), check.Bucket)
	assert.Equal(t, null.StringFrom("test-token"), check.Token)
	assert.Equal(t, null.BoolFrom(true), check.InsecureSkipTLSVerify)
	assert.Equal(t, types.NullDurationFrom(duration999s), check.PushInterval)
	assert.Equal(t, null.IntFrom(999), check.ConcurrentWrites)
	assert.Equal(t, types.NullDurationFrom(duration999s), check.Precision)
	assert.Equal(t, []string{"test-tag-1", "test-tag-2", "test-tag-3"}, check.TagsAsFields)
}
