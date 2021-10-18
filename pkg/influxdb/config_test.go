package influxdb

import (
	"testing"

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
