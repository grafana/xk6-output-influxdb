package influxdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/lib/testutils"
	"go.k6.io/k6/metrics"
	"go.k6.io/k6/output"
)

func TestNew(t *testing.T) {
	t.Parallel()
	logger := testutils.NewLogger(t)

	t.Run("BucketRequired", func(t *testing.T) {
		t.Parallel()
		_, err := New(output.Params{
			Logger:         logger,
			ConfigArgument: "/",
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "Bucket option is required")
	})
	t.Run("ConcurrentWrites", func(t *testing.T) {
		t.Parallel()

		t.Run("FailWithNegative", func(t *testing.T) {
			t.Parallel()
			tests := []string{"0", "-2"}
			for _, tc := range tests {
				_, err := New(output.Params{
					Logger:     logger,
					JSONConfig: json.RawMessage(fmt.Sprintf(`{"bucket":"b","concurrentWrites":%q}`, tc)),
				})
				require.Error(t, err)
				require.Equal(t, "the ConcurrentWrites option must be a positive number", err.Error())
			}
		})

		t.Run("SuccessWithPositive", func(t *testing.T) {
			t.Parallel()

			_, err := New(output.Params{
				Logger:     logger,
				JSONConfig: json.RawMessage(`{"bucket":"b","concurrentWrites":"2"}`),
			})
			require.NoError(t, err)
		})
	})
}

func TestExtractTagsToValues(t *testing.T) {
	t.Parallel()
	o, err := New(output.Params{
		Logger:     testutils.NewLogger(t),
		JSONConfig: []byte(`{"bucket":"mybucket","tagsAsFields":["stringField","stringField2:string","boolField:bool","floatField:float","intField:int"]}`),
	})
	require.NoError(t, err)
	tags := map[string]string{
		"stringField":  "string",
		"stringField2": "string2",
		"boolField":    "true",
		"floatField":   "3.14",
		"intField":     "12345",
	}
	values := o.extractTagsToValues(tags, map[string]interface{}{})

	require.Equal(t, "string", values["stringField"])
	require.Equal(t, "string2", values["stringField2"])
	require.Equal(t, true, values["boolField"])
	require.Equal(t, 3.14, values["floatField"])
	require.Equal(t, int64(12345), values["intField"])
}

func testOutputCycle(t testing.TB, handler http.HandlerFunc, body func(testing.TB, *Output)) {
	ts := httptest.NewServer(handler)
	defer ts.Close()

	c, err := New(output.Params{
		Logger:         testutils.NewLogger(t),
		ConfigArgument: fmt.Sprintf("%s/testbucket", ts.URL),
	})
	require.NoError(t, err)

	require.NoError(t, c.Start())
	body(t, c)

	require.NoError(t, c.Stop())
}

func TestOutputFlushMetrics(t *testing.T) {
	t.Parallel()

	var samplesRead int
	defer func() {
		require.Equal(t, 20, samplesRead)
	}()

	registry := metrics.NewRegistry()

	testOutputCycle(t, func(rw http.ResponseWriter, r *http.Request) {
		b := bytes.NewBuffer(nil)
		_, _ = io.Copy(b, r.Body)
		for {
			s, err := b.ReadString('\n')
			if len(s) > 0 {
				samplesRead++
			}
			if err != nil {
				break
			}
		}
		rw.WriteHeader(http.StatusNoContent)
	}, func(tb testing.TB, c *Output) {
		samples := make(metrics.Samples, 10)
		for i := 0; i < len(samples); i++ {
			metric, err := registry.NewMetric("test_gauge", metrics.Gauge)
			require.NoError(tb, err)
			samples[i] = metrics.Sample{
				TimeSeries: metrics.TimeSeries{
					Metric: metric,
					Tags: registry.RootTagSet().WithTagsFromMap(map[string]string{
						"something": "else",
						"VU":        "21",
						"else":      "something",
					}),
				},
				Time:  time.Now(),
				Value: 2.0,
			}
		}
		c.AddMetricSamples([]metrics.SampleContainer{samples})
		c.AddMetricSamples([]metrics.SampleContainer{samples})
	})
}

func TestMakeFieldKinds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		tagsAsFields []string
		expErr       bool
		expFields    map[string]FieldKind
	}{
		{
			name:         "Success",
			tagsAsFields: []string{"vu", "boolField:bool", "floatField:float", "intField:int"},
			expErr:       false,
			expFields:    map[string]FieldKind{"vu": String, "boolField": Bool, "floatField": Float, "intField": Int},
		},
		{
			name:         "Success without seprator",
			tagsAsFields: []string{"iter;bool"}, // this is detected as a string type
			expErr:       false,
			expFields:    map[string]FieldKind{"iter;bool": String},
		},
		{
			name:         "Duplicated field",
			tagsAsFields: []string{"vu", "iter", "url", "boolField:bool", "boolField:bool"},
			expErr:       true,
			expFields:    nil,
		},
		{
			name:         "Duplicated field with different kinds",
			tagsAsFields: []string{"vu", "boolField:bool", "boolField:float"},
			expErr:       true,
			expFields:    nil,
		},
		{
			name:         "Bad type",
			tagsAsFields: []string{"boolField:book"},
			expErr:       true,
			expFields:    nil,
		},
	}
	for _, tc := range tests {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			conf := NewConfig()
			conf.TagsAsFields = tc.tagsAsFields
			fieldKinds, err := makeFieldKinds(conf)
			if tc.expErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.expFields, fieldKinds)
		})
	}
}
