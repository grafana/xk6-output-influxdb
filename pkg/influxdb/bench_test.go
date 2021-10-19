package influxdb

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/core/local"
	"go.k6.io/k6/js"
	"go.k6.io/k6/lib"
	"go.k6.io/k6/lib/metrics"
	"go.k6.io/k6/lib/testutils"
	"go.k6.io/k6/loader"
	"go.k6.io/k6/output"
	"go.k6.io/k6/stats"
)

type requestCount struct {
	Target     http.RoundTripper
	successful int64
}

func (rc *requestCount) RoundTrip(r *http.Request) (*http.Response, error) {
	res, err := rc.Target.RoundTrip(r)
	if res.StatusCode == http.StatusNoContent {
		atomic.AddInt64(&rc.successful, 1)
	}
	return res, err
}

func (rc *requestCount) Completed() int64 {
	return atomic.LoadInt64(&rc.successful)
}

func TestOutputThroughput(t *testing.T) {
	if os.Getenv("K6_INFLUXDB_ORGANIZATION") == "" ||
		os.Getenv("K6_INFLUXDB_BUCKET") == "" ||
		os.Getenv("K6_INFLUXDB_TOKEN") == "" {
		t.Skip("env vars are not configured, skip the integration test for throughput benchmark")
	}

	o, err := New(output.Params{
		Logger:         testutils.NewLogger(t),
		ConfigArgument: "http://localhost:8086",
	})
	require.NoError(t, err)

	copts := o.client.Options()
	httpc := copts.HTTPClient()
	require.NoError(t, err)

	rc := &requestCount{Target: httpc.Transport}
	httpc.Transport = rc
	copts.SetHTTPClient(httpc)

	require.NoError(t, o.Start())

	srcdata := &loader.SourceData{
		URL: &url.URL{Path: "script.js"},
		Data: []byte(`
 import { sleep } from 'k6';

export let options = {
scenarios: {
bench: {
executor: 'constant-vus',
vus: 100000,
duration: '10s',
}
}
}

export default function() {
sleep(0.5)
}`),
	}

	logger := logrus.New()
	logger.SetOutput(testutils.NewTestOutput(t))
	registry := metrics.NewRegistry()
	builtinMetrics := metrics.RegisterBuiltinMetrics(registry)
	runner, err := js.New(logger, srcdata, nil, lib.RuntimeOptions{}, builtinMetrics, registry)
	require.NoError(t, err)

	execScheduler, err := local.NewExecutionScheduler(runner, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	samples := make(chan stats.SampleContainer)
	go func() {
		err := execScheduler.Init(ctx, samples)
		require.NoError(t, err)

		err = execScheduler.Run(ctx, ctx, samples, builtinMetrics)
		require.NoError(t, err)
		close(done)
	}()
	for {
		select {
		case container := <-samples:
			containers := []stats.SampleContainer{container}
			o.AddMetricSamples(containers)
		case <-done:
			require.NoError(t, o.Stop())
			assert.InDelta(t, 12, rc.Completed(), 2)
			return
		case <-time.After(20 * time.Second):
			t.Error("timed out: the benchmark is taking more than expected")
			return
		}
	}
}

func BenchmarkWritePoints(b *testing.B) {
	if os.Getenv("K6_INFLUXDB_ORGANIZATION") == "" ||
		os.Getenv("K6_INFLUXDB_BUCKET") == "" ||
		os.Getenv("K6_INFLUXDB_TOKEN") == "" {
		b.Skip("env vars are not configured, skip the integration test for WritePoints benchmark")
	}

	ctx := context.Background()
	o, err := New(output.Params{
		Logger:         testutils.NewLogger(b),
		ConfigArgument: "http://localhost:8086",
	})
	require.NoError(b, err)

	samples := make(stats.Samples, 10)
	for i := 0; i < len(samples); i++ {
		samples[i] = stats.Sample{
			Metric: stats.New("testGauge", stats.Gauge),
			Time:   time.Now(),
			Tags: stats.NewSampleTags(map[string]string{
				"something": "else",
				"VU":        "21",
				"else":      "something",
			}),
			Value: 2.0,
		}
	}
	batch := o.batchFromSamples([]stats.SampleContainer{samples})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := o.pointWriter.WritePoint(ctx, batch...)
		if err != nil {
			b.Fatal(err)
		}
	}
}
