# xk6-output-influxdb

k6 output extension that ships test metrics to InfluxDB v2. Exists as a separate extension because the v2 API has breaking changes that cannot coexist with the v1 output bundled in k6 core.

## Architecture

The extension registers itself via k6's output extension system at init time. A registration shim at the repo root delegates to the implementation package.

Configuration consolidates from three sources in priority order: JSON config from k6 options, environment variables (all prefixed with `K6_INFLUXDB_`), and the URL passed via k6's output argument. Each layer overrides the previous. The bucket name is required; the address defaults to localhost:8086.

The output implementation buffers metric samples and flushes them to InfluxDB on a periodic interval. Flush runs in a goroutine pool bounded by a semaphore channel (default 4 concurrent writers). If a flush takes longer than the push interval, a warning is logged but no samples are dropped -- they queue for the next cycle.

Tags can be promoted to InfluxDB fields (making them non-indexable but queryable with more types) via a configuration option. Each promoted tag gets a type annotation (string/int/float/bool) parsed from a colon-delimited format. By default, VU number, iteration count, and URL are promoted to fields.

The tag-to-field extraction mutates the tags map in place -- it deletes promoted keys from the tags map and adds them to the values map. This is cached per unique tag set pointer for the duration of a single flush batch to avoid repeated conversion.

The InfluxDB client's internal logger is explicitly disabled at init time to prevent noisy output.

A docker-compose setup provides InfluxDB, Grafana, and k6 for local testing, with pre-provisioned Grafana dashboards for visualizing test results.

## Gotchas

- Flush errors are logged but silently swallowed. If InfluxDB is unreachable or rejects writes, metrics are lost with only a log line. There is no retry mechanism.

- The semaphore channel blocks the periodic flusher goroutine when all concurrent write slots are occupied. Under sustained backpressure, this causes sample buffer growth in memory without bound.

- The linter config is not checked in. It is downloaded from k6 core's master branch on first lint run. Do not commit it.

- Tests mock the InfluxDB server with Go's httptest. No real InfluxDB instance is needed for unit tests.
