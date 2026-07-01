# xk6-output-influxdb
k6 extension for [InfluxDB v2](https://docs.influxdata.com/influxdb/v2.0), it adds the support for the latest `v2` version and the compatibility API for v1.8+.

#### **Why is this output not directly part of `k6` core?**
The `k6` core already supports the [InfluxDB v1](https://k6.io/docs/results-visualization/influxdb-+-grafana) so the natural feeling would be to do the same for the `v2`. Unfortunately, the `v2` has introduced some breaking changes in the core parts of the API. This would make it difficult to support both versions without taking a bunch of compromises for maintaining the retro-compatibility or introducing breaking changes in the current user experience of the k6's InfluxDB output, with a high probability to create more confusion for k6's users. For this main reason, the `k6` development team has decided to create a new and independent extension for InfluxDB v2.

# Install

To build a `k6` binary with this extension, first ensure you have the prerequisites:

- [Go toolchain](https://go101.org/article/go-toolchain.html)
- Git
- [xk6](https://github.com/grafana/xk6#install)

1. Build with `xk6`:

```bash
xk6 build --with github.com/grafana/xk6-output-influxdb
```

This will result in a `k6` binary in the current directory.

2. Run with the just built `k6` binary:

```bash
K6_INFLUXDB_ORGANIZATION=<insert-here-org-name> \
K6_INFLUXDB_BUCKET=<insert-here-bucket-name> \
K6_INFLUXDB_TOKEN=<insert-here-valid-token> \
./k6 run -o xk6-influxdb=http://localhost:8086 <script.js>
```

**Using Docker**

This [Dockerfile](./Dockerfile) builds a docker image with the k6 binary.

## Configuration

Options for fine-grained control for flushing and connections.


| ENV | Default | Description |
|-----|---------|-------------|
| K6_INFLUXDB_ORGANIZATION      |                       | The [Organization](https://docs.influxdata.com/influxdb/v2.0/reference/glossary/#organization). |
| K6_INFLUXDB_BUCKET            |                       | The [Bucket](https://docs.influxdata.com/influxdb/v2.0/reference/glossary/#bucket). |
| K6_INFLUXDB_TOKEN             |                       | The [Token](https://docs.influxdata.com/influxdb/v2.0/reference/glossary/#token). |
| K6_INFLUXDB_ADDR              | http://localhost:8086 | The address of the instance. |
| K6_INFLUXDB_PUSH_INTERVAL     | 1s | The flush's frequency of the `k6` metrics. |
| K6_INFLUXDB_CONCURRENT_WRITES | 4 | Number of concurrent requests for flushing data. It is useful when a request takes more than the expected time (more than flush interval). |
| K6_INFLUXDB_TAGS_AS_FIELDS    | vu:int,iter:int,url | A comma-separated string to set `k6` metrics as non-indexable fields (instead of tags). An optional type can be specified using :type as in vu:int will make the field integer. The possible field types are int, bool, float and string, which is the default. Example: vu:int,iter:int,url:string,event_time:int. |
| K6_INFLUXDB_INSECURE          | false | When `true`, it will skip `https` certificate verification. |
| K6_INFLUXDB_PRECISION         | 1ns | The timestamp [Precision](https://docs.influxdata.com/influxdb/v2.0/reference/glossary/#precision). |


## Aggregated metrics (JMeter-style)

By default this output writes **one InfluxDB point per metric sample**, which can overwhelm InfluxDB under high load. You can instead enable optional **aggregation**, which buffers samples over a time window and writes compact JMeter-style summaries (count / avg / min / max / percentiles per request and group, split into `ok`/`ko`/`all`), drastically reducing the number of points written.

Aggregation is **off by default**; when off, behavior is unchanged. Enable it with `K6_INFLUXDB_AGGREGATION_ENABLED=true`.

Your tags — both k6 built-in tags and your own custom tags (set via test-wide `tags` in options or per-request) — are carried into the aggregated output **automatically**, with no extra configuration. Only a small denylist of known high-cardinality tags (`vu`, `iter`, `url`) is excluded so the series count stays manageable.

| ENV | Default | Description |
|-----|---------|-------------|
| K6_INFLUXDB_AGGREGATION_ENABLED        | false | When `true`, send aggregated summaries instead of raw per-sample points. |
| K6_INFLUXDB_AGGREGATION_FLUSH_INTERVAL | 5s    | How often aggregated summaries are written. |
| K6_INFLUXDB_AGGREGATION_MEASUREMENT    | k6_aggregated | InfluxDB measurement name for aggregated points. |
| K6_INFLUXDB_AGGREGATION_PERCENTILES    | 90,95,99 | Response-time percentiles to compute (rendered as fields `p90`/`pct90.0`, depending on schema — see below). |
| K6_INFLUXDB_AGGREGATION_DROP_TAGS      | vu,iter,url | Comma-separated denylist of high-cardinality tags excluded from the aggregation grouping. Every other tag (built-in or custom) is kept automatically. |
| K6_INFLUXDB_AGGREGATION_SCHEMA         | k6 | Tag/field naming scheme for aggregated points: `k6` (this output's own naming) or `jmeter`, which mirrors a real JMeter InfluxDB backend listener's schema so existing JMeter dashboards/queries can be reused against k6 data. |

### `k6` schema (default)

- Tags: `name` (request name), `group`, `statut` (`ok`/`ko`/`all`), plus every other tag on the sample (your custom tags such as `testid`, plus built-in tags like `method`, `scenario`) except those in the denylist.
- Fields (per request/group row): `count`, `avg`, `min`, `max`, `hits`, and one field per configured percentile (`p90`, `p95`, `p99`, ...).
- Per-response-code rows (tags `status` = HTTP code such as `200`/`404`/`0`, `error` = message when not a success) with a `count` field. These give a full response-code distribution — **including successful 2xx** — even though the main ok/ko/all rows omit the status code. Counts are additive across flush windows, so a response-code chart is exact at any time range. (`status=0` denotes a transport-level failure: connection refused, timeout, DNS, etc.)
- A cumulative row (`name=all`, `statut=all`) with exact run-wide `count`, `avg`, `min`, `max`, percentiles, `hits`, `countError`, plus `data_sent` and `data_received`. (`data_sent`/`data_received` are run-wide totals because k6 measures them at the connection level and cannot attribute them to individual requests.)
- A `name=internal` row with active-VU metrics `minAT`, `maxAT`, `meanAT`.

### `jmeter` schema (opt-in, `K6_INFLUXDB_AGGREGATION_SCHEMA=jmeter`)

Structurally identical to a real JMeter InfluxDB backend listener's output (verified against a live JMeter-written bucket), so existing JMeter dashboards/queries work unmodified against k6 data — only the values differ, not the shape:

- Tags: `transaction` (k6's `group` + `name` combined, e.g. `checkout::GET /cart`), `statut` (`ok`/`ko`/`all`). k6-only tags with no JMeter equivalent (`method`, `proto`, `tls_version`, `scenario`) are dropped; any other custom tag (e.g. `application`, `testTitle` set via k6's `options.tags`) still passes through.
- Fields (per request/group row): `count`, `avg`, `min`, `max`, `hit`, and one field per configured percentile (`pct90.0`, `pct95.0`, `pct99.0`, ...).
- Per-response-code rows (tags `responseCode`, `responseMessage` — `"OK"` on success) with a `count` field, no `statut` tag. Same additive-counts behavior as the k6 schema.
- A cumulative row (`transaction=all`, `statut=all`) with exact run-wide `count`, `avg`, `min`, `max`, percentiles, `hit`, `countError`. No `data_sent`/`data_received` (no JMeter equivalent).
- A `transaction=internal` row with active-VU metrics `minAT`, `maxAT`, `meanAT`.
- Real JMeter's `rb`/`sb` (per-request byte counts) and `startedT`/`endedT` (threads started/ended per interval) are **not** emitted — k6 has no equivalent per-request data.

In both schemas the response-time signal is k6's `http_req_duration`; `ok`/`ko` is derived from the sample's `expected_response` tag. The synthetic ok/ko/all split is exposed as the `statut` tag, kept separate from k6's built-in `status` (HTTP status code) tag.


# Docker Compose

This repo includes a [docker-compose.yml](./docker-compose.yml) file that starts InfluxDB, Grafana and k6. This is just a quick setup to show the usage; for real use case you might want to deploy outside of docker, use volumes and probably update versions.

Clone the repo to get started and follow these steps: 

1. Put your k6 scripts in the `samples` directory or use the `http_2.js` example.

2. Start the docker compose environment.
   
	```shell
	docker compose up -d
	```

	```shell
	# Output
	Creating xk6-output-influxdb_influxdb_1 ... done
	Creating xk6-output-influxdb_k6_1       ... done
	Creating xk6-output-influxdb_grafana_1  ... done
	```

3. Use the k6 Docker image to run the k6 script and send metrics to the InfluxDB container started on the previous step. You must [set the `testid` tag](https://k6.io/docs/using-k6/tags-and-groups/#test-wide-tags) with a unique identifier to segment the metrics into discrete test runs for the Grafana dashboards.
    ```shell
    docker compose run --rm -T k6 run -<samples/http_2.js --tag testid=<SOME-ID>
    ```
   For convenience, the `docker-run.sh` can be used to simply:
    ```shell
    ./docker-run.sh samples/http_2.js
    ```

4. Visit http://localhost:3000/ to view results in Grafana. 
	> This repository includes a [basic dashboard](./grafana/dashboards/dashboard.yml). If you want to build a custom Dashboard, contact the k6 team in Slack.


### Compatibility API
The v2 includes a [InfluxDB v1.8+ compatibility API](https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x) that adds endpoints for communicating with an InfluxDB v1.

>[Client API usage differences summary](https://github.com/influxdata/influxdb-client-go#influxdb-18-api-compatibility):
>
>    1. Use the form username:password for an authentication token. Example: my-user:my-password. Use an empty string ("") if the server doesn't require authentication.
>    2. The organization parameter is not used. Use an empty string ("") where necessary.
>    3. Use the form database/retention-policy where a bucket is required. Skip retention policy if the default retention policy should be used. Examples: telegraf/autogen, telegraf.
