# Distributed URL Shortener

Go API replicas serve immutable short links from DynamoDB with a disposable Redis cache. Redirects make a bounded SQS Standard send; separate workers apply deduplicated click increments to 16 DynamoDB shards. See [DESIGN.md](DESIGN.md) for the architecture and tradeoffs.

## Run locally

Prerequisites: Docker with Compose. Native builds require Go 1.25 or newer (the container builds with Go 1.26). Only the approved AWS SDK, go-redis and Prometheus client families are application dependencies.

```sh
docker compose up --build -d
docker compose ps
curl -i http://localhost:8080/health/ready
curl -sS http://localhost:8080/shorten \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/some/long/path"}'
```

Open **http://localhost:8080/** for the browser client. Paste an HTTP(S) URL, select **Shorten URL**, then select **Open short link** to follow the redirect in a new tab. The page displays API errors and the link expiry. After changing the client, rebuild with `docker compose up --build -d`.

Use the returned code in `curl -i http://localhost:8080/CODE` and `curl http://localhost:8080/stats/CODE`. Redirects return 302; counts become visible after the worker commits. Unknown codes return 404. Expired links return 410 while their DynamoDB item still exists, then 404 after TTL cleanup. POST rejects credentials, non-HTTP(S) targets, oversized URLs, malformed JSON and caller-selected expiry.

Compose starts two APIs, one worker, HAProxy, DynamoDB Local, Redis, LocalStack SQS, Prometheus, Grafana and a Redis exporter. The one-shot `setup` container provisions tables, TTL settings, the Standard queue, and its dead-letter queue before applications start. API/worker startup never creates infrastructure. DynamoDB Local may need a few seconds to start; setup retries on failure.

- API: http://localhost:8080
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 — `admin` / `local-development-only`; set `GRAFANA_PASSWORD` before starting to change it.
- Grafana includes the **URL Shortener** dashboard automatically.

Published ports bind only to localhost. HAProxy blocks `/metrics` and `/debug`; Prometheus scrapes container addresses directly. DynamoDB, LocalStack and Redis have no host ports. Native operational endpoints are `GET /health/live`, `GET /health/ready`, and `GET /metrics`.

```sh
docker compose logs -f api worker setup
docker compose up -d --scale api=4 --scale worker=2
docker compose stop
docker compose start
```

HAProxy discovers up to 16 API replicas through Docker DNS; Prometheus discovers API/worker replicas independently. DynamoDB data survives container recreation through a named volume. Redis is intentionally disposable. LocalStack queues survive an application worker restart, but this local configuration does **not** persist SQS across LocalStack container recreation; use AWS for durability evaluation. Run `docker compose up -d --force-recreate setup` after recreating LocalStack to provision its queues again.

## Configuration

All processes read environment variables. Defaults target native localhost dependencies; Compose supplies container addresses and emulator credentials.

| Variable | Default | Meaning |
|---|---|---|
| `LISTEN_ADDR` | `:8080` | API or worker metrics/health listener |
| `BASE_URL` | `http://localhost:8080` | Public HTTP(S) origin; never derived from request headers |
| `LINK_TTL` | `720h` | Immutable link lifetime |
| `MAX_URL_LENGTH` | `2048` | Input URL bytes |
| `MAX_REQUESTS` | `256` | API concurrency; excess requests get 429 |
| `REQUEST_TIMEOUT` | `2s` | Request/database and non-poll SQS operation budget |
| `AWS_REGION` | `us-east-1` | AWS region |
| `DYNAMODB_ENDPOINT`, `SQS_ENDPOINT` | unset | Emulator overrides; leave unset on AWS |
| `LINKS_TABLE`, `STATS_TABLE`, `EVENTS_TABLE` | `Links`, `LinkStats`, `ProcessedEvents` | Table names |
| `CACHE_REDIS_URL` | `redis://localhost:6379` | Supports authenticated Redis and `rediss://` TLS |
| `REDIS_TIMEOUT` | `50ms` | Cache operation deadline; errors open a 2s cooldown |
| `SQS_QUEUE_URL`, `SQS_DLQ_URL` | unset | Provisioned queue URLs; explicit URLs recommended on AWS |
| `SQS_QUEUE_NAME`, `SQS_DLQ_NAME` | `redirects`, `redirects-dlq` | Local setup/discovery names |
| `SQS_ENQUEUE_TIMEOUT` | `50ms` | Maximum redirect enqueue wait |
| `SQS_VISIBILITY_TIMEOUT` | `30s` | Receive visibility; must cover processing and deletion |
| `WORKER_CONCURRENCY`, `WORKER_BATCH_SIZE` | `8`, `8` | Effective batch concurrency is the smaller value; SQS batches max 10 |
| `MAX_EVENT_ATTEMPTS` | `10` | Local queue redrive receive count |
| `SQS_RETENTION`, `SQS_DLQ_RETENTION` | `96h`, `336h` | Local queue retention settings |
| `DEDUPE_TTL` | `1080h` | Deduplication retention, 45 days |

Use Go duration strings (`30s`, `720h`), not `30d`. Queue configuration variables provision local queues; changing them does not mutate existing queues at application startup. Update queue infrastructure consistently when changing deployment settings. Keep deduplication TTL longer than the entire retention/redrive horizon; the configuration requires it to exceed source retention plus twice DLQ retention. With defaults, finish redrives within 32 days of the original event and never replay archived events beyond that horizon. Do not change the 16-shard mapping for existing traffic without migrating stats.

The AWS SDK uses its standard credential chain, including workload IAM roles and local profiles. Do not use emulator credentials for AWS. No destination URLs are fetched, and logs omit raw URLs, query strings and client IPs.

## Delivery and failure semantics

- Create uses a cryptographic 10-character Base62 code and a conditional write. Ambiguous write responses reserve time for a strongly consistent read; a matching creation token confirms success. Five collision attempts fit inside one request budget.
- Cache hits still enforce absolute expiry. Positive TTL is jittered downward from 24h and capped at expiry; negative results live for 30s. A cache failure falls back to DynamoDB after a short deadline and opens a cooldown.
- Each redirect generates one UUID. SDK send retries reuse the same event body. An unconfirmed SQS send increments `analytics_events_dropped_total`, but the redirect still succeeds. Ambiguous sends can be accepted even when that counter increments.
- Workers long-poll for up to 20s and process bounded batches without prefetching more than they can execute. Each transaction conditionally inserts the event ID and increments one deterministic stats shard. Delete happens only after commit or strongly verified deduplication.
- Failed transactions stay in SQS. Visibility changes implement exponential jittered backoff. Throttling halves worker concurrency; ten healthy batches recover one slot. The SQS redrive policy moves repeatedly failing messages to the DLQ.
- SIGTERM stops new work. HTTP requests drain for up to 10s; workers finish the current bounded batch and leave anything uncommitted for visibility-based redelivery. There is no unbounded fire-and-forget analytics buffer.
- API readiness checks DynamoDB. Redis and SQS outages do not make an already-running API unready, matching the availability policy. Worker readiness checks DynamoDB and SQS.

## Monitoring and runbooks

The Grafana dashboard shows traffic, redirect p50/p95/p99, errors/rejections, cache hit ratio, dependency latency/errors, consumed DynamoDB capacity, analytics throughput/drops, visible/in-flight/DLQ depths, received-event age, process CPU/memory, goroutines and Redis memory. Labels never include URL codes or destinations. Queue gauges are duplicated on each worker; aggregate them with `max`, not `sum`.

SQS `GetQueueAttributes` exposes approximate depths, not queue-wide oldest-message age. `analytics_event_age_seconds` measures the messages workers actually receive and is not a substitute when workers are stopped. The AWS stack supplies a CloudWatch dashboard and alarm using SQS `ApproximateAgeOfOldestMessage`. See [AWS's SQS metric definitions](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-available-cloudwatch-metrics.html).

Prometheus rules cover API errors and latency, analytics drops/stale deliveries, DLQ depth, DynamoDB errors, Redis availability/memory and low cache hit ratio. Rules are visible in Prometheus; external alert delivery requires your Alertmanager or an SNS topic for the AWS alarms. Structured logs carry request IDs, W3C trace IDs and bounded error classes. Successful redirect and worker logs are sampled at 1%; HTTP errors are always logged. Trace context propagates into the event; this is log correlation, with no OTLP span exporter configured.

| Symptom | Inspect / action |
|---|---|
| Rising 429 rate | API CPU, active requests, dependency latency; add API replicas when CPU-bound, or resolve dependency pressure |
| Cache errors or low hit ratio | Redis exporter and cache cooldown counters; expect higher DynamoDB reads while restoring cache |
| Redirects succeed but analytics drops rise | SQS reachability, IAM, send latency and enqueue timeout; already lost events cannot be reconstructed |
| Queue age/depth grows | Worker availability, transaction throttles and CPU; add workers only when DynamoDB has capacity |
| DynamoDB throttles | Capacity guard, consumed units and hot shards; reduce worker concurrency or explicitly adjust the guard |
| DLQ has messages | Inspect payload/error classes, fix the cause, then redrive with the original event IDs within the documented dedupe horizon |
| Worker restart | Expect in-flight messages to return after visibility timeout; duplicate delivery must not add another click |

For local queue inspection, run `docker compose exec localstack awslocal sqs list-queues` and use the returned URL with `get-queue-attributes --attribute-names All`. Avoid deleting or purging the queue as a recovery step. SQS automatically owns dead-letter routing; `analytics_dead_letters_total` counts failed deliveries at the redrive threshold, while `analytics_dlq_depth` and the CloudWatch alarm confirm actual DLQ backlog.

## Isolated AWS data environment

`deploy/aws-data.yaml` provisions three encrypted, on-demand DynamoDB tables with point-in-time recovery and explicit per-table throughput guards, an encrypted SQS Standard queue and DLQ, separate least-privilege API/worker managed policies, and a CloudWatch dashboard/alarms. Table and queue resources are retained on stack deletion to prevent accidental loss. It does not provision compute, networking, TLS certificates or managed Redis.

Deploy into an isolated account/environment when you choose to incur AWS charges:

```sh
aws cloudformation deploy --stack-name shortener-evaluation \
  --template-file deploy/aws-data.yaml --capabilities CAPABILITY_IAM
aws cloudformation describe-stacks --stack-name shortener-evaluation \
  --query 'Stacks[0].Outputs'
```

Use outputs for `LINKS_TABLE`, `STATS_TABLE`, `EVENTS_TABLE`, `SQS_QUEUE_URL` and `SQS_DLQ_URL`. Attach the respective managed policy to each workload's IAM role. Explicit queue URLs avoid requiring queue-discovery IAM permissions. Run the same `api` / `worker` image entrypoints on your private compute with a TLS/authenticated managed Redis cache and AWS region; leave emulator endpoints unset. The deployment needs outbound HTTPS to AWS (or private service endpoints), public TLS only at ingress, and private metrics access. Add your SNS topic through `AlarmTopicARN` for external notification. Throughput guards limit request rate, not total spend; use account budgets for financial limits.

## Development and verification

Use the Makefile to run individual suites or all tests:

```sh
make unit         # Default for plain make; race-enabled unit tests
make integration  # HTTP + service integration; starts and cleans up test containers
make load         # Explicitly run the load test
make stress       # Explicitly run the race-enabled stress test
make all          # Run all four suites sequentially, including load and stress
```

`make integration` and `make all` require Docker Compose and free ports 18000, 16379 and 14566. Integration uses the disposable `shortener-tests` Compose project and tears it down on completion or failure. `make unit` excludes integration, load and stress tests. Load/stress retain their existing in-memory defaults.

Override settings on the command line, for example `make load LOAD_DURATION=30s LOAD_CONCURRENCY=16 TEST_TIMEOUT=2m`. Set `LOAD_BASE_URL=http://localhost:8080` to load-test a running application. `TEST_TIMEOUT` defaults to `2m` for each Go test invocation.

```sh
go build ./cmd/...
go vet ./...
gofmt -l .
go test -race ./... -skip '^Test(Load|Stress)'
```

Tests use testify. The normal suite covers HTTP validation, collisions, caching, expiry, dependency failures, concurrency admission, tracing, shutdown, configuration, identifiers, metrics, DynamoDB request/response handling, Redis commands and worker acknowledgement/retry behavior. HTTP integration tests use a real local listener with in-memory dependencies; AWS SDK tests use scripted HTTP transports and Redis unit tests use command hooks.

Full-service integration tests use the actual DynamoDB, Redis and SQS clients and analytics worker. They create uniquely named tables/queues, verify the shorten → redirect → worker → stats flow, duplicate delivery, cache repair and expiry, then delete their test resources. Start the isolated, disposable services (ports 18000, 16379 and 14566 must be free):

```sh
docker compose -p shortener-tests -f tests/compose.yaml up -d --wait --wait-timeout 60
RUN_INTEGRATION=1 \
  INTEGRATION_DYNAMODB_URL=http://localhost:18000 \
  INTEGRATION_REDIS_URL=redis://localhost:16379 \
  INTEGRATION_SQS_URL=http://localhost:14566 \
  go test -race ./... -skip '^Test(Load|Stress)' -count=1
docker compose -p shortener-tests -f tests/compose.yaml down
```

The service test skips unless `RUN_INTEGRATION=1`; when enabled, missing endpoints or unavailable services fail the test. Use isolated local emulators: the test supplies local credentials and provisions/deletes its own resources. The normal application Compose stack is separate.

Load and stress tests are opt-in and are never executed by the normal suite, even without `-skip`:

```sh
RUN_LOAD=1 LOAD_DURATION=10s LOAD_CONCURRENCY=8 \
  go test ./internal/api -run '^TestLoad' -count=1 -v -timeout=2m
RUN_STRESS=1 \
  go test -race ./internal/api -run '^TestStress' -count=1 -v -timeout=2m
```

The load test seeds 32 links, then repeatedly mixes GET redirects (70%), stats (20%) and HEAD redirects (10%) over real HTTP connections. It requires successful responses and reports throughput and p50/p95/p99 latency. Defaults are five seconds and eight concurrent clients. In-memory dependencies are used by default; set `LOAD_BASE_URL=http://localhost:8080` to exercise a running application and its dependencies. External runs create 32 links that remain until normal expiry; redirect analytics also persist. Choose concurrency below the target's admission capacity for this success-only load scenario. Configure `-timeout` to exceed `LOAD_DURATION` plus setup time. This is a closed-loop client workload, not a fixed-arrival-rate capacity measurement.

The stress test deliberately blocks storage while filling 1, 4, 16 and 64 API slots, then sends four times as many excess requests. It checks 429 rejections, bounded storage concurrency and recovery after releasing blocked requests. It uses controlled in-memory dependencies to make overload deterministic. No load, stress or benchmark run is part of normal checks.

Implementation verification on the local Compose stack covered create/redirect/stats, input rejection, 404/410 responses, duplicate SQS delivery (one aggregate increment), and continued redirects with Redis and SQS unavailable (drop metric incremented). All four Prometheus targets were healthy, Grafana loaded its dashboard, and `promtool` accepted the configuration and alert rules. Build, vet, formatting and the required race-enabled suite passed. The added service integration suite also passed against isolated DynamoDB Local, Redis and LocalStack SQS containers. AWS infrastructure was syntax-checked but has not been deployed or validated against AWS.

## Known limitations

- Load/stress suites and an open-loop 95/5 redirect/create generator (`cmd/loadgen`, `make loadgen`) are implemented, but no saved load/scaling results are included in this directory. Prior load/stress outcomes cannot be confirmed from the retained evidence; neither suite was run during this documentation update. Sustainable throughput, saturation point, bottleneck identification, and measurable before/after scaling improvement remain pending documented runs; no production SLO is claimed. See [the evaluation plan](DESIGN.md#10-load-test-and-scaling-plan). The deterministic stress suite covers API admission and recovery, not distributed infrastructure saturation or a long-running soak test.
- Source code packaging/GitHub delivery and Grafana screenshots/snapshots are excluded from this housekeeping task and remain outstanding deliverable evidence.
- Local emulators do not demonstrate AWS durability, latency, capacity or failure behavior. LocalStack SQS state is ephemeral across emulator recreation in this Compose deployment.
- Availability takes priority over complete analytics during failed or timed-out sends. SQS retention and the dedupe/redrive horizon bound recovery; infinite replay is unsupported.
- The AWS template provisions the data/monitoring plane only; production compute, private networking, managed Redis, TLS, secret management and alert destinations must be supplied by the deployment environment.
- Trace propagation/log correlation is included; distributed span collection and export are not configured. Queue-wide oldest-message age is available in AWS CloudWatch, not through the local application's queue-depth API.
- Stats reads sum 16 items and are not a single atomic snapshot. Shard `last_clicked_at` records the most recently processed event's timestamp; reordered SQS deliveries can move it backward. Only total clicks are exposed publicly.
- Authentication, billing, custom aliases, destination editing, malware scanning, multi-region operation and historical event replay are out of scope. Aggregate shard rows do not have a cleanup policy yet and outlive expired links.
