# Load and scaling results

Status: **First measured run recorded (2026-09-23, local emulators)**. One 20 s stage per rate, single run each, so treat numbers as indicative.

The API, worker, local Compose stack, telemetry, and load generator are implemented. The required race suite passed during the preceding housekeeping update, and formatting was clean. Those checks do not establish throughput, saturation, or scaling improvement.

## Reproducible run record

For each run, record:

- Date/time and timezone, source version or archive identifier, and raw output locations.
- Host CPU/memory, container resource limits, and generator host/resources.
- API and worker replica counts, worker concurrency, request limits/timeouts, and dependency versions/configuration.
- Local emulators or AWS; for AWS, region, DynamoDB capacity mode and throughput guards.
- Exact command, stage duration, offered rates, seed size, cache warm-up procedure, and initial queue backlog.

The existing open-loop generator defaults to 95% redirects and 5% creates, with a Zipf-distributed hot set and 2% unknown codes among redirects. The following is an example for a future explicitly opted-in run against an already-running stack, not a recorded execution:

```sh
go run ./cmd/loadgen -base http://localhost:8080 \
  -rates 200,500,1000,2000 -stage 30s \
  -json /tmp/shortener-load-results.json
```

Retain both console output and JSON with the run record. Move evidence out of temporary storage when retaining final results. Runs create links and analytics in the target environment. Thirty-second stages are a starting configuration; extend them until resource and queue trends can be assessed.

## Measurements

Setup: Docker Desktop VM, 12 CPUs / 7.7 GiB, generator on the same host, DynamoDB Local, LocalStack SQS, one Redis, `MAX_REQUESTS=256`, `SQS_ENQUEUE_TIMEOUT=50ms`, 1 worker. Command: `go run ./cmd/loadgen -rates ... -stage 20s` (500 seeded links, 95% redirects / 5% creates, 2% unknown codes). HAProxy's per-source-IP limit (about 1,000 rps) was removed through an uncommitted Compose override for these runs. Raw console output was not saved to a file; the tables are copied from it. Generator-skipped was 0 in every stage.

**Baseline: 1 API, 1 worker**

| Offered RPS | Achieved RPS | Redirect p50/p95/p99 | Create p95 | OK % | 429 % | Error % |
|---:|---:|---|---:|---:|---:|---:|
| 1000 | 1000 | 9.3 / 51.8 / 53.1 ms | 40 ms | 100 | 0 | 0 |
| 2000 | 1994 | 52.1 / 53.2 / 55.5 ms | 72 ms | 100 | 0 | 0 |
| 3000 | 2973 | 52.0 / 53.2 / 54.6 ms | 452 ms | 100 | 0 | 0 |
| 4500 | 4446 | 51.8 / 53.5 / 110.8 ms | 962 ms | 69.2 | 30.8 | 0 |
| 6000 | 5719 | 4.3 / 52.8 / 53.8 ms | 1.11 s | 49.8 | 50.2 | 0 |

**Scaled: 3 API replicas, 1 worker**

| Offered RPS | Achieved RPS | Redirect p50/p95/p99 | Create p95 | OK % | 429 % | Error % |
|---:|---:|---|---:|---:|---:|---:|
| 3000 | 2992 | 52.2 / 53.3 / 56.4 ms | 383 ms | 100 | 0 | 0 |
| 4500 | 4372 | 52.1 / 53.4 / 59.4 ms | 1.16 s | 99.9 | 0 | 0.07 |
| 6000 | 5624 | 52.0 / 54.1 / 78.2 ms | 2.00 s | 80.6 | 16.4 | 3.0 |
| 9000 | 8436 | 3.1 / 55.3 / 73.0 ms | 2.00 s | 44.2 | 38.7 | 17.1 |

An earlier baseline through the committed HAProxy config returned 429 from 1,000 rps and 100% 429 at 4,000 rps. The API's own rejected counter was only 1,378 of about 120,000, so that limiter (10,000 requests per 10 s per IP, sticky because denied requests still count) was measuring the generator's single source IP, not the application.

## Saturation, failure analysis, and scaling comparison

| Finding | Result / supporting evidence |
|---|---|
| Highest sustainable load | About 3,000 rps for 1 API (0% 429/errors, but create p95 already 452 ms and redirect p50 52 ms, above the 50 ms p95 target). About 4,500 rps for 3 APIs (99.9% OK, create p95 above the 250 ms target). The design's redirect p95 < 50 ms target is not met at any load, see below. |
| First saturated component | The SQS send on the redirect path. Redirect p50 is a flat ~52 ms from 2,000 rps up, which matches the 50 ms `SQS_ENQUEUE_TIMEOUT` plus overhead, and `analytics_events_dropped_total` reached 220k after the baseline run, so LocalStack send calls time out. Each redirect holds one of 256 admission slots for that wait, so one replica tops out near 256 / 0.052 s ≈ 4,900 rps, which is where 429s begin. Container CPU was moderate afterwards (LocalStack 38%, DynamoDB Local 50%, worker 24%); per-stage CPU during the run was not captured, so the API-CPU ruling-out is not proven. |
| Second bottleneck: creates | Create p95 climbs with load (40 ms to 1.1 s at baseline, 2 s timeouts when scaled). Creates use DynamoDB Local conditional writes; this is the source of the scaled run's 3-17% errors. |
| Overload behavior | Admission control sheds with 429 fast (429 p50 about 1-4 ms) instead of queueing; achieved rate keeps rising with offered rate. No crashes observed. Analytics are dropped rather than slowing redirects further. Backlog reached 21.5k messages (baseline) and 29.7k (scaled) by the end of the runs. |
| Recovery after reducing load | Not measured. |
| Scaling change | Adding API replicas (1 to 3) moved the 429 knee from between 3,000-4,500 to between 4,500-6,000 rps and peak achieved rps from about 5,700 to about 8,400 (offered 9,000), with 429 at 4,500 dropping from 30.8% to 0%. The gain is less than 3x because the shared LocalStack/DynamoDB Local emulators and the co-located generator are also saturating, and the change added errors at 6,000+ (creates timing out). |

## Remaining work and limits

Repeat runs, saved raw output, per-stage CPU capture, a worker-scaling comparison, and a recovery test remain outstanding. Local emulator results cannot establish AWS capacity, durability, or production SLO compliance. The mixed HTTP load test and deterministic admission stress test are separate checks and cannot replace this evaluation.

Source code delivery and Grafana screenshots/snapshots are excluded from this documentation task. Future engineering priorities should be revised after the measured bottleneck is known; current operational gaps are listed in the [README limitations](README.md#known-limitations).
