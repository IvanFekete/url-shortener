# Load and scaling results

Status: **Pending measurements**. No saved load/scaling results were found in the project. This document establishes where to record them; no load or stress tests were run for this documentation update.

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

Add one row per measured load stage. Pending entries are not estimates.

| Run / configuration | Offered RPS | Achieved RPS | Redirect p50/p95/p99 | Create p95 | 429 % | Error % | Generator skipped |
|---|---:|---:|---|---|---:|---:|---:|
| Baseline | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Baseline overload | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Scaled | Pending | Pending | Pending | Pending | Pending | Pending | Pending |

Retain status counts and document treatment of expected unknown-code responses. Report generator skips separately: the current JSON `ErrorPct` excludes skipped requests, while the console summary's error percentage includes them. Achieved request throughput alone is not proof of successful service throughput or sustainable capacity.

For the same time windows, retain cache hit ratio, dependency latency/errors, DynamoDB consumed capacity/throttles, queue depth, worker throughput, analytics drops, received-event age, and API/worker/generator CPU and memory. Use [the Prometheus guide](prometheus.md) for local queries. Queue-wide oldest-message age requires AWS CloudWatch; local received-event age does not describe a queue with stopped workers.

## Saturation, failure analysis, and scaling comparison

| Finding | Result / supporting evidence |
|---|---|
| Highest sustainable load meeting the design targets with stable queue/resource trends | Pending |
| First saturated component and correlated metrics | Pending |
| Overload behavior: latency, errors, rejections, analytics drops/backlog | Pending |
| Recovery after reducing load | Pending |
| Isolated scaling change and before/after impact | Pending |

Compare a one-API/one-worker baseline with an isolated change, such as adding API replicas or workers. Keep workload, host resources, warm-up, stage lengths, and generator capacity comparable, and record any differences. Follow the [evaluation plan](DESIGN.md#10-load-test-and-scaling-plan); do not infer a bottleneck from request latency alone.

## Remaining work and limits

Baseline measurements, saturation evidence, failure analysis under load, and a measurable scaling comparison remain outstanding. Local emulator results cannot establish AWS capacity, durability, or production SLO compliance. The mixed HTTP load test and deterministic admission stress test are separate checks and cannot replace this evaluation.

Source code delivery and Grafana screenshots/snapshots are excluded from this documentation task. Future engineering priorities should be revised after the measured bottleneck is known; current operational gaps are listed in the [README limitations](README.md#known-limitations).
