# Prometheus helper

Quick guide to checking that metrics are reported on the local Compose stack.

## Links

- Prometheus UI: http://localhost:9090
- Targets (scrape health): http://localhost:9090/targets
- Alert rules: http://localhost:9090/rules and http://localhost:9090/alerts
- Grafana: see the URL in `README.md` (dashboard is provisioned)
- [PromQL basics](https://prometheus.io/docs/prometheus/latest/querying/basics/)
- [PromQL functions](https://prometheus.io/docs/prometheus/latest/querying/functions/) (`rate`, `histogram_quantile`)
- [Histograms and quantiles](https://prometheus.io/docs/practices/histograms/)

## Is anything reported?

1. `docker compose up -d`
2. Open the Targets page. `api` (one per replica), `worker` and `redis` should be **UP**.
3. Generate traffic. Labelled counters appear only after their first increment.
4. Run queries below in the Graph tab.

Raw output from an API replica (HAProxy blocks `/metrics`, so go through the network):

```sh
docker compose exec prometheus wget -qO- http://api:8080/metrics | grep -E '^(http_requests_total|analytics_)'
```

## Queries

Scrape health:

```promql
up
```

HTTP:

```promql
sum by (route, status) (http_requests_total)
sum(rate(http_requests_total[1m]))
sum(rate(http_requests_total{status=~"5.."}[1m]))
histogram_quantile(0.95, sum by (le, route) (rate(http_request_duration_seconds_bucket[1m])))
http_requests_in_flight
rate(http_requests_rejected_total[1m])
```

Dependencies and cache:

```promql
sum by (dependency, operation, class) (dependency_errors_total)
histogram_quantile(0.95, sum by (le, dependency, operation) (rate(dependency_duration_seconds_bucket[1m])))
sum by (result) (rate(cache_requests_total[1m]))
```

Analytics pipeline:

```promql
sum by (result) (rate(analytics_events_total[1m]))
analytics_events_dropped_total
analytics_queue_depth
analytics_dlq_depth
histogram_quantile(0.95, sum by (le) (rate(analytics_event_age_seconds_bucket[5m])))
```

## Troubleshooting

- **Empty result:** no traffic yet, or the range is too short. `rate(...[1m])` needs at least two scrapes (interval is 5s).
- **Target DOWN:** the Targets page shows the scrape error.
- **Only one replica's numbers:** wrap the query in `sum(...)`.
- **5xx vs. 429/503:** compare `http_requests_total` by `status` with `http_requests_rejected_total` to tell dependency failures from the app's own admission limit.

Scrape config: `deploy/prometheus.yml`. Alert rules: `deploy/alerts.yml`.
