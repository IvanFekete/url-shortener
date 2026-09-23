# Instructions:
Build a small but realistic distributed system that demonstrates how you design, operate and observe a production environment. You can use any language, environment, tooling you prefer.
# Expectation:
You are expected to create a running system, you can utilize services, database, telemetry of your choosing.

At the end of the exercise please be able to 

Share your code (via zip file or github repo)
Share how you designed it
Share how you observe the system
Demonstrate scaling of the system and get saturation point of service
Describe what breaks under load
Describe what changes you made to improve it
Describe what would you do if you have more time 


Here is the description of the task below that has to be implemented:

# Distributed URL Shortener

Build a small but realistic distributed URL shortening service that could run in a production environment.

The system should allow users to create short URLs and redirect users from a short URL to the original destination. It should also collect basic analytics for each shortened URL.

## Core Requirements

### 1. Create a short URL

Provide an API that accepts a long URL and returns a unique short URL.

Example:

```http
POST /shorten

{
  "url": "https://example.com/some/very/long/path"
}
```

Response:

```json
{
  "short_url": "http://localhost:8080/abc123",
  "code": "abc123"
}
```

The generated code must uniquely identify the original URL.

### 2. Redirect

When a user accesses the short URL, redirect them to the original URL.

```http
GET /abc123
```

The service should return an HTTP redirect to the original URL.

This endpoint is expected to receive significantly more traffic than the URL creation endpoint.

### 3. Analytics

Record basic information about each redirect, for example:

* timestamp
* short URL / code
* HTTP status
* optionally user agent or other lightweight metadata

Analytics should not unnecessarily slow down the redirect path. Consider processing analytics asynchronously.

Provide an endpoint such as:

```http
GET /stats/abc123
```

that returns basic statistics:

```json
{
  "url": "https://example.com/some/very/long/path",
  "clicks": 12345
}
```

## Distributed Systems Requirements

The service should be designed as a production-oriented distributed system rather than a single monolithic process.

Consider:

* horizontal scaling of API instances
* database scalability and connection management
* caching for frequently accessed URLs
* asynchronous processing for analytics
* failure handling and retries
* avoiding a single overloaded component
* consistency requirements
* backpressure under high load

You may use any technologies you prefer.

## Observability

The system should provide enough telemetry to understand how it behaves under load.

At minimum, expose metrics for:

* request rate
* request latency (p50/p95/p99)
* error rate
* database latency
* cache hit/miss rate
* analytics queue depth
* worker throughput
* CPU/memory usage where applicable

Use any monitoring/telemetry stack you prefer.

## Load Testing

Create a load test that exercises the system with realistic traffic.

The workload should contain significantly more redirects than URL creation requests.

For example:

```text
5%   POST /shorten
95%  GET /:code
```

Determine:

* maximum sustainable throughput
* latency at different load levels
* where the system reaches saturation
* which component becomes the bottleneck
* what happens when the system is overloaded

Document the results.

## Demonstrate Scaling

Start with a minimal deployment and gradually increase the load.

Then demonstrate at least one scaling improvement, for example:

* adding API replicas
* adding a cache
* adding analytics workers
* changing database configuration
* batching analytics writes

Compare the system before and after the change using your telemetry and load-test results.

## Deliverables

At the end of the exercise, provide:

1. **Source code**

   * GitHub repository

2. **Architecture**

   * diagram of the system
   * explanation of the major components
   * explanation of important design decisions

3. **Observability**

   * dashboards and/or metrics
   * explanation of what you monitor and why

4. **Load testing results**

   * throughput
   * latency
   * error rate
   * saturation point

5. **Failure analysis**

   * what breaks under load
   * which component becomes the bottleneck
   * how the system behaves when overloaded

6. **Improvements**

   * changes made during the exercise
   * measurable impact of those changes

7. **Future work**

   * what you would improve with more engineering time
   * additional reliability, scalability, or operational improvements you would consider


