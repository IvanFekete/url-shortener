# AGENTS.md

Conventions for AI-assisted work in this repo. Read this before making any changes.
Fill in the "Project-specific" section during the first few minutes of any new task.

## Language & tooling
- Go (stdlib-first; avoid new dependencies unless clearly justified — ask before adding one)
- Formatting: `gofmt -l .` must be clean before reporting done
- Tests: testify (`require`/`assert`), table-driven where it fits naturally

## Testing discipline
- After every code change, run: `go test -race ./... -skip '^Test(Load|Stress)'`
- Never report a task as done with failing tests or a broken build
- If a test fails, fix it and re-run before replying — don't hand back a red build "for me to check"
- Load/stress/benchmark tests are opt-in only (e.g. `make load`, `make stress`) — never run them automatically as part of a normal edit loop

## Change scope
- Keep diffs minimal and scoped to the request — don't refactor unrelated code
- Don't add dependencies, files, or abstractions beyond what's asked
- No speculative features ("might be useful later") unless requested

## Code review requests
- Return at most 5 issues, ranked by severity (correctness / concurrency / failure-handling first, style last)
- One line per issue: what's wrong + concrete fix direction — no essays
- Don't fix anything during a review pass unless explicitly told to fix it

## When something is deferred, not done
- If a review finding or requirement isn't addressed, say so explicitly
- Add unaddressed items to a "Known limitations" section in the README — never stay silent about them
- Never imply something is handled when it isn't

## Communication
- Summarize what changed and why in plain language, not a raw diff dump
- Flag any assumption you made explicitly — don't bury it only in a code comment

---

## Project-specific (fill in per task)
- Package / module name: `url-shortener`; API and analytics worker binaries under `cmd/`.
- Core interfaces already decided: HTTP API for shorten/redirect/stats; DynamoDB repository with sharded analytics counters; Redis cache; SQS Standard analytics queue (LocalStack locally); metrics endpoint; configurable link expiry (30 days by default).
- Concurrency model (if chosen): Stateless API replicas; asynchronous analytics workers using SQS long polling and visibility timeouts; bounded request and worker concurrency.
- Explicitly out of scope for this session: Production capacity measurements; multi-region deployment; authentication/billing; custom aliases and caller-selected expiry.

- Current task: Refresh DESIGN.md implementation status and README measurement limitations using available evidence; source code delivery and Grafana screenshots/snapshots are excluded.
