# Reactive Job Ingest Gateway

A small, focused Spring WebFlux service: a non-blocking gateway that ingests job-posting
events and republishes them as a live server-sent-events stream. Built as a standalone
learning/reference project to get real, running experience with Spring's reactive stack
(WebFlux, R2DBC, Project Reactor) alongside the Python-based systems that make up the rest
of this job-search tooling -- this one isn't wired into them, it's deliberately self-contained.

## Why reactive here

Ingest and fan-out is exactly the case reactive streams are for: many potential SSE
subscribers watching one shared feed, none of them blocking a thread while they wait, and a
producer (`POST /api/postings`) that shouldn't have to know or care how many consumers are
listening. `Sinks.Many` gives one hot, replayable stream that both a REST snapshot read and
any number of live SSE subscriptions can be built from without re-querying the database per
subscriber.

## Endpoints

- `POST /api/postings` -- ingest a posting (`companyName`, `jobTitle`, `source`), persists via
  R2DBC and pushes it onto the live feed. Returns 201 with the saved record, or 400 if a
  required field is blank.
- `GET /api/postings` -- the persisted postings, most recent first.
- `GET /api/postings/stream` -- a `text/event-stream` connection that replays the last 20
  postings, then stays open and pushes new ones as they're ingested.
- `GET /` -- a real, minimal live dashboard (`src/main/resources/static/index.html`, plain
  `EventSource` against `/api/postings/stream`, no build step) so there's an actual way to
  watch the feed besides `curl -N`.

## Backpressure handling

A slow SSE subscriber (a slow network, a client that isn't reading fast enough) is a normal
case for a public ingest gateway, not an edge case -- without a bound, that subscriber's
backlog of not-yet-sent postings would grow without limit. `JobPostingService.stream()` wraps
the live feed in `onBackpressureBuffer(50, onOverflow, BufferOverflowStrategy.DROP_OLDEST)`:
once a subscriber falls more than 50 postings behind, the oldest ones in its backlog are
dropped (and logged) to make room for newer ones, rather than the stream growing memory
unboundedly or failing outright with an overflow error. Also fixed while adding this: the
live feed's `tryEmitNext` result was previously never checked, so a failed publish (the
posting itself is still safely persisted either way) would have vanished with no trace --
now logged as a warning. `JobPostingServiceBackpressureTest` exercises the real operator
chain directly (no Spring context needed): one test confirms a keeping-up subscriber sees
every posting unchanged, the other confirms a subscriber that hasn't requested anything yet
gets the newest N postings with the rest dropped, rather than an overflow error.

## Running it

```bash
mvn spring-boot:run
```

Then, in another terminal:

```bash
curl -N http://localhost:8080/api/postings/stream &
curl -X POST http://localhost:8080/api/postings \
  -H "Content-Type: application/json" \
  -d '{"companyName":"Acme Corp","jobTitle":"Data Engineer","source":"greenhouse"}'
```

The posting should appear on the `stream` connection as soon as it's ingested.

Data lives in an in-memory H2 instance (via R2DBC) -- nothing to configure, nothing persists
across restarts.

## Testing

```bash
mvn test
```

Real integration tests (`WebTestClient` + `StepVerifier`, no mocks) covering: ingest +
persistence, validation rejection, the snapshot list reflecting what was actually ingested,
and a live SSE subscriber genuinely receiving a posting ingested after it connected.
