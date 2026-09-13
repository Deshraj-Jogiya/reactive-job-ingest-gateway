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
