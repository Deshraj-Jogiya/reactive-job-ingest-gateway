package com.deshrajjogiya.jobingest.service;

import com.deshrajjogiya.jobingest.model.JobPostingEvent;
import com.deshrajjogiya.jobingest.repository.JobPostingRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Instant;
import java.util.function.Consumer;

@Service
public class JobPostingService {

    private static final Logger log = LoggerFactory.getLogger(JobPostingService.class);

    // How many not-yet-delivered postings a single slow SSE subscriber is
    // allowed to fall behind by before this gateway starts dropping the
    // oldest ones rather than letting that one subscriber's backlog grow
    // without bound. Deliberately separate from the replay sink's own
    // 20-item history window below -- this bounds per-subscriber lag, not
    // how much history a brand-new subscriber gets replayed.
    private static final int STREAM_BUFFER_CAPACITY = 50;

    private final JobPostingRepository repository;

    // Replay the last N postings to a new subscriber, then keep pushing live
    // ones -- lets an SSE client that connects mid-stream still see recent
    // history instead of only events ingested after it joined.
    private final Sinks.Many<JobPostingEvent> liveFeed = Sinks.many().replay().limit(20);

    public JobPostingService(JobPostingRepository repository) {
        this.repository = repository;
    }

    public Mono<JobPostingEvent> ingest(JobPostingEvent posting) {
        // Set server-side, not left to the client's JSON body: Jackson
        // deserializes via the no-arg constructor + setters, so a request
        // that omits receivedAt (the normal case) would otherwise hit the
        // NOT NULL column with nothing set.
        posting.setReceivedAt(Instant.now());
        return repository.save(posting)
                .doOnNext(saved -> {
                    Sinks.EmitResult result = liveFeed.tryEmitNext(saved);
                    if (result.isFailure()) {
                        // A real, previously-silent gap: tryEmitNext's result was
                        // never checked, so a failed publish to the live feed
                        // (the posting is still safely persisted either way) would
                        // have disappeared with no trace at all.
                        log.warn("Failed to publish posting id={} to the live feed: {}", saved.getId(), result);
                    }
                });
    }

    public Flux<JobPostingEvent> recent() {
        return repository.findAllByOrderByReceivedAtDesc();
    }

    public Flux<JobPostingEvent> stream() {
        return applyBackpressureBuffer(
                liveFeed.asFlux(),
                STREAM_BUFFER_CAPACITY,
                dropped -> log.warn(
                        "SSE subscriber falling behind -- dropped posting id={} from its backlog",
                        dropped.getId()
                )
        );
    }

    /**
     * Real backpressure handling for a subscriber that can't keep up with
     * the live feed: rather than letting an unbounded per-subscriber queue
     * grow (a slow SSE client over a slow network is a completely normal
     * case for a public ingest gateway, not an edge case) or letting the
     * stream fail outright with an overflow error, the oldest not-yet-sent
     * postings are dropped to make room for newer ones once the backlog
     * hits {@code capacity}. Package-private and generic so it can be
     * exercised directly in a test without needing the full Spring context.
     */
    static <T> Flux<T> applyBackpressureBuffer(Flux<T> source, int capacity, Consumer<T> onOverflow) {
        return source.onBackpressureBuffer(capacity, onOverflow, BufferOverflowStrategy.DROP_OLDEST);
    }
}
