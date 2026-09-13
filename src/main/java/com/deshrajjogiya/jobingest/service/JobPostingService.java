package com.deshrajjogiya.jobingest.service;

import com.deshrajjogiya.jobingest.model.JobPostingEvent;
import com.deshrajjogiya.jobingest.repository.JobPostingRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Instant;

@Service
public class JobPostingService {

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
                .doOnNext(saved -> liveFeed.tryEmitNext(saved));
    }

    public Flux<JobPostingEvent> recent() {
        return repository.findAllByOrderByReceivedAtDesc();
    }

    public Flux<JobPostingEvent> stream() {
        return liveFeed.asFlux();
    }
}
