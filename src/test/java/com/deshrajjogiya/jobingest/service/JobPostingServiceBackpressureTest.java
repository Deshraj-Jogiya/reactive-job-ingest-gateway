package com.deshrajjogiya.jobingest.service;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real tests for the backpressure-buffer wrapper {@link JobPostingService}
 * applies to the live SSE feed -- isolated from Spring/R2DBC so they run as
 * plain, fast Reactor unit tests against the exact operator chain
 * production code uses via the package-private {@code applyBackpressureBuffer}.
 */
class JobPostingServiceBackpressureTest {

    @Test
    void passesEverythingThroughUnchangedWhenTheSubscriberKeepsUp() {
        List<Integer> dropped = new ArrayList<>();
        Flux<Integer> source = Flux.range(1, 10);

        Flux<Integer> buffered = JobPostingService.applyBackpressureBuffer(source, 3, dropped::add);

        StepVerifier.create(buffered)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();

        assertThat(dropped).isEmpty();
    }

    @Test
    void dropsTheOldestBacklogInsteadOfErroringWhenASubscriberFallsBehind() {
        // Flux.create's onNext calls run eagerly as the source is
        // subscribed to, regardless of downstream demand -- the standard
        // way to simulate a fast producer against a subscriber that
        // hasn't requested anything yet, which is exactly the scenario
        // onBackpressureBuffer(capacity, onOverflow, DROP_OLDEST) exists
        // for.
        List<Integer> dropped = new ArrayList<>();
        Flux<Integer> fastProducer = Flux.create(sink -> {
            for (int i = 1; i <= 10; i++) {
                sink.next(i);
            }
            sink.complete();
        });

        Flux<Integer> buffered = JobPostingService.applyBackpressureBuffer(fastProducer, 3, dropped::add);

        // Zero initial demand: everything the producer emits before the
        // first request lands in the bounded buffer, past which the
        // oldest values get dropped to make room for newer ones.
        StepVerifier.create(buffered, 0)
                .thenRequest(3)
                .expectNext(8, 9, 10)
                .verifyComplete();

        assertThat(dropped).containsExactly(1, 2, 3, 4, 5, 6, 7);
    }
}
