package com.deshrajjogiya.jobingest.service;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

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
        // Real bug found via this repo's own CI on the first attempt at
        // this test: Flux.create's default FluxSink buffers internally
        // and only actually delivers according to real downstream
        // demand, so it never forced an overflow at all -- every value
        // arrived in order, nothing was ever dropped. TestPublisher's
        // REQUEST_OVERFLOW violation is the real, documented Reactor
        // testing tool for this exact case: it deliberately emits more
        // items than have been requested, the same way Sinks.tryEmitNext
        // (what production code actually uses) pushes regardless of
        // subscriber demand.
        List<Integer> dropped = new ArrayList<>();
        TestPublisher<Integer> source = TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

        Flux<Integer> buffered = JobPostingService.applyBackpressureBuffer(source.flux(), 3, dropped::add);

        // Zero initial demand: everything the producer emits before the
        // first request lands in the bounded buffer, past which the
        // oldest values get dropped to make room for newer ones.
        StepVerifier.create(buffered, 0)
                .then(() -> source.next(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
                .then(source::complete)
                .thenRequest(3)
                .expectNext(8, 9, 10)
                .verifyComplete();

        assertThat(dropped).containsExactly(1, 2, 3, 4, 5, 6, 7);
    }
}
