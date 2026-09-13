package com.deshrajjogiya.jobingest;

import com.deshrajjogiya.jobingest.model.JobPostingEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient(timeout = "10000")
class JobPostingIntegrationTest {

    @Autowired
    private WebTestClient client;

    @Test
    void ingestingAPostingPersistsItAndReturns201() {
        JobPostingEvent posting = new JobPostingEvent("Acme Corp", "Data Engineer", "greenhouse");

        client.post().uri("/api/postings")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(posting)
                .exchange()
                .expectStatus().isCreated()
                .expectBody()
                .jsonPath("$.id").isNumber()
                .jsonPath("$.companyName").isEqualTo("Acme Corp")
                .jsonPath("$.jobTitle").isEqualTo("Data Engineer");
    }

    @Test
    void ingestingRejectsAMissingRequiredField() {
        JobPostingEvent posting = new JobPostingEvent("", "Data Engineer", "greenhouse");

        client.post().uri("/api/postings")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(posting)
                .exchange()
                .expectStatus().is4xxClientError();
    }

    @Test
    void recentListReflectsWhatWasReallyIngested() {
        JobPostingEvent posting = new JobPostingEvent("Streamline Labs", "Platform Engineer", "lever");

        client.post().uri("/api/postings")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(posting)
                .exchange()
                .expectStatus().isCreated();

        Flux<JobPostingEvent> recent = client.get().uri("/api/postings")
                .accept(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus().isOk()
                .returnResult(JobPostingEvent.class)
                .getResponseBody();

        StepVerifier.create(recent.filter(p -> p.getCompanyName().equals("Streamline Labs")))
                .expectNextMatches(p -> p.getJobTitle().equals("Platform Engineer"))
                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void streamPushesANewlyIngestedPostingToASubscriberInRealTime() {
        Flux<JobPostingEvent> stream = client.get().uri("/api/postings/stream")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .returnResult(new ParameterizedTypeReference<JobPostingEvent>() {})
                .getResponseBody();

        StepVerifier.FirstStep<JobPostingEvent> verifier = StepVerifier.create(
                stream.filter(p -> p.getCompanyName().equals("Live Streaming Co")).take(1)
        );

        JobPostingEvent posting = new JobPostingEvent("Live Streaming Co", "Backend Engineer", "workable");
        client.post().uri("/api/postings")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(posting)
                .exchange()
                .expectStatus().isCreated();

        verifier
                .expectNextMatches(p -> p.getJobTitle().equals("Backend Engineer"))
                .expectComplete()
                .verify(Duration.ofSeconds(10));
    }
}
