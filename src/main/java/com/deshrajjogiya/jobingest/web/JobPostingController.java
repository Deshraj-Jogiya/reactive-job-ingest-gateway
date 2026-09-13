package com.deshrajjogiya.jobingest.web;

import com.deshrajjogiya.jobingest.model.JobPostingEvent;
import com.deshrajjogiya.jobingest.service.JobPostingService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/postings")
public class JobPostingController {

    private final JobPostingService service;

    public JobPostingController(JobPostingService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<JobPostingEvent> ingest(@Valid @RequestBody JobPostingEvent posting) {
        return service.ingest(posting);
    }

    @GetMapping
    public Flux<JobPostingEvent> recent() {
        return service.recent();
    }

    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<JobPostingEvent> stream() {
        return service.stream();
    }
}
