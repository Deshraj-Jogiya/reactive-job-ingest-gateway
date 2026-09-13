package com.deshrajjogiya.jobingest.repository;

import com.deshrajjogiya.jobingest.model.JobPostingEvent;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

public interface JobPostingRepository extends ReactiveCrudRepository<JobPostingEvent, Long> {

    Flux<JobPostingEvent> findAllByOrderByReceivedAtDesc();
}
