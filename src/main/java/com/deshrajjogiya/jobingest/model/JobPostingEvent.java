package com.deshrajjogiya.jobingest.model;

import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import jakarta.validation.constraints.NotBlank;

@Table("job_posting_event")
public class JobPostingEvent {

    @Id
    private Long id;

    @NotBlank
    private String companyName;

    @NotBlank
    private String jobTitle;

    @NotBlank
    private String source;

    private Instant receivedAt;

    public JobPostingEvent() {
    }

    public JobPostingEvent(String companyName, String jobTitle, String source) {
        this.companyName = companyName;
        this.jobTitle = jobTitle;
        this.source = source;
        this.receivedAt = Instant.now();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getJobTitle() {
        return jobTitle;
    }

    public void setJobTitle(String jobTitle) {
        this.jobTitle = jobTitle;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(Instant receivedAt) {
        this.receivedAt = receivedAt;
    }
}
