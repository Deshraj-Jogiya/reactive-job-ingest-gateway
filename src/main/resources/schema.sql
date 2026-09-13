CREATE TABLE IF NOT EXISTS job_posting_event (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    job_title VARCHAR(255) NOT NULL,
    source VARCHAR(100) NOT NULL,
    received_at TIMESTAMP NOT NULL
);
