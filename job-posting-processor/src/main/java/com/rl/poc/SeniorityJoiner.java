package com.rl.poc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rl.poc.grpc.SeniorityService;
import com.rl.poc.models.JobPosting;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.net.URI;
import java.net.URISyntaxException;

@Builder
@Slf4j
public final class SeniorityJoiner implements ValueJoiner<JobPosting, JobPosting, JobPosting> {
    public ObjectMapper mapper;

    public JobPosting apply(JobPosting posting, JobPosting seniority) {
        if (seniority != null) {
            return JobPosting.builder()
                    .company(posting.getCompany())
                    .location(posting.getLocation())
                    .seniority(seniority.getSeniority())
                    .title(posting.getTitle())
                    .url(posting.getUrl())
                    .build();
        }
        return null;
    }

}

