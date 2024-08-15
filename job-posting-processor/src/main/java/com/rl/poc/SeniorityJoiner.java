package com.rl.poc;

import com.rl.poc.models.JobPosting;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueJoiner;

@Builder
@Slf4j
public final class SeniorityJoiner implements ValueJoiner<JobPosting, JobPosting, JobPosting> {

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

