package com.rl.poc;

import com.rl.poc.models.JobPosting;
import com.rl.poc.models.JobPostingAug;
import com.rl.poc.models.Seniority;
import lombok.Builder;
import org.apache.kafka.streams.kstream.ValueJoiner;

@Builder
public final class SeniorityJoiner implements ValueJoiner<JobPosting, Seniority, JobPostingAug> {
    public JobPostingAug apply(JobPosting posting, Seniority seniority) {

        return JobPostingAug.seniorityBuilder()
                .company(posting.getCompany())
                .location(posting.getLocation())
                .seniority(seniority.getSeniority())
                .title(posting.getTitle())
                .url(posting.getUrl())
                .build();
    }
}

