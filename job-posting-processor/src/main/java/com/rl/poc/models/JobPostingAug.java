package com.rl.poc.models;

import lombok.Builder;
import lombok.Getter;

@Getter
public class JobPostingAug extends JobPosting {
    int seniority;

    @Builder(builderMethodName = "seniorityBuilder")
    public JobPostingAug(String company, String location, Long scraped_on, String title, String url, int seniority) {
        super(company, location, scraped_on, title, url);
        this.seniority = seniority;
    }
}
