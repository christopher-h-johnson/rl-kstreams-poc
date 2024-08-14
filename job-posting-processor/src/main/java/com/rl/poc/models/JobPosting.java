package com.rl.poc.models;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class JobPosting {
    String company;
    String location;
    Long scraped_on;
    String title;
    String url;
}




