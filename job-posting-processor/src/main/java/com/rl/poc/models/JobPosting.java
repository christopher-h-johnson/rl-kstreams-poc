package com.rl.poc.models;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class JobPosting {
    String company;
    String location;
    int seniority;
    Long scraped_on;
    String title;
    String url;
    String uuid;
}




