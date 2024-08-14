package com.rl.poc.models;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class Seniority {
    String company;
    int seniority;
    String title;
    int uuid;
}
