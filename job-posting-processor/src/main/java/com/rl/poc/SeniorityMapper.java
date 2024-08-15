package com.rl.poc;

import com.rl.poc.grpc.ApiService;
import com.rl.poc.models.JobPosting;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.net.URI;
import java.net.URISyntaxException;

@Builder
@Slf4j
public class SeniorityMapper implements ValueMapper<JobPosting, JobPosting> {
    ApiService seniorityService;

    public JobPosting apply(JobPosting posting) {

        SeniorityRequest request = SeniorityRequest.newBuilder()
                .setCompany(posting.getCompany())
                .setTitle(posting.getTitle())
                .setUuid(getUUID(posting.getUrl()))
                .build();

        SeniorityRequestBatch requestBatch = SeniorityRequestBatch.newBuilder()
                .addBatch(request)
                .build();

        SeniorityResponseBatch response = seniorityService.send(requestBatch);
        if (response != null) {
            int seniority = response.getBatchList().getFirst().getSeniority();
            posting.setSeniority(seniority);
            log.info("received response of seniority {} from service", response.getBatchList().getFirst().getSeniority());
        }
        return posting;
    }

    public String getUUID(String url) {
        try {
            URI uri = new URI(url);
            String path = uri.getPath();
            String[] segments = path.split("/");
            return segments[segments.length - 1];
        } catch (URISyntaxException e) {
            log.error("could not get UUID from {}", url);
        }
        return null;
    }
}
