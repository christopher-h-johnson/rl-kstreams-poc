package com.rl.poc;

import com.rl.poc.grpc.SeniorityService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.grpcmock.GrpcMock;
import org.grpcmock.junit5.GrpcMockExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.grpcmock.GrpcMock.*;

@ExtendWith(GrpcMockExtension.class)
class SeniorityServiceTest {

    private ManagedChannel channel;
    private SeniorityService seniorityService;

    @BeforeEach
    void setup() {
        channel = ManagedChannelBuilder.forAddress("localhost", GrpcMock.getGlobalPort())
                .usePlaintext()
                .build();
        seniorityService = new SeniorityService(channel);
    }

    @AfterEach
    void cleanup() {
        Optional.ofNullable(channel).ifPresent(ManagedChannel::shutdownNow);
    }

    @Test
    void should_return_correct_downstream_message() {
        String uuid = UUID.randomUUID().toString();
        String company = "Big Data LLC";
        String title = "Accounting Intern";
        SeniorityResponse responseMessage = SeniorityResponse.newBuilder()
                .setSeniority(1)
                .setUuid(uuid)
                .build();

        SeniorityResponseBatch expectedResponseBatch = SeniorityResponseBatch.newBuilder()
                .addBatch(responseMessage)
                .build();

        SeniorityRequest expectedRequest = SeniorityRequest.newBuilder()
                .setCompany(company)
                .setTitle(title)
                .setUuid(uuid)
                .build();

        SeniorityRequestBatch requestBatch = SeniorityRequestBatch.newBuilder()
                .addBatch(expectedRequest)
                .build();

        stubFor(unaryMethod(SeniorityServiceGrpc.getInferSeniorityMethod())
                .willReturn(expectedResponseBatch));

        SeniorityResponseBatch response = seniorityService.send(requestBatch);

//        assertThat(response)
//                .usingRecursiveComparison()
//                .ignoringFields("value.lastUpdated")
//                .isEqualTo(expectedResponseBatch);
        verifyThat(calledMethod(SeniorityServiceGrpc.getInferSeniorityMethod())
                .withRequest(requestBatch));
    }
}
