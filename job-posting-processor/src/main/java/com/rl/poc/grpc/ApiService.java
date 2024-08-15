package com.rl.poc.grpc;

import com.rl.poc.SeniorityRequestBatch;
import com.rl.poc.SeniorityResponseBatch;
import com.rl.poc.SeniorityServiceGrpc;
import io.grpc.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ApiService {

    private final SeniorityServiceGrpc.SeniorityServiceBlockingStub blockingStub;

    public ApiService(Channel channel) {
        blockingStub = SeniorityServiceGrpc.newBlockingStub(channel);
    }

    public SeniorityResponseBatch send(SeniorityRequestBatch requestBatch) {
        log.info("Sending batch inference with {} request", requestBatch.getBatchCount());
        SeniorityResponseBatch responseBatch = null;
        try {
            responseBatch = blockingStub.inferSeniority(requestBatch);
        } catch (StatusRuntimeException e) {
            log.warn("RPC failed: {}", e.getStatus());
        }
        return responseBatch;
    }
}