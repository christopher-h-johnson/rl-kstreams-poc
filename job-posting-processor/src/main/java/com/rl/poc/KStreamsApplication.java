package com.rl.poc;

import com.rl.poc.grpc.ApiService;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Properties;

public interface KStreamsApplication {

    StreamsBuilder buildTopology(Properties envProps, ApiService apiService);

    void createTopics(Properties envProps, Properties adminConfig);

}
