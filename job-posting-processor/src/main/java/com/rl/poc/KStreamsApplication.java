package com.rl.poc;

import org.apache.kafka.streams.StreamsBuilder;
import org.opensearch.client.RestHighLevelClient;

import java.util.Properties;

public interface KStreamsApplication {

    StreamsBuilder buildTopology(Properties envProps, RestHighLevelClient restHighLevelClient);

    void createTopics(Properties envProps, Properties adminConfig);

}
