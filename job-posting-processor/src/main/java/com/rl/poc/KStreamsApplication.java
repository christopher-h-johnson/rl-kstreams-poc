package com.rl.poc;

import org.apache.kafka.streams.StreamsBuilder;

import java.util.Properties;

public interface KStreamsApplication {

    StreamsBuilder buildTopology(Properties envProps);

    void createTopics(Properties envProps, Properties adminConfig);

}
