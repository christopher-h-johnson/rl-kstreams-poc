package com.rl.poc;

import com.rl.poc.models.JobPosting;
import com.rl.poc.models.Seniority;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.opensearch.client.RestHighLevelClient;

import java.util.List;
import java.util.Properties;

@Slf4j
public final class JobPostingProcessor implements KStreamsApplication {

    public static void main(final String[] args) {

        final JobPostingProcessor ts = new JobPostingProcessor();

        KStreamsApplicationBuilder.builder()
                .args(args)
                .configurationFile("configuration/dev.properties")
                .processorClass(ts)
                .build()
                .run();
    }

    public void createTopics(Properties envProps, Properties adminConfig) {
        final TopicConfig tc = new TopicConfig();
        final List<String> topicNames = tc.getAllTopicNameConfigValues();

        TopicBuilder.builder()
                .adminConfig(adminConfig)
                .envProps(envProps)
                .topicNames(topicNames)
                .build()
                .create();
    }

    public StreamsBuilder buildTopology(Properties envProps, RestHighLevelClient restClient) {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        final KStream<String, JobPosting> postingStream =
                streamsBuilder
                        .stream(envProps.getProperty(TopicConfig.JOB_POSTING_TOPIC_NAME_CONFIG));

        final KStream<String, Seniority> seniorityStream =
                streamsBuilder
                        .stream(envProps.getProperty(TopicConfig.SENIORITY_TOPIC_NAME_CONFIG));


        return streamsBuilder;
    }
}

