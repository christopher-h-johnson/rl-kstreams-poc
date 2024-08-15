package com.rl.poc;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.rl.poc.grpc.SeniorityService;
import com.rl.poc.models.JobPosting;
import com.rl.poc.serdes.JsonDeserializer;
import com.rl.poc.serdes.JsonSerializer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.*;

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

    public StreamsBuilder buildTopology(Properties envProps) {
        final ManagedChannel channel = ManagedChannelBuilder
                .forAddress(envProps.getProperty("seniority.service.address"),
                        Integer.parseInt(envProps.getProperty("seniority.service.port")))
                .usePlaintext()
                .build();
        final SeniorityService seniorityService = new SeniorityService(channel);

        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("JsonClass", JobPosting.class);
        final Serializer<JobPosting> jobPostingSerializer = new JsonSerializer<>();
        jobPostingSerializer.configure(serdeProps, false);
        final Deserializer<JobPosting> jobPostingDeserializer = new JsonDeserializer<>();
        jobPostingDeserializer.configure(serdeProps, false);
        final Serde<JobPosting> jobPostingSerde = Serdes.serdeFrom(jobPostingSerializer,
                jobPostingDeserializer);

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KTable<String, JobPosting> postingStream =
                streamsBuilder
                        .table(envProps.getProperty(TopicConfig.JOB_POSTING_TOPIC_NAME_CONFIG),
                                Consumed.with(Serdes.String(), jobPostingSerde));

        final KStream<String, JobPosting> seniorityStream =
                streamsBuilder
                        .stream(envProps.getProperty(TopicConfig.SENIORITY_TOPIC_NAME_CONFIG),
                                Consumed.with(Serdes.String(), jobPostingSerde));


        final KTable<String, JobPosting> seniorityTable = seniorityStream
                .toTable();

        final SeniorityJoiner seniorityJoiner = SeniorityJoiner.builder()
                .build();

        final String NIL_GUID = UUID.randomUUID().toString();

        final KStream<String, JobPosting> compositeStream = postingStream
                .leftJoin(seniorityTable,
                        posting -> Optional.of(posting.getCompany() + "_" + posting.getTitle()).orElse(NIL_GUID),
                        seniorityJoiner)
                .toStream();

        final Map<String, KStream<String, JobPosting>> branches = compositeStream
                        .split(Named.as("jobs-"))
                .branch((k,v) -> v != null && v.getSeniority() == 0,
                        Branched.withFunction(s -> s, "no-seniority"))
                .defaultBranch(Branched.as("default"));

        final SeniorityMapper seniorityMapper = SeniorityMapper.builder()
                .seniorityService(seniorityService)
                        .build();

        branches.get("jobs-no-seniority")
                .mapValues(seniorityMapper)
                .to(envProps.getProperty(TopicConfig.SENIORITY_TOPIC_NAME_CONFIG),
                        Produced.with(Serdes.String(),
                                jobPostingSerde));

        branches.get("jobs-default")
                .to(envProps.getProperty(TopicConfig.COMPOSITE_JOB_POSTING_TOPIC_NAME_CONFIG),
                        Produced.with(Serdes.String(),
                                jobPostingSerde));

        return streamsBuilder;
    }
}

