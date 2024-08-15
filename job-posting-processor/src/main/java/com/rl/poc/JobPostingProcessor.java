package com.rl.poc;

import com.rl.poc.grpc.ApiService;
import com.rl.poc.models.JobPosting;
import com.rl.poc.serdes.JsonDeserializer;
import com.rl.poc.serdes.JsonSerializer;
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

    public StreamsBuilder buildTopology(Properties envProps, ApiService seniorityService) {

        /*
            Use Jackson for JSON Serde.
         */
        final Serde<JobPosting> jobPostingSerde = createJsonSerde();

        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        final KStream<String, JobPosting> postingStream =
                streamsBuilder
                        .stream(envProps.getProperty(TopicConfig.JOB_POSTING_TOPIC_NAME_CONFIG),
                                Consumed.with(Serdes.String(), jobPostingSerde));

        final KStream<String, JobPosting> seniorityStream =
                streamsBuilder
                        .stream(envProps.getProperty(TopicConfig.SENIORITY_TOPIC_NAME_CONFIG),
                                Consumed.with(Serdes.String(), jobPostingSerde));

        /*
            This creates a materialized state as a table of API augmented job posts that represents a distributed
            and highly scalable cache.
         */
        final KTable<String, JobPosting> seniorityTable = seniorityStream
                .toTable();

        final ApiCacheJoiner apiCacheJoiner = ApiCacheJoiner.builder()
                .build();

        /*
            Key incoming job postings with composite key and optionally (left) join with cached augmented job posts
            (when available)
         */
        final KStream<String, JobPosting> compositeStream = postingStream
                .selectKey((k,v) -> v.getCompany() + "_" + v.getTitle())
                .leftJoin(seniorityTable, apiCacheJoiner);

        /*
            Branch logic to split source topic into augmented and non-augmented variants.
            Non-augmented records are routed into a branch topic that via a value mapper
            for each record will call the Seniority API.

            The augmented records are then produced into a distinct seniority topic that represents a materialized
            persistent history of responses from the API. As such, the seniority topic acts as a highly scalable
            partitioned cache that is available for incoming job postings via a selected composite key
            (company_name + job_title).
         */
        final Map<String, KStream<String, JobPosting>> branches = compositeStream
                        .split(Named.as("jobs-"))
                .branch((k,v) -> v.getSeniority() == 0,
                        Branched.withFunction(s -> s, "call-api"))
                .defaultBranch(Branched.as("default"));

        final SeniorityMapper seniorityMapper = SeniorityMapper.builder()
                .seniorityService(seniorityService)
                .build();

        branches.get("jobs-call-api")
                .mapValues(seniorityMapper)
                .to(envProps.getProperty(TopicConfig.SENIORITY_TOPIC_NAME_CONFIG),
                        Produced.with(Serdes.String(),
                                jobPostingSerde));

        /*
            Only emit non-null augmented records into output topic
         */
        branches.get("jobs-default")
                .merge(branches.get("jobs-call-api"))
                .filter((k,v) -> v.getSeniority() != 0)
                .to(envProps.getProperty(TopicConfig.COMPOSITE_JOB_POSTING_TOPIC_NAME_CONFIG),
                        Produced.with(Serdes.String(),
                                jobPostingSerde));

        return streamsBuilder;
    }

    <VT> Serde<VT> createJsonSerde() {
        final Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("JsonClass", JobPosting.class);
        final Serializer<VT> jobPostingSerializer = new JsonSerializer<>();
        jobPostingSerializer.configure(serdeProps, false);
        final Deserializer<VT> jobPostingDeserializer = new JsonDeserializer<>();
        jobPostingDeserializer.configure(serdeProps, false);
        return Serdes.serdeFrom(jobPostingSerializer, jobPostingDeserializer);
    }
}

