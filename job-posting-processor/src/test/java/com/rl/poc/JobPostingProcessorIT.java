package com.rl.poc;

import com.rl.poc.grpc.ApiService;
import com.rl.poc.models.JobPosting;
import com.rl.poc.serdes.JsonDeserializer;
import com.rl.poc.serdes.JsonSerializer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.grpcmock.GrpcMock;
import org.grpcmock.junit5.GrpcMockExtension;
import org.instancio.Assign;
import org.instancio.Instancio;
import org.javatuples.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.*;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.grpcmock.GrpcMock.stubFor;
import static org.grpcmock.GrpcMock.unaryMethod;
import static org.instancio.Select.field;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(GrpcMockExtension.class)
public class JobPostingProcessorIT extends AbstractContainerTest {

    TestInputTopic<String, JobPosting> jobPostingTopic;
    TestInputTopic<String, JobPosting> seniorityTopic;
    TestOutputTopic<String, JobPosting> compositeJobPostingTopic;

    @BeforeEach
    public void setup() {
        final JobPostingProcessor jst = new JobPostingProcessor();
        final ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", GrpcMock.getGlobalPort())
                .usePlaintext()
                .build();
        final ApiService seniorityService = new ApiService(channel);
        final Topology topology = jst.buildTopology(envProps, seniorityService)
                .build(streamProps);
        testDriver = new TopologyTestDriver(topology, streamProps);
        createTestTopics(envProps);
    }

    List<Pair<String, Integer>> getSeniorityPairs() {
        return List.of(
                Pair.with("Accounting Intern", 1),
                Pair.with("Software Engineer Trainee",1),
                Pair.with("Paralegal",1),
                Pair.with("Account Receivable Bookkeeper",2),
                Pair.with("Junior Software QA Engineer",2),
                Pair.with("Legal Adviser",2),
                Pair.with("Senior Tax Accountant",3),
                Pair.with("Lead Electrical Engineer",3),
                Pair.with("Attorney",3),
                Pair.with("Account Manager",4),
                Pair.with("Superintendent Engineer",4),
                Pair.with("Lead Lawyer",4),
                Pair.with("Chief of Accountants",5),
                Pair.with("VP Network Engineering",5),
                Pair.with("Head of Legal",5),
                Pair.with("Managing Director, Treasury",6),
                Pair.with("Director of Engineering, Backend Systems",6),
                Pair.with("Attorney, Partner",6),
                Pair.with("CFO",7),
                Pair.with("COO",7),
                Pair.with("CEO",7));
    }

    List<TestRecord<String, JobPosting>> buildJobPostingRecords() {
        final List<JobPosting> jobPostings = new ArrayList<>();
        IntStream.range(0, 10).forEach(i -> {
            final JobPosting jobPosting = Instancio.of(JobPosting.class)
                    .generate(field("title"), gen -> gen.oneOf("Accounting Intern",
                            "Software Engineer Trainee","Paralegal","Account Receivable Bookkeeper",
                            "Junior Software QA Engineer","Legal Adviser","Senior Tax Accountant",
                            "Lead Electrical Engineer","Attorney","Account Manager","Superintendent Engineer",
                            "Lead Lawyer","Chief of Accountants","VP Network Engineering","Head of Legal",
                            "Managing Director, Treasury","Director of Engineering, Backend Systems",
                            "Attorney, Partner","CFO","COO","CEO"))
                    .generate(field("company"), gen -> gen.oneOf(
                            "Company A", "Company B", "Company C", "Company D"))
                    .set(field("seniority"), 0)
                    .create();
            jobPostings.add(jobPosting);
        });

        final List<TestRecord<String, JobPosting>> testRecords = new ArrayList<>();
        jobPostings.forEach(p -> {
            String hashKey = p.getCompany() + "_" + p.getTitle();
            TestRecord<String, JobPosting> jobPostingTestRecord = new TestRecord<>(hashKey, p);
            testRecords.add(jobPostingTestRecord);
         });
        return testRecords;
    }

    List<TestRecord<String, JobPosting>> buildSeniorityRecords(List<TestRecord<String, JobPosting>> jobs) {
        final List<TestRecord<String, JobPosting>> testRecords = new ArrayList<>();
        jobs.forEach(j -> {
            final List<Pair<String, Integer>> seniorityPairs = getSeniorityPairs();
            final String title = j.getValue().getTitle();
            final Integer seniorityForTitle = seniorityPairs.stream()
                    .filter(p -> title.equals(p.getValue0()))
                    .map(Pair::getValue1)
                    .findAny()
                    .orElse(0);
            final JobPosting seniority = Instancio.of(JobPosting.class)
                .assign(Assign.valueOf(JobPosting::getSeniority).set(seniorityForTitle))
                .create();
                TestRecord<String, JobPosting> seniorityTestRecord = new TestRecord<>(j.key(), seniority);
                testRecords.add(seniorityTestRecord);
        });
        return testRecords;
    }

    @Test
    public void testJoin() {
        final List<TestRecord<String, JobPosting>> jobPostingRecords = buildJobPostingRecords();
        final List<TestRecord<String, JobPosting>> seniorityRecords = buildSeniorityRecords(jobPostingRecords);
        seniorityTopic.pipeRecordList(seniorityRecords);
        jobPostingTopic.pipeRecordList(jobPostingRecords);

        final List<KeyValue<String, JobPosting>> compositeJobPostings = compositeJobPostingTopic.readKeyValuesToList();

        final KeyValue<String, JobPosting> compositeJobPostingRecord = compositeJobPostings.stream().toList().getFirst();
        assertThat(compositeJobPostingRecord.value.getSeniority()).isGreaterThan(0);
        assertEquals(10, compositeJobPostings.size());
    }

    @Test
    public void testApiBranch() {
        final String uuid = UUID.randomUUID().toString();
        final SeniorityResponse responseMessage = SeniorityResponse.newBuilder()
                .setSeniority(1)
                .setUuid(uuid)
                .build();
        final SeniorityResponseBatch expectedResponseBatch = SeniorityResponseBatch.newBuilder()
                .addBatch(responseMessage)
                .build();
        stubFor(unaryMethod(SeniorityServiceGrpc.getInferSeniorityMethod())
                .willReturn(expectedResponseBatch));

        final List<TestRecord<String, JobPosting>> jobPostingRecords = buildJobPostingRecords();
        // first list of records use the API
        jobPostingTopic.pipeRecordList(jobPostingRecords);
        // send list of records join with cached topic and are emitted into the composite output topic.
        // API is not called.
        jobPostingTopic.pipeRecordList(jobPostingRecords);
        final List<KeyValue<String, JobPosting>> compositeJobPostings = compositeJobPostingTopic.readKeyValuesToList();

        final KeyValue<String, JobPosting> compositeJobPostingRecord = compositeJobPostings.stream().toList().getFirst();
        // seniority is set to 1 in api service mock response
        assertEquals(1, compositeJobPostingRecord.value.getSeniority());
        assertEquals(20, compositeJobPostings.size());
    }

    private void createTestTopics(Properties envProps) {
        final Serializer<String> stringSerializer = Serdes.String().serializer();
        final Deserializer<String> stringDeserializer = Serdes.String().deserializer();
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("JsonClass", JobPosting.class);
        final Serializer<JobPosting> jobPostingSerializer = new JsonSerializer<>();
        final Deserializer<JobPosting> jobPostingDeserializer = new JsonDeserializer<>();
        jobPostingDeserializer.configure(serdeProps, false);

        jobPostingTopic = testDriver.createInputTopic(
                envProps.getProperty(TopicConfig.JOB_POSTING_TOPIC_NAME_CONFIG),
                stringSerializer,
                jobPostingSerializer);
        seniorityTopic = testDriver.createInputTopic(
                envProps.getProperty(TopicConfig.SENIORITY_TOPIC_NAME_CONFIG),
                stringSerializer,
                jobPostingSerializer);
        compositeJobPostingTopic = testDriver.createOutputTopic(
                envProps.getProperty(TopicConfig.COMPOSITE_JOB_POSTING_TOPIC_NAME_CONFIG),
                stringDeserializer,
                jobPostingDeserializer);
    }
}
