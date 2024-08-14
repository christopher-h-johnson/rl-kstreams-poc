package com.rl.poc;

import com.rl.poc.models.JobPosting;
import com.rl.poc.models.JobPostingAug;
import com.rl.poc.models.Seniority;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JobPostingProcessorIT extends AbstractContainerTest {
    TestInputTopic<String, String> jobPostingTopic;
    TestInputTopic<String, String> seniorityTopic;
    TestOutputTopic<String, String> jobPostingAugTopic;

    @BeforeEach
    public void setup() {
        final JobPostingProcessor jst = new JobPostingProcessor();
        final Topology topology = jst.buildTopology(envProps, restClient).build(streamProps);
        testDriver = new TopologyTestDriver(topology, streamProps);
        createTestTopics(envProps);
    }

    @Test
    public void testJoin() throws IOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        final JobPosting jobPosting = mapper.readValue(
                cl.getResourceAsStream("test-data/composite-account.json"), JobPosting.class);
        final Seniority seniority = mapper.readValue(
                cl.getResourceAsStream("test-data/composite-account.json"), Seniority.class);
        final JobPostingAug jobPostingAug = mapper.readValue(
                cl.getResourceAsStream("test-data/composite-account.json"), JobPostingAug.class);

        jobPostingTopic.pipeInput(s_org_ext_x.getROWID(), jobPosting);
        seniorityTopic.pipeInput(s_addr_per.getROWID(), seniority);

        final List<KeyValue<String, JobPostingAug>> accounts = jobPostingAug.readKeyValuesToList();

        final KeyValue<String, Account> account1Record = accounts.stream().filter(sd
                -> sd.key.equals(s_org_ext.getROWID())).toList().get(0);
        final KeyValue<String, Account> accountEvRecord = evAccounts.stream().filter(sd
                -> sd.key.equals(s_org_ext.getROWID())).toList().get(0);
        assertEquals(1, accounts.size());
        assertThat(account1Record)
                .usingRecursiveComparison()
                .ignoringFields("value.lastUpdated")
                .isEqualTo(testUtils.buildKeyValue(s_org_ext.getROWID(), composite_account1));
        assertThat(accountEvRecord)
                .usingRecursiveComparison()
                .ignoringFields("value.lastUpdated")
                .isEqualTo(testUtils.buildKeyValue(s_org_ext.getROWID(), composite_account1));
    }

    private void createTestTopics(Properties envProps) {
        final Serializer<String> stringSerializer = Serdes.String().serializer();
        final Deserializer<String> stringDeserializer = Serdes.String().deserializer();

        jobPostingTopic = testDriver.createInputTopic(
                envProps.getProperty(TopicConfig.JOB_POSTING_TOPIC_NAME_CONFIG),
                stringSerializer,
                stringSerializer);
        seniorityTopic = testDriver.createInputTopic(
                envProps.getProperty(TopicConfig.SENIORITY_TOPIC_NAME_CONFIG),
                stringSerializer,
                stringSerializer);
        jobPostingAugTopic = testDriver.createOutputTopic(
                envProps.getProperty(TopicConfig.JOB_POSTING_AUG_TOPIC_NAME_CONFIG),
                stringDeserializer,
                stringDeserializer);
    }
}
