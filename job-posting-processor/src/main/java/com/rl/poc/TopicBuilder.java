package com.rl.poc;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.streams.StreamsConfig;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

@Builder
@Slf4j
public class TopicBuilder {
    Properties envProps;
    Properties adminConfig;
    List<String> topicNames;
    Predicate<NewTopic> configurationFilter;
    Map<String,String> topicConfig;

    public void create() {
        //get existing topic names
        adminConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        List<String> existingTopics = new ArrayList<>(topics(adminConfig));

        //get topic names from selected environment configuration props
        List<String> topicNamesToCreate = new ArrayList<>();
        topicNames.forEach(tn -> topicNamesToCreate.add(envProps.getProperty(tn)));

        //remove existing topic names from topics to create
        topicNamesToCreate.removeAll(existingTopics);

        //build List of NewTopics to create
        final List<NewTopic> topicsToCreate = new ArrayList<>();
        topicNamesToCreate.forEach(tn -> topicsToCreate.add(new NewTopic(
                tn,
                Integer.parseInt(envProps.getProperty(BaseTopicConfig.DEFAULT_TOPIC_PARTITIONS_CONFIG)),
                Short.parseShort(envProps.getProperty(BaseTopicConfig.DEFAULT_TOPIC_REPLICATION_FACTOR_CONFIG)))));

        // apply configuration filter
        if (topicConfig != null && configurationFilter != null) {
            List<NewTopic> filteredTopics = topicsToCreate.stream()
                    .filter(configurationFilter)
                    .toList();
            filteredTopics.forEach(t -> t.configs(topicConfig));
        } else if (topicConfig != null) {
            topicsToCreate.forEach(t -> t.configs(topicConfig));
        }

        // execute client createTopics
        try (AdminClient client = AdminClient.create(adminConfig)) {
            final CreateTopicsResult result = client.createTopics(topicsToCreate);
            result.all().get();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
    public Set<String> topics(Properties adminConfig) {
        try (AdminClient client = AdminClient.create(adminConfig)) {
            KafkaFuture<Set<String>> names = client.listTopics().names();
            KafkaFuture.allOf(names).get(20, TimeUnit.SECONDS);
            return names.get();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Exception occurred during topic retrieval.", e);
        }
    }
}
