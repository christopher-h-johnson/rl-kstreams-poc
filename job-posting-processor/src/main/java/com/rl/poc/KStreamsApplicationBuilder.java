package com.rl.poc;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.opensearch.client.RestHighLevelClient;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

@Builder
@Slf4j
public class KStreamsApplicationBuilder {
    String[] args;
    KStreamsApplication processorClass;
    String configurationFile;

    @SneakyThrows
    public void run() {

        if (args.length < 1) {
            configurationFile =
                    Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource(
                            configurationFile)).getPath();
        } else {
            configurationFile = args[0];
        }

        final ConfigurationUtils configUtils = ConfigurationUtils.builder().build();
        final Properties envProps = configUtils.loadEnvProperties(configurationFile);

        final Properties streamProps = configUtils.buildStreamsProperties(envProps);
        final Properties saslConfig = configUtils.setSaslConfig(envProps);
        final Properties properties = Stream.of(streamProps, saslConfig).collect(Properties::new, Map::putAll, Map::putAll);

        RestHighLevelClient restHighLevelClient = null;

        if (envProps.containsKey("search.secret.name")) {
            restHighLevelClient = configUtils.getSearchClient(envProps);
        }

        processorClass.createTopics(envProps, saslConfig);
        final Topology topology = processorClass.buildTopology(envProps, restHighLevelClient).build(properties);

        final KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.setUncaughtExceptionHandler((exception) ->
                StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD);

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
                latch.countDown();
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }));

        if (Boolean.parseBoolean(envProps.getProperty("application.reset"))) {
            streams.cleanUp();
        }

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
