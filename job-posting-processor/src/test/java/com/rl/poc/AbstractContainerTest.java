package com.rl.poc;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
@Testcontainers
@Slf4j
public class AbstractContainerTest {
    public static final DockerComposeContainer<?> composeContainer;
    public static final String KAFKA_SERVICE_NAME = "broker";
    static final int KAFKA_PORT = 9092;
    public static final String TEST_CONFIG_FILE =
            Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource(
                    "test.properties")).getPath();

    public static TestUtils testUtils;

    public static String schemaRegistryUrl;
    public TopologyTestDriver testDriver;
    public static Properties envProps;
    public static Properties streamProps;

    @SneakyThrows
    @BeforeAll
    public static void init() {
        final ConfigurationUtils configUtils = ConfigurationUtils.builder().build();
        testUtils = TestUtils.builder().schemaRegistryUrl(schemaRegistryUrl).build();
        envProps = configUtils.loadEnvProperties(TEST_CONFIG_FILE);
        streamProps = configUtils.buildStreamsProperties(envProps);
   }

    @AfterEach
    public void close() {
        testDriver.close();
    }

    @AfterAll
    public void tearDown() {
        composeContainer.stop();
    }

    static {
        composeContainer =
                new DockerComposeContainer<>(new File("src/test/resources/docker-compose.yml"))
                        .withExposedService(KAFKA_SERVICE_NAME, KAFKA_PORT,
                                Wait.forListeningPorts().withStartupTimeout(Duration.ofMinutes(8)));
        composeContainer
                .start();
    }


}
