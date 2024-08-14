package com.rl.poc;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.opensearch.client.RestHighLevelClient;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
@Testcontainers
@Slf4j
public class AbstractContainerTest {
    public static final DockerComposeContainer<?> composeContainer;
    public static final int SCHEMA_REGISTRY_PORT = 8081;
    public static final String SCHEMA_REGISTRY_SERVICE_NAME = "schema-registry";
    public static final String KAFKA_SERVICE_NAME = "broker";
    public static final String TEST_CONFIG_FILE =
            Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource(
                    "test.properties")).getPath();
    static final int KAFKA_PORT = 9092;
    static File file;
    public static TestUtils testUtils;

    public ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, false);
    public static String schemaRegistryUrl;
    public TopologyTestDriver testDriver;
    public static Properties envProps;
    public static Properties streamProps;
    public static RestHighLevelClient restClient;

    @SneakyThrows
    @BeforeAll
    public static void init() {
        schemaRegistryUrl = "http://" + composeContainer.getServiceHost(SCHEMA_REGISTRY_SERVICE_NAME, SCHEMA_REGISTRY_PORT)
                + ":" + composeContainer.getServicePort(SCHEMA_REGISTRY_SERVICE_NAME, SCHEMA_REGISTRY_PORT);
        final ConfigurationUtils configUtils = ConfigurationUtils.builder().build();
        testUtils = TestUtils.builder().schemaRegistryUrl(schemaRegistryUrl).build();
        envProps = configUtils.loadEnvProperties(TEST_CONFIG_FILE);
        streamProps = configUtils.buildStreamsProperties(envProps);

        if (envProps.containsKey("search.environment")) {
            final String environment = envProps.getProperty("search.environment");
            final String password = testUtils.getSsmParam("/wmd/kafka_connect_" + environment + "/search.password");
            envProps.setProperty("search.password", password);
            restClient = configUtils.getSearchClient(envProps);
        }
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
        InputStream COMPOSE_FILE = Objects.requireNonNull(AbstractContainerTest.class.getClassLoader().getResourceAsStream(
                "docker-compose.yml"));

        try {
            file = File.createTempFile("docker-compose", ".tmp");
            FileUtils.copyInputStreamToFile(COMPOSE_FILE, file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        composeContainer =
                new DockerComposeContainer<>(file)
                        .withExposedService(SCHEMA_REGISTRY_SERVICE_NAME, SCHEMA_REGISTRY_PORT)
                        .withExposedService(KAFKA_SERVICE_NAME, KAFKA_PORT)
                        .waitingFor(SCHEMA_REGISTRY_SERVICE_NAME,
                                Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(4)))
                        .waitingFor(KAFKA_SERVICE_NAME,
                                Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(4)));
        composeContainer.start();
    }


}
