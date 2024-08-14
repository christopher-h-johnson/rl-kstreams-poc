package com.rl.poc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

@Builder
@Slf4j
public class ConfigurationUtils {

    public Properties loadEnvProperties(String fileName) {
        Properties envProps = new Properties();
        try {
            FileInputStream input = new FileInputStream(fileName);
            envProps.load(input);
            input.close();
        } catch (Exception e) {
            log.error("could not load properties from filename {} with exception {}", fileName, e.getMessage());
        }
        return envProps;
    }

    public Properties buildStreamsProperties(Properties envProps) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        streamsConfiguration.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, IgnoreRecordTooLargeHandler.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, -1);
        streamsConfiguration.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        streamsConfiguration.put(StreamsConfig.consumerPrefix(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG), 600000);
        streamsConfiguration.put(StreamsConfig.consumerPrefix(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), 600000);
        streamsConfiguration.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 600000);
        streamsConfiguration.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), 600000);
        streamsConfiguration.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), 100);
        streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), 240000);
        streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
        streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "snappy");
        streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG), 600000);
        streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,
                envProps.getProperty(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS) == null);
        streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
        return streamsConfiguration;
    }

    public static class IgnoreRecordTooLargeHandler implements ProductionExceptionHandler {
        public void configure(Map<String, ?> config) {}

        public ProductionExceptionHandlerResponse handle(final ProducerRecord<byte[], byte[]> record,
                                                         final Exception exception) {
            if (exception instanceof RecordTooLargeException || exception instanceof TimeoutException) {
                return ProductionExceptionHandlerResponse.CONTINUE;
            } else {
                return ProductionExceptionHandlerResponse.FAIL;
            }
        }
    }

    public Properties setSslConfig(Properties envProps) {
        final Properties config = new Properties();
        final String keystorePassword = System.getenv("SSL_KEYSTORE_PASSWORD") != null
                ? System.getenv("SSL_KEYSTORE_PASSWORD") : envProps.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        final String keyPassword = System.getenv("SSL_KEY_PASSWORD") != null
                ? System.getenv("SSL_KEY_PASSWORD") : envProps.getProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
        final String keystoreLocation = System.getenv("SSL_KEYSTORE_LOCATION") != null
                ? System.getenv("SSL_KEYSTORE_LOCATION") : envProps.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        final String truststoreLocation = System.getenv("SSL_TRUSTSTORE_LOCATION") != null
                ? System.getenv("SSL_TRUSTSTORE_LOCATION") : envProps.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        final String truststorePassword = System.getenv("SSL_TRUSTSTORE_PASSWORD") != null
                ? System.getenv("SSL_TRUSTSTORE_PASSWORD") : envProps.getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation);
        config.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword);
        config.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
        config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
        config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
        config.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
        return config;
    }

    public RestHighLevelClient getSearchClient(Properties envProps) {
        final String ELASTICSEARCH_HOSTNAME = envProps.getProperty("elasticsearch.hostname");
        final String SEARCH_SECRET_NAME = envProps.getProperty("search.secret.name");
        final String searchUsername = getSecretValue(SEARCH_SECRET_NAME, "username");
        final String searchPassword = getSecretValue(SEARCH_SECRET_NAME, "password");

        final CredentialsProvider credentialsProvider =
                new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(searchUsername, searchPassword));

        return new RestHighLevelClient(RestClient.builder(
                        new HttpHost(ELASTICSEARCH_HOSTNAME, 443, "https"))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                        .setDefaultCredentialsProvider(credentialsProvider)));
    }

    public Properties setSaslConfig(Properties envProps) {
        final Properties saslConfig = new Properties();
        final String saslUsername = getSecretValue(envProps.getProperty("sasl.secret.name"), "username");
        final String saslPassword = getSecretValue(envProps.getProperty("sasl.secret.name"), "password");
        saslConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        saslConfig.put("sasl.mechanism", "SCRAM-SHA-512");
        final String truststoreLocation = System.getenv("SSL_TRUSTSTORE_LOCATION") != null
                ? System.getenv("SSL_TRUSTSTORE_LOCATION") : envProps.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        saslConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
        saslConfig.put("sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + saslUsername + "\" password=\"" + saslPassword + "\";");
        return saslConfig;
    }

    public String getSecretValue(String secretName, String key) {
        try (SecretsManagerClient secretsClient = SecretsManagerClient.create()) {
            final ObjectMapper objectMapper = new ObjectMapper();
            JsonNode secretsJson;
            try {
                final String secret = secretsClient.getSecretValue(r -> r.secretId(secretName))
                        .secretString();
                if (secret != null) {
                    try {
                        secretsJson = objectMapper.readTree(secret);
                        return secretsJson.get(key).textValue();
                    } catch (IOException e) {
                        log.error("Exception while retrieving secret values: " + e.getMessage());
                    }
                }
            } catch (SecretsManagerException e) {
                System.err.println(e.awsErrorDetails().errorMessage());
            }
        }
        return null;
    }
}
