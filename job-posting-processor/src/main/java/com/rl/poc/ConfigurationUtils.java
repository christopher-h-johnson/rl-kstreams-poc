package com.rl.poc;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.io.FileInputStream;
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
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
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
//        streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,
//                envProps.getProperty(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS) == null);
//        streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
//                envProps.getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
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

    public Properties setSaslConfig(Properties envProps) {
        return new Properties();
    }

}
