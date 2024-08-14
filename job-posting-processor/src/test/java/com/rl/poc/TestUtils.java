package com.rl.poc;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KeyValue;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterResponse;
import software.amazon.awssdk.services.ssm.model.Parameter;
import software.amazon.awssdk.services.ssm.model.SsmException;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

@Builder
@Slf4j
public class TestUtils {
    String schemaRegistryUrl;

    public <VT extends SpecificRecord> SpecificAvroSerializer<VT> createSerializer() {
        SpecificAvroSerializer<VT> serializer = new SpecificAvroSerializer<>();
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        serializer.configure(config, false);
        return serializer;
    }

    public <VT extends SpecificRecord> SpecificAvroDeserializer<VT> createDeserializer() {
        SpecificAvroDeserializer<VT> deserializer = new SpecificAvroDeserializer<>();
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        deserializer.configure(config, false);
        return deserializer;
    }

    public <VT> VT buildTestData(InputStream is, Schema schema) throws IOException {
        DecoderFactory decoderFactory = new DecoderFactory();
        Decoder decoder = decoderFactory.jsonDecoder(schema, is);
        DatumReader<VT> reader = new SpecificDatumReader<>(schema);
        return reader.read(null, decoder);
    }

    public KeyValue<String, ?> buildKeyValue(String key, Object value) {
        return new KeyValue<>(key, value);
    }

    public String getSsmParam(String paramName) {
        SsmClient secretsClient = SsmClient.create();
        String password = getValue(secretsClient, paramName);
        secretsClient.close();
        return password;
    }

    public String getValue(SsmClient ssmClient, String paramName) {
        String password;
        try {
            GetParameterResponse valueResponse = ssmClient.getParameter(r -> r.withDecryption(Boolean.TRUE).name(paramName));
            Parameter param = valueResponse.parameter();
            if (param != null) {
                password = param.value();
                return password;
            }
        } catch (SsmException e) {
            log.error("could not get SSM credential, please authenticate to AWS");
            System.exit(1);
        }
        return null;
    }
}
