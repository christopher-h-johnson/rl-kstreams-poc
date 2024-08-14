package com.rl.poc;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BaseTopicConfig {

    public static final String DEFAULT_TOPIC_PARTITIONS_CONFIG = "default.topic.partitions";
    public static final String DEFAULT_TOPIC_REPLICATION_FACTOR_CONFIG = "default.topic.replication.factor";

    public List<String> getAllTopicNameConfigValues() {
        List<Field> fields = Arrays.asList(this.getClass().getDeclaredFields());
        List<String> fieldValues = new ArrayList<>();
        fields
                .forEach(f -> {
                    try {
                        fieldValues.add((String) f.get(this));
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                });
        return fieldValues.stream().filter(fv -> fv.contains("name")).collect(Collectors.toList());
    }
}
