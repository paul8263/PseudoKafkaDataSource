package com.paul.kafkadatasource.factory;

import com.paul.kafkadatasource.configSource.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class KafkaProducerFactory {
    public KafkaProducerFactory() {}

    public static KafkaProducer<String, String> getKafkaProducer(Configuration configuration) {
        return new KafkaProducer<>(convertConfigToProperties(configuration));
    }

    private static Properties convertConfigToProperties(Configuration configuration) {
        Properties props = new Properties();
        props.put("bootstrap.servers", configuration.getBootstrapServers());
        props.put("acks", configuration.getAcks());
        props.put("retries", configuration.getRetries());
        props.put("batch.size", configuration.getBatchSize());
        props.put("linger.ms", configuration.getLingerMs());
        props.put("buffer.memory", configuration.getBufferMemory());
        props.put("key.serializer", configuration.getKeySerializer());
        props.put("value.serializer", configuration.getValueSerializer());
        return props;
    }
}
