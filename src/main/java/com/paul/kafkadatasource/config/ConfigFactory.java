package com.paul.kafkadatasource.config;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class ConfigFactory {

    private static final String DEFAULT_ACKS = "all";
    private static final Integer DEFAULT_RETRIES = 0;
    private static final Integer DEFAULT_BATCH_SIZE = 16384;
    private static final Long DEFAULT_LINGER_MS = 1L;
    private static final Long DEFAULT_BUFFER_MEMORY = 33554432L;
    private static final String DEFAULT_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String DEFAULT_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";


    public static KafkaConfigBuilder builder() {
        return new KafkaConfigBuilder();
    }

    public static class KafkaConfigBuilder {
        private String bootstrapServers;
        private String acks;
        private Integer retries;
        private Integer batchSize;
        private Long lingerMs;
        private Long bufferMemory;
        private String keySerializer;
        private String valueSerializer;


        public KafkaConfigBuilder setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public KafkaConfigBuilder setAcks(String acks) {
            this.acks = acks;
            return this;
        }

        public KafkaConfigBuilder setRetries(Integer retries) {
            this.retries = retries;
            return this;
        }

        public KafkaConfigBuilder setBatchSize(Integer batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public KafkaConfigBuilder setLingerMs(Long lingerMs) {
            this.lingerMs = lingerMs;
            return this;
        }

        public KafkaConfigBuilder setBufferMemory(Long bufferMemory) {
            this.bufferMemory = bufferMemory;
            return this;
        }

        public KafkaConfigBuilder setKeySerializer(String keySerializer) {
            this.keySerializer = keySerializer;
            return this;
        }

        public KafkaConfigBuilder setValueSerializer(String valueSerializer) {
            this.valueSerializer = valueSerializer;
            return this;
        }

        public KafkaProducer build() {
            if (null == this.bootstrapServers) throw new IllegalArgumentException("Property bootstrap.servers has not been set!");
            Properties properties = new Properties();
            properties.put("bootstrap.servers", this.bootstrapServers);
            properties.put("acks", null != this.acks ? this.acks : DEFAULT_ACKS);
            properties.put("retries", null != this.retries ? this.retries : DEFAULT_RETRIES);
            properties.put("batch.size", null != this.batchSize ? this.batchSize : DEFAULT_BATCH_SIZE);
            properties.put("linger.ms", null != this.lingerMs ? this.lingerMs : DEFAULT_LINGER_MS);
            properties.put("buffer.memory", null != this.bufferMemory ? this.bufferMemory : DEFAULT_BUFFER_MEMORY);
            properties.put("key.serializer", null != this.keySerializer ? this.keySerializer : DEFAULT_KEY_SERIALIZER);
            properties.put("value.serializer", null != this.valueSerializer ? this.valueSerializer : DEFAULT_VALUE_SERIALIZER);

            return new KafkaProducer(properties);
        }
    }
}
