package com.paul.kafkadatasource.config;

import com.paul.kafkadatasource.KafkaMessageSender;
import org.apache.kafka.clients.producer.KafkaProducer;

public class KafkaMessageSenderConfig {
    private String messageFilePath;
    private long minInterval;
    private long jitter;
    private String topic;
    private boolean endlessMode;

    public KafkaMessageSenderConfig setMessageFilePath(String messageFilePath) {
        this.messageFilePath = messageFilePath;
        return this;
    }

    public KafkaMessageSenderConfig setMinInterval(long minInterval) {
        this.minInterval = minInterval;
        return this;
    }

    public KafkaMessageSenderConfig setJitter(long jitter) {
        this.jitter = jitter;
        return this;
    }

    public KafkaMessageSenderConfig setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public KafkaMessageSenderConfig setEndlessMode(boolean endlessMode) {
        this.endlessMode = endlessMode;
        return this;
    }

    public KafkaMessageSender build(KafkaProducer kafkaProducer) {
        return KafkaMessageSender.create(this, kafkaProducer);
    }

    public String getMessageFilePath() {
        return messageFilePath;
    }

    public long getMinInterval() {
        return minInterval;
    }

    public long getJitter() {
        return jitter;
    }

    public String getTopic() {
        return topic;
    }

    public boolean isEndlessMode() {
        return endlessMode;
    }
}
