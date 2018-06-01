package com.paul;

import com.paul.kafkadatasource.KafkaMessageSender;
import com.paul.kafkadatasource.config.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Test;

public class KafkaDataSourceTest {
    @Test
    public void test() {
        String path = "C:\\Users\\Paul\\Desktop\\2016_07_14_20_10-15-15f1.flows";
        KafkaProducer producer = ConfigFactory.builder()
                .setBootstrapServers("192.168.100.128:9092")
                .setAcks("all")
                .build();
        KafkaMessageSender kafkaMessageSender = KafkaMessageSender.builder()
                .setMessageFilePath(path).setTopic("txdata")
                .setMinInterval(500)
                .setJitter(2000)
                .build(producer);

        kafkaMessageSender.start();

    }
}
