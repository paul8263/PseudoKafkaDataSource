package com.paul.kafkadatasource;

import com.paul.kafkadatasource.config.KafkaMessageSenderConfig;
import com.paul.kafkadatasource.util.FileReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

public class KafkaMessageSender {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageSender.class);

    private List<String> messages;
    private KafkaProducer kafkaProducer;
    private long minInterval;
    private long jitter;
    private String topic;
    private boolean endlessMode;

    private ExecutorService executorService;

    public static KafkaMessageSender create(KafkaMessageSenderConfig config, KafkaProducer kafkaProducer) {
        KafkaMessageSender kafkaMessageSender = new KafkaMessageSender();
        kafkaMessageSender.messages = FileReader.readFile(config.getMessageFilePath());
        kafkaMessageSender.kafkaProducer = kafkaProducer;
        kafkaMessageSender.minInterval = config.getMinInterval();
        kafkaMessageSender.jitter = config.getJitter();
        kafkaMessageSender.topic = config.getTopic();
        kafkaMessageSender.endlessMode = config.isEndlessMode();
        return kafkaMessageSender;
    }

    public static KafkaMessageSenderConfig builder() {
        return new KafkaMessageSenderConfig();
    }

    private static class WorkerThread implements Callable<String> {
        private static final Logger LOGGER = LoggerFactory.getLogger(WorkerThread.class);

        private KafkaMessageSender kafkaMessageSender;

        WorkerThread(KafkaMessageSender kafkaMessageSender) {
            this.kafkaMessageSender = kafkaMessageSender;
        }

        @Override
        public String call() {
            execute();
            if (kafkaMessageSender.endlessMode) execute();
            return "Kafka Message sender execution finished";
        }

        private void execute() {
            try {
                for (String message : kafkaMessageSender.messages) {
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(kafkaMessageSender.topic, message);
                    Thread.sleep(kafkaMessageSender.minInterval + ThreadLocalRandom.current().nextLong(kafkaMessageSender.jitter));
                    kafkaMessageSender.kafkaProducer.send(producerRecord);
                }

                kafkaMessageSender.kafkaProducer.close();

            } catch (InterruptedException e) {
                e.printStackTrace();
                LOGGER.error("Worker thread was interrupted");
            } finally {
                kafkaMessageSender.kafkaProducer.close();
            }
        }

    }


    public void start() {
        if(null == executorService) executorService = Executors.newSingleThreadExecutor();
        CompletionService<String> completionService = new ExecutorCompletionService<>(executorService);

        WorkerThread workerThread = new WorkerThread(this);

        LOGGER.info("Kafka message sender started");

        completionService.submit(workerThread);
        try {
            completionService.take().get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        executorService.shutdown();
    }

    public void stopImmediately() {
        executorService.shutdownNow();
    }
}
