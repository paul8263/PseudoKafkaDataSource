package com.paul.kafkadatasource.main;

import com.paul.kafkadatasource.configSource.Configuration;
import com.paul.kafkadatasource.factory.KafkaProducerFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

public class MessageSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageSender.class);

    private ExecutorService executorService;

    private List<String> messageList;

    private Configuration configuration;

    public MessageSender(List<String> messageList, Configuration configuration) {
        this.messageList = messageList;
        this.configuration = configuration;
    }

    public void start() {
        try {
            KafkaProducer<String, String> kafkaProducer = KafkaProducerFactory.getKafkaProducer(configuration);
            WorkerThread workerThread = new WorkerThread(configuration, kafkaProducer, messageList);
            if (null == executorService) executorService = Executors.newSingleThreadExecutor();

            CompletionService<String> completionService = new ExecutorCompletionService<>(executorService);

            completionService.submit(workerThread);

            completionService.take().get();
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        } finally {
            if (null != executorService) executorService.shutdown();
        }
    }

    public void stop() {
        if (null != executorService) executorService.shutdownNow();
    }

    static class WorkerThread implements Callable<String> {
        private static final Logger LOGGER = LoggerFactory.getLogger(WorkerThread.class);
        private Configuration configuration;

        private KafkaProducer<String, String> kafkaProducer;

        private List<String> messageList;

        WorkerThread(Configuration configuration, KafkaProducer<String, String> kafkaProducer, List<String> messageList) {
            this.configuration = configuration;
            this.kafkaProducer = kafkaProducer;
            this.messageList = messageList;
        }

        @Override
        public String call() {
            do {
                execute();
            } while (configuration.isEndlessMode());
            kafkaProducer.close();
            return "FINISHED";
        }

        private void execute() {
            for (String msg : messageList) {
                sleep();
                ProducerRecord<String, String> record = new ProducerRecord<>(configuration.getTopic(), msg);
                kafkaProducer.send(record);
            }
        }

        private void sleep() {
            long jitter = 0;
            if (configuration.getJitter() != 0) {
                jitter = ThreadLocalRandom.current().nextLong(configuration.getJitter());
            }
            try {
                Thread.sleep(configuration.getMinInterval() + jitter);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage());
            }

        }
    }

}
