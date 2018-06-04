package com.paul.kafkadatasource.runner;

import com.paul.kafkadatasource.KafkaMessageSender;
import com.paul.kafkadatasource.config.ConfigFactory;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Runner {
    public static final String FILE_OPT = "file";
    public static final String BOOTSTRAP_SERVER_OPT = "bootstrapServer";
    public static final String TOPIC_OPT = "topic";
    public static final String MIN_INTERVAL_OPT = "minInterval";
    public static final String JITTER_OPT = "jitter";

    private static final Logger LOGGER = LoggerFactory.getLogger(Runner.class);

    public static void main(String[] args) {
        Properties properties = parseCommandLine(args);
        if(null != properties) buildKafkaMessageSender(properties).start();
    }

    private static Properties parseCommandLine(String[] args) {
        Options options = new Options();
        options.addOption(FILE_OPT, true, "Path for the flows file");
        options.addOption(BOOTSTRAP_SERVER_OPT, true, "Bootstrap servers for kafka");
        options.addOption(TOPIC_OPT, true, "Kafka Topic");
        options.addOption(MIN_INTERVAL_OPT, true, "Min interval");
        options.addOption(JITTER_OPT, true, "Jitter");
        options.addOption("h", false, "Show help");
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine;

        String helpString = "[-file] [-bootstrapServer] [-topic] [-minInterval] [-jitter]";
        HelpFormatter helpFormatter = new HelpFormatter();

        try {
            commandLine = parser.parse(options, args);

            if (commandLine.hasOption("h")) {
                helpFormatter.printHelp(helpString, options);
                return null;
            }

            String fileOpt = commandLine.getOptionValue(FILE_OPT);
            String bootstrapServersOpt = commandLine.getOptionValue(BOOTSTRAP_SERVER_OPT);
            String topic = commandLine.getOptionValue(TOPIC_OPT);
            String minInterval = commandLine.getOptionValue(MIN_INTERVAL_OPT, "1000");
            String jitter = commandLine.getOptionValue(JITTER_OPT, "500");
            Properties properties = new Properties();
            properties.put(FILE_OPT, fileOpt);
            properties.put(BOOTSTRAP_SERVER_OPT, bootstrapServersOpt);
            properties.put(TOPIC_OPT, topic);
            properties.put(MIN_INTERVAL_OPT, minInterval);
            properties.put(JITTER_OPT, jitter);
            return properties;

        } catch (ParseException e) {
            helpFormatter.printHelp(helpString, options);
            LOGGER.error("Parsing command line error");
        }

        return null;
    }

    private static KafkaMessageSender buildKafkaMessageSender(Properties properties) {
        String path = properties.getProperty(FILE_OPT);
        KafkaProducer producer = ConfigFactory.builder()
                .setBootstrapServers(properties.getProperty(BOOTSTRAP_SERVER_OPT))
                .setAcks("all")
                .build();
        return KafkaMessageSender.builder()
                .setMessageFilePath(path).setTopic(properties.getProperty(TOPIC_OPT))
                .setMinInterval(Long.valueOf(properties.getProperty(MIN_INTERVAL_OPT)))
                .setJitter(Long.valueOf(properties.getProperty(JITTER_OPT)))
                .build(producer);
    }
}
