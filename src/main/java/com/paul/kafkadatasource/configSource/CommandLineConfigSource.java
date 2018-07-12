package com.paul.kafkadatasource.configSource;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandLineConfigSource implements ConfigSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommandLineConfigSource.class);
    private static final String HELP = "help";
    private static final String CONFIG = "config";
    private static final String DATA_FILE = "dataFile";
    private static final String BOOTSTRAP_SERVERS = "bootstrapServers";
    private static final String MIN_INTERVAL = "minInterval";
    private static final String JITTER = "jitter";
    private static final String TOPIC = "topic";
    private static final String ENDLESS_MODE = "endlessMode";

    private String[] args;

    public CommandLineConfigSource(String[] args) {
        this.args = args;
    }

    @Override
    public Configuration buildConfiguration() {
        Options options = buildOptions();

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine commandLine = parser.parse(options, args);
            if (commandLine.hasOption(HELP)) {

                HelpFormatter helpFormatter = new HelpFormatter();
                helpFormatter.printHelp(buildHelpString(options), options);
                return null;
            } else if (commandLine.hasOption(CONFIG)) {

                String configFilePath = commandLine.getOptionValue(CONFIG);
                ConfigSource propertiesFileConfigSource = new PropertiesFileConfigSource(configFilePath);
                return propertiesFileConfigSource.buildConfiguration();
            } else {
                String dataFile = commandLine.getOptionValue(DATA_FILE, "");
                String bootstrapServers = commandLine.getOptionValue(BOOTSTRAP_SERVERS, "");
                long minInterval = Long.valueOf(commandLine.getOptionValue(MIN_INTERVAL, "500") );
                long jitter = Long.valueOf(commandLine.getOptionValue(JITTER, "0"));
                String topic = commandLine.getOptionValue(TOPIC, "");
                String endlessMode = commandLine.getOptionValue(ENDLESS_MODE, "");

                boolean endlessModeBoolean = "true".equalsIgnoreCase(endlessMode) || "1".equalsIgnoreCase(endlessMode);

                return Configuration.builder()
                        .messageFilePath(dataFile)
                        .bootstrapServers(bootstrapServers)
                        .minInterval(minInterval)
                        .jitter(jitter)
                        .topic(topic)
                        .endlessMode(endlessModeBoolean)
                        .build();

            }
        } catch (ParseException e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        }

        return null;
    }

    private Options buildOptions() {
        Options options = new Options();

        options.addOption(HELP, false, "Print help");
        options.addOption(CONFIG, true, "Path for Config file");
        options.addOption(DATA_FILE, true, "Path for data file");
        options.addOption(BOOTSTRAP_SERVERS, true, "Kafka bootstrap servers");
        options.addOption(MIN_INTERVAL, true, "Min interval between messages");
        options.addOption(JITTER, true, "Interval jitter value");
        options.addOption(TOPIC, true, "Kafka topic");
        options.addOption(ENDLESS_MODE, true, "Endless mode");

        return options;
    }

    private String buildHelpString(Options options) {
        StringBuilder stringBuilder = new StringBuilder(100);
        for (Option option : options.getOptions()) {
            stringBuilder.append("[-").append(option.getOpt()).append("] ");
        }
        return stringBuilder.toString();
    }
}
