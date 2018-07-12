package com.paul.kafkadatasource.configSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

public class PropertiesFileConfigSource implements ConfigSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesFileConfigSource.class);

    private String filePath;

    public PropertiesFileConfigSource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public Configuration buildConfiguration() {
        Properties properties = new Properties();
        try {
            FileReader fileReader = new FileReader(new File(filePath));
            properties.load(fileReader);
            String dataFile = properties.getProperty(PropertyFileKeyConstants.DATA_FILE, "");
            String bootstrapServer = properties.getProperty(PropertyFileKeyConstants.BOOTSTRAP_SERVERS, "");
            long minInterval = Long.valueOf(properties.getProperty(PropertyFileKeyConstants.MIN_INTERVAL, ""));
            long jitter = Long.valueOf(properties.getProperty(PropertyFileKeyConstants.JITTER, "500"));
            String topic = properties.getProperty(PropertyFileKeyConstants.TOPIC, "0");
            String endlessMode = properties.getProperty(PropertyFileKeyConstants.ENDLESS_MODE, "");
            boolean endlessModeBoolean = "true".equalsIgnoreCase(endlessMode) || "1".equals(endlessMode);

            return Configuration.builder()
                    .messageFilePath(dataFile)
                    .bootstrapServers(bootstrapServer)
                    .minInterval(minInterval)
                    .jitter(jitter)
                    .topic(topic)
                    .endlessMode(endlessModeBoolean)
                    .build();

        } catch (Exception e) {
            LOGGER.error(e.toString());
        }
        return null;
    }
}
