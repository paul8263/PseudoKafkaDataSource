package com.paul.kafkadatasource.messageSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class FileStringMessageSource implements StringMessageSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileStringMessageSource.class);
    private String path;

    public FileStringMessageSource(String path) {
        this.path = path;
    }

    @Override
    public List<String> getMessage() {
        List<String> result = null;
        try {
            Path filePath = Paths.get(path);
            result = Files.readAllLines(filePath);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Cannot read message file with path: " + path);
        }

        return result;
    }
}
