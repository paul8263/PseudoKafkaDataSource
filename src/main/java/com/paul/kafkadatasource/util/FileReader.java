package com.paul.kafkadatasource.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class FileReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileReader.class);

    private FileReader() {}

    public static List<String> readFile(String path) {
        List<String> result = null;
        try {
            Path filePath = Paths.get(path);
            result = Files.readAllLines(filePath);
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("Read file error!");
        }

        return result;
    }
}
