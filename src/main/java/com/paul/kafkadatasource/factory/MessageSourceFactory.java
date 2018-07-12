package com.paul.kafkadatasource.factory;

import com.paul.kafkadatasource.configSource.Configuration;
import com.paul.kafkadatasource.messageSource.FileStringMessageSource;
import com.paul.kafkadatasource.messageSource.StringMessageSource;

public class MessageSourceFactory {
    public static StringMessageSource getMessageSource(Configuration configuration) {
        return new FileStringMessageSource(configuration.getMessageFilePath());
    }
}
