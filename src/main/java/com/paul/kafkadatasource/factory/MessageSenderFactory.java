package com.paul.kafkadatasource.factory;

import com.paul.kafkadatasource.configSource.Configuration;
import com.paul.kafkadatasource.main.MessageSender;
import com.paul.kafkadatasource.messageSource.StringMessageSource;

public class MessageSenderFactory {

    public static MessageSender getMessageSender(Configuration configuration) {
        StringMessageSource messageSource = MessageSourceFactory.getMessageSource(configuration);

        return new MessageSender(messageSource.getMessage(), configuration);
    }
}
