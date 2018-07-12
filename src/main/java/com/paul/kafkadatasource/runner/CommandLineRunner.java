package com.paul.kafkadatasource.runner;

import com.paul.kafkadatasource.configSource.CommandLineConfigSource;
import com.paul.kafkadatasource.configSource.ConfigSource;
import com.paul.kafkadatasource.configSource.Configuration;
import com.paul.kafkadatasource.factory.MessageSenderFactory;
import com.paul.kafkadatasource.main.MessageSender;

public class CommandLineRunner {
    public static void main(String[] args) {
        ConfigSource configSource = new CommandLineConfigSource(args);
        Configuration configuration = configSource.buildConfiguration();
        if (null != configuration) {
            MessageSender messageSender = MessageSenderFactory.getMessageSender(configuration);
            messageSender.start();
        }
    }
}
