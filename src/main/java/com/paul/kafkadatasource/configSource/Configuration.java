package com.paul.kafkadatasource.configSource;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class Configuration {
    @Builder.Default
    private String bootstrapServers = "localhost:9092";
    @Builder.Default
    private long minInterval = 1000;
    @Builder.Default
    private long jitter = 0;
    @Builder.Default
    private String topic = "output";
    @Builder.Default
    private boolean endlessMode = false;
    @Builder.Default
    private String messageFilePath = "/path/to/messageFile";

    @Builder.Default
    private String acks = "all";
    @Builder.Default
    private int retries = 0;
    @Builder.Default
    private int batchSize = 16384;
    @Builder.Default
    private int lingerMs = 1;
    @Builder.Default
    private int bufferMemory = 33554432;
    @Builder.Default
    private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
    @Builder.Default
    private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";

}
