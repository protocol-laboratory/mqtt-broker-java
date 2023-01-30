package io.github.protocol.mqtt.broker.processor;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class KafkaProcessorConfig {

    private String bootstrapServers = "localhost:9092";

    private int producerNum = 1;

    public KafkaProcessorConfig() {
    }
}
