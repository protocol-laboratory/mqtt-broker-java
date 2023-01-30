package io.github.protocol.mqtt.broker.processor;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class PulsarProcessorConfig {

    private String httpUrl = "http://localhost:8080";

    private String serviceUrl = "pulsar://localhost:6650";

    public PulsarProcessorConfig() {
    }
}
