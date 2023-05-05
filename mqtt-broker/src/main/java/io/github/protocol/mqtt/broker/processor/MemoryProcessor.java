package io.github.protocol.mqtt.broker.processor;

import io.github.protocol.mqtt.broker.auth.MqttAuth;

public class MemoryProcessor extends AbstractProcessor {
    public MemoryProcessor(MqttAuth mqttAuth, MemoryProcessorConfig memoryProcessorConfig) {
        super(mqttAuth);
    }
}
