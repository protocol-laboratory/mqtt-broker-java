package io.github.protocol.mqtt.broker;

import io.github.protocol.mqtt.broker.auth.MqttAuth;
import io.github.protocol.mqtt.broker.auth.MqttAuthTrustAll;
import io.github.protocol.mqtt.broker.processor.KafkaProcessorConfig;
import io.github.protocol.mqtt.broker.processor.MemoryProcessorConfig;
import io.github.protocol.mqtt.broker.processor.PulsarProcessorConfig;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class MqttServerConfig {

    private int port = 1883;

    private MqttAuth mqttAuth = new MqttAuthTrustAll();

    private ProcessorType processorType;

    private KafkaProcessorConfig kafkaProcessorConfig;

    private MemoryProcessorConfig memoryProcessorConfig;

    private PulsarProcessorConfig pulsarProcessorConfig;

    public MqttServerConfig() {
    }
}
