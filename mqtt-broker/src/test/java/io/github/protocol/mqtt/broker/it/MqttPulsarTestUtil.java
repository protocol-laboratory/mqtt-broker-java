package io.github.protocol.mqtt.broker.it;

import io.github.embedded.pulsar.core.EmbeddedPulsarServer;
import io.github.protocol.mqtt.broker.MqttServer;
import io.github.protocol.mqtt.broker.MqttServerConfig;
import io.github.protocol.mqtt.broker.ProcessorType;
import io.github.protocol.mqtt.broker.processor.PulsarProcessorConfig;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MqttPulsarTestUtil {

    public static MqttServer setupMqttPulsar() throws Exception {
        EmbeddedPulsarServer embeddedPulsarServer = new EmbeddedPulsarServer();
        embeddedPulsarServer.start();
        MqttServerConfig mqttServerConfig = new MqttServerConfig();
        mqttServerConfig.setPort(0);
        mqttServerConfig.setProcessorType(ProcessorType.PULSAR);
        PulsarProcessorConfig pulsarProcessorConfig = new PulsarProcessorConfig();
        pulsarProcessorConfig.setHttpUrl(String.format("http://localhost:%d", embeddedPulsarServer.getWebPort()));
        pulsarProcessorConfig.setServiceUrl(String.format("pulsar://localhost:%d", embeddedPulsarServer.getTcpPort()));
        mqttServerConfig.setPulsarProcessorConfig(pulsarProcessorConfig);
        MqttServer mqttServer = new MqttServer(mqttServerConfig);
        new Thread(() -> {
            try {
                mqttServer.start();
            } catch (Exception e) {
                log.error("mqsar broker started exception ", e);
            }
        }).start();
        Thread.sleep(5000L);
        return mqttServer;
    }
}
