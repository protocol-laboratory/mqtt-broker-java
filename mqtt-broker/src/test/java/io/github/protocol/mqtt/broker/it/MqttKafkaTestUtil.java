package io.github.protocol.mqtt.broker.it;

import io.github.embedded.kafka.core.EmbeddedKafkaServer;
import io.github.protocol.mqtt.broker.ProcessorType;
import io.github.protocol.mqtt.broker.MqttServer;
import io.github.protocol.mqtt.broker.MqttServerConfig;
import io.github.protocol.mqtt.broker.processor.KafkaProcessorConfig;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MqttKafkaTestUtil {

    public static MqttServer setupMqttKafka() throws Exception {
        EmbeddedKafkaServer embeddedKafkaServer = new EmbeddedKafkaServer();
        new Thread(() -> {
            try {
                embeddedKafkaServer.start();
            } catch (Exception e) {
                log.error("kafka broker started exception ", e);
            }
        }).start();
        Thread.sleep(5_000);
        MqttServerConfig mqttServerConfig = new MqttServerConfig();
        mqttServerConfig.setPort(0);
        mqttServerConfig.setProcessorType(ProcessorType.KAFKA);
        KafkaProcessorConfig kafkaProcessorConfig = new KafkaProcessorConfig();
        kafkaProcessorConfig.setBootstrapServers(String.format("localhost:%d", embeddedKafkaServer.getKafkaPort()));
        mqttServerConfig.setKafkaProcessorConfig(kafkaProcessorConfig);
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
