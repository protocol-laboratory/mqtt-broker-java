package io.github.protocol.mqtt.broker.it;

import io.github.protocol.mqtt.broker.MqttServer;
import lombok.extern.log4j.Log4j2;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Log4j2
public class MqttKafkaPubSubTest {

    @Test
    public void pubSubTest() throws Exception {
        MqttServer mqttServer = MqttKafkaTestUtil.setupMqttKafka();
        String topic = UUID.randomUUID().toString();
        String content = "test-msg";
        String broker = String.format("tcp://localhost:%d", mqttServer.getPort());
        String clientId = UUID.randomUUID().toString();
        MemoryPersistence persistence = new MemoryPersistence();
        MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setUserName(UUID.randomUUID().toString());
        connOpts.setPassword(UUID.randomUUID().toString().toCharArray());
        connOpts.setCleanSession(true);
        log.info("Mqtt connecting to broker");
        sampleClient.connect(connOpts);
        CompletableFuture<String> future = new CompletableFuture<>();
        log.info("Mqtt subscribing");
        sampleClient.subscribe(topic, (s, mqttMessage) -> {
            log.info("messageArrived");
            future.complete(mqttMessage.toString());
        });
        log.info("Mqtt subscribed");
        MqttMessage message = new MqttMessage(content.getBytes());
        message.setQos(1);
        log.info("Mqtt message publishing");
        sampleClient.publish(topic, message);
        log.info("Mqtt message published");
        TimeUnit.SECONDS.sleep(3);
        sampleClient.disconnect();
        String msg = future.get(5, TimeUnit.SECONDS);
        Assertions.assertEquals(content, msg);
    }

}
