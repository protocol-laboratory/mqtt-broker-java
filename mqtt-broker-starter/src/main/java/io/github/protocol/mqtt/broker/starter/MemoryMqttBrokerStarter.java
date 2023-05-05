package io.github.protocol.mqtt.broker.starter;

import io.github.protocol.mqtt.broker.MqttServer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MemoryMqttBrokerStarter {
    public static void main(String[] args) throws Exception {
        log.info("begin to start memory mqtt broker");
        new MqttServer().start();
    }
}
