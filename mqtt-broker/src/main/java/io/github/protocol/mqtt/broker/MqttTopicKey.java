package io.github.protocol.mqtt.broker;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@EqualsAndHashCode
@ToString
public class MqttTopicKey {

    private MqttSessionKey mqttSessionKey;

    private String topic;

    public MqttTopicKey() {
    }
}
