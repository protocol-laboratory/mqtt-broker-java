package io.github.protocol.mqtt.broker.util;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;

public class MqttMessageUtil {

    public static MqttPubAckMessage pubAckMessage(int packetId) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK,
                false, MqttQoS.AT_MOST_ONCE, false, 0);
        return (MqttPubAckMessage) MqttMessageFactory.newMessage(mqttFixedHeader,
                MqttMessageIdVariableHeader.from(packetId), null);
    }

    public static MqttMessage pingResp() {
        MqttFixedHeader pingHeader = new MqttFixedHeader(MqttMessageType.PINGRESP, false,
                MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(pingHeader);
    }

    public static MqttPublishMessage publishMessage(MqttQoS qos, String topic, int packetId, byte[] value) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH,
                false, qos, false, 0);
        return (MqttPublishMessage) MqttMessageFactory.newMessage(mqttFixedHeader,
                new MqttPublishVariableHeader(topic, packetId), Unpooled.wrappedBuffer(value));
    }
}
