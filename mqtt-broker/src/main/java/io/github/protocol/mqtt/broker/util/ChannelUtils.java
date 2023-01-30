package io.github.protocol.mqtt.broker.util;

import io.github.protocol.mqtt.broker.MqttSessionKey;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

public class ChannelUtils {

    private static final AttributeKey<MqttSessionKey> ATTR_KEY_SESSION = AttributeKey.valueOf("mqtt_session");

    public static void setMqttSession(Channel channel, MqttSessionKey mqttSessionKey) {
        channel.attr(ATTR_KEY_SESSION).set(mqttSessionKey);
    }

    public static MqttSessionKey getMqttSession(Channel channel) {
        return channel.attr(ATTR_KEY_SESSION).get();
    }

}
