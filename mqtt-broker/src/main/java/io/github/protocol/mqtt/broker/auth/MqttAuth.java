package io.github.protocol.mqtt.broker.auth;

public interface MqttAuth {

    boolean connAuth(String username, String clientId, byte[] password);

    boolean pubAuth(String username, String clientId, byte[] password, String topic);

    boolean subAuth(String username, String clientId, byte[] password, String topic);

}
