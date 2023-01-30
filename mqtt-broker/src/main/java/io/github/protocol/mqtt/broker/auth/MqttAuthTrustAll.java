package io.github.protocol.mqtt.broker.auth;

public class MqttAuthTrustAll implements MqttAuth {

    @Override
    public boolean connAuth(String username, String clientId, byte[] password) {
        return true;
    }

    @Override
    public boolean pubAuth(String username, String clientId, byte[] password, String topic) {
        return true;
    }

    @Override
    public boolean subAuth(String username, String clientId, byte[] password, String topic) {
        return true;
    }
}
