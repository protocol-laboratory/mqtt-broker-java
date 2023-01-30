package io.github.protocol.mqtt.broker.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;

public class SocketUtil {

    public static int getFreePort() {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
