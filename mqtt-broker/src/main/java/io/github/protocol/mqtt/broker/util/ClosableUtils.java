package io.github.protocol.mqtt.broker.util;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClosableUtils {
    public static void close(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            log.error("close failed exception is ", e);
        }
    }

}
