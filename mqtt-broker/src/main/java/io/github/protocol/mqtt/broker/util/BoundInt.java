package io.github.protocol.mqtt.broker.util;

public class BoundInt {

    private final int max;

    private int val;

    public BoundInt(int max) {
        this.max = max;
        this.val = 0;
    }

    public int nextVal() {
        if (val == max) {
            val = 0;
        }
        return val++;
    }
}
