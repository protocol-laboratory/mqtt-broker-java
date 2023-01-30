package io.github.protocol.mqtt.broker.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface KafkaMessageListener {
    void messageReceived(ConsumerRecord<String, byte[]> record);
}
