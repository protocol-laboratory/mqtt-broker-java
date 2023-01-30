package io.github.protocol.mqtt.broker.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class KafkaConsumerListenerWrapper implements AutoCloseable {

    private final AdminClient adminClient;

    private final KafkaConsumer<String, byte[]> consumer;

    public KafkaConsumerListenerWrapper(KafkaProcessorConfig config, String username) {
        Properties adminProperties = new Properties();
        adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        this.adminClient = KafkaAdminClient.create(adminProperties);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, username);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        this.consumer = new KafkaConsumer<>(properties);
    }

    public void start(String topic, KafkaMessageListener listener) throws Exception {
        try {
            TopicDescription topicDescription = adminClient.describeTopics(Collections.singletonList(topic))
                    .topicNameValues().get(topic).get();
            log.info("topic info is {}", topicDescription);
        } catch (ExecutionException ee) {
            if (ee.getCause() instanceof UnknownTopicOrPartitionException) {
                log.info("topic {} not exist, create it", topic);
                adminClient.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)));
            } else {
                log.error("find topic info {} error", topic, ee);
            }
        } catch (Exception e) {
            throw new IllegalStateException("find topic info error", e);
        }
        consumer.subscribe(Collections.singletonList(topic));
        log.info("consumer topic {} start", topic);
        new Thread(() -> {
            try {
                while (true) {
                    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
                    for (ConsumerRecord<String, byte[]> record : records) {
                        listener.messageReceived(record);
                    }
                }
            } catch (WakeupException we) {
                consumer.close();
            } catch (Exception e) {
                log.error("consumer topic {} consume error", topic, e);
                consumer.close();
            }
        }).start();
        Thread.sleep(5_000);
    }

    @Override
    public void close() throws Exception {
        log.info("wake up {} consumer", consumer);
        consumer.wakeup();
    }
}
