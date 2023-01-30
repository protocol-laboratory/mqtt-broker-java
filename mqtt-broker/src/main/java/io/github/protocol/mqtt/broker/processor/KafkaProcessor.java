package io.github.protocol.mqtt.broker.processor;

import io.github.protocol.mqtt.broker.MqttSessionKey;
import io.github.protocol.mqtt.broker.MqttTopicKey;
import io.github.protocol.mqtt.broker.auth.MqttAuth;
import io.github.protocol.mqtt.broker.util.BoundInt;
import io.github.protocol.mqtt.broker.util.ClosableUtils;
import io.github.protocol.mqtt.broker.util.MqttMessageUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class KafkaProcessor extends AbstractProcessor {

    protected final ReentrantReadWriteLock.ReadLock rLock;

    protected final ReentrantReadWriteLock.WriteLock wLock;

    protected final Map<MqttSessionKey, List<MqttTopicKey>> sessionConsumerMap;

    protected final Map<MqttTopicKey, KafkaConsumerListenerWrapper> consumerMap;

    private final KafkaProcessorConfig kafkaProcessorConfig;

    private final KafkaProducer<String, ByteBuffer>[] producerArray;

    public KafkaProcessor(MqttAuth mqttAuth, KafkaProcessorConfig kafkaProcessorConfig) {
        super(mqttAuth);
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.sessionConsumerMap = new HashMap<>();
        this.consumerMap = new HashMap<>();
        this.kafkaProcessorConfig = kafkaProcessorConfig;
        this.producerArray = new KafkaProducer[kafkaProcessorConfig.getProducerNum()];
        for (int i = 0; i < kafkaProcessorConfig.getProducerNum(); i++) {
            producerArray[i] = createProducer();
        }
    }

    @Override
    protected void onConnect(MqttSessionKey mqttSessionKey) {
        wLock.lock();
        try {
            sessionConsumerMap.put(mqttSessionKey, new ArrayList<>());
        } finally {
            wLock.unlock();
        }
    }

    protected KafkaProducer<String, ByteBuffer> createProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProcessorConfig.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class);
        return new KafkaProducer<>(properties);
    }

    @Override
    protected void onPublish(ChannelHandlerContext ctx, MqttSessionKey mqttSessionKey, MqttPublishMessage msg) {
        String topic = msg.variableHeader().topicName();
        ProducerRecord<String, ByteBuffer> record = new ProducerRecord<>(topic, msg.payload().nioBuffer());
        switch (msg.fixedHeader().qosLevel()) {
            case AT_MOST_ONCE -> getProducer(mqttSessionKey).send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("mqttSessionKey {} send message to kafka error", mqttSessionKey, exception);
                    return;
                }
                log.debug("mqttSessionKey {} send message to kafka success, topic {}, partition {}, offset {}",
                        mqttSessionKey, metadata.topic(), metadata.partition(), metadata.offset());
            });
            case AT_LEAST_ONCE -> {
                try {
                    RecordMetadata recordMetadata = getProducer(mqttSessionKey).send(record).get();
                    log.info("mqttSessionKey {} send message to kafka success, topic {}, partition {}, offset {}",
                            mqttSessionKey, recordMetadata.topic(),
                            recordMetadata.partition(), recordMetadata.offset());
                    ctx.writeAndFlush(MqttMessageUtil.pubAckMessage(msg.variableHeader().packetId()));
                } catch (Exception e) {
                    log.error("mqttSessionKey {} send message to kafka error", mqttSessionKey, e);
                }
            }
            case EXACTLY_ONCE, FAILURE -> throw new IllegalStateException(
                    String.format("mqttSessionKey %s can not reach here", mqttSessionKey));
        }
    }

    private Producer<String, ByteBuffer> getProducer(MqttSessionKey mqttSessionKey) {
        return producerArray[Math.abs(mqttSessionKey.hashCode() % kafkaProcessorConfig.getProducerNum())];
    }

    @Override
    protected void onSubscribe(ChannelHandlerContext ctx, MqttSessionKey mqttSessionKey,
                               MqttSubscribePayload subscribePayload) throws Exception {
        for (MqttTopicSubscription topicSubscription : subscribePayload.topicSubscriptions()) {
            KafkaConsumerListenerWrapper consumer = createConsumer(mqttSessionKey, topicSubscription.topicName());
            subscribe(ctx, consumer, topicSubscription.topicName());
        }
    }

    private KafkaConsumerListenerWrapper createConsumer(MqttSessionKey mqttSessionKey, String topic) {
        MqttTopicKey mqttTopicKey = new MqttTopicKey();
        mqttTopicKey.setMqttSessionKey(mqttSessionKey);
        mqttTopicKey.setTopic(topic);

        wLock.lock();
        try {
            KafkaConsumerListenerWrapper consumer = consumerMap.get(mqttTopicKey);
            if (consumer == null) {
                consumer = new KafkaConsumerListenerWrapper(kafkaProcessorConfig, mqttSessionKey.getUsername());
                sessionConsumerMap.compute(mqttSessionKey, (mqttSessionKey1, mqttTopicKeys) -> {
                    if (mqttTopicKeys == null) {
                        mqttTopicKeys = new ArrayList<>();
                    }
                    mqttTopicKeys.add(mqttTopicKey);
                    return mqttTopicKeys;
                });
                consumerMap.put(mqttTopicKey, consumer);
            }
            return consumer;
        } finally {
            wLock.unlock();
        }
    }

    protected void subscribe(ChannelHandlerContext ctx,
                             KafkaConsumerListenerWrapper consumer, String topic) throws Exception {
        BoundInt boundInt = new BoundInt(65535);
        consumer.start(topic, record -> {
            log.info("receive message from kafka, topic {}, partition {}, offset {}",
                    record.topic(), record.partition(), record.offset());
            MqttPublishMessage mqttPublishMessage = MqttMessageUtil.publishMessage(
                    MqttQoS.AT_LEAST_ONCE, topic, boundInt.nextVal(), record.value());
            ctx.writeAndFlush(mqttPublishMessage);
        });
    }

    @Override
    protected void onDisconnect(MqttSessionKey mqttSessionKey) {
        wLock.lock();
        try {
            List<MqttTopicKey> consumeTopicKeys = sessionConsumerMap.get(mqttSessionKey);
            if (consumeTopicKeys != null) {
                for (MqttTopicKey mqttTopicKey : consumeTopicKeys) {
                    KafkaConsumerListenerWrapper consumer = consumerMap.get(mqttTopicKey);
                    if (consumer != null) {
                        ClosableUtils.close(consumer);
                        consumerMap.remove(mqttTopicKey);
                    }
                }
            }
            sessionConsumerMap.remove(mqttSessionKey);
        } finally {
            wLock.unlock();
        }
    }

}
