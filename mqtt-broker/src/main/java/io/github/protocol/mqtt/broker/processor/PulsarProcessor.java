package io.github.protocol.mqtt.broker.processor;

import io.github.protocol.mqtt.broker.MqttSessionKey;
import io.github.protocol.mqtt.broker.MqttTopicKey;
import io.github.protocol.mqtt.broker.auth.MqttAuth;
import io.github.protocol.mqtt.broker.util.BoundInt;
import io.github.protocol.mqtt.broker.util.ClosableUtils;
import io.github.protocol.mqtt.broker.util.MqttMessageUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class PulsarProcessor extends AbstractProcessor {

    protected final ReentrantReadWriteLock.ReadLock rLock;

    protected final ReentrantReadWriteLock.WriteLock wLock;

    protected final Map<MqttSessionKey, List<MqttTopicKey>> sessionProducerMap;

    protected final Map<MqttSessionKey, List<MqttTopicKey>> sessionConsumerMap;

    protected final Map<MqttTopicKey, Producer<byte[]>> producerMap;

    protected final Map<MqttTopicKey, Consumer<byte[]>> consumerMap;

    private final PulsarProcessorConfig pulsarProcessorConfig;

    private final PulsarAdmin pulsarAdmin;

    private final PulsarClient pulsarClient;

    public PulsarProcessor(MqttAuth mqttAuth, PulsarProcessorConfig pulsarProcessorConfig) {
        super(mqttAuth);
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.sessionProducerMap = new HashMap<>();
        this.sessionConsumerMap = new HashMap<>();
        this.producerMap = new HashMap<>();
        this.consumerMap = new HashMap<>();
        this.pulsarProcessorConfig = pulsarProcessorConfig;
        try {
            this.pulsarAdmin = PulsarAdmin.builder()
                    .serviceHttpUrl(pulsarProcessorConfig.getHttpUrl())
                    .build();
            this.pulsarClient = PulsarClient.builder()
                    .serviceUrl(pulsarProcessorConfig.getServiceUrl())
                    .build();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create pulsar client", e);
        }
    }

    @Override
    protected void onConnect(MqttSessionKey mqttSessionKey) {
        wLock.lock();
        try {
            sessionProducerMap.put(mqttSessionKey, new ArrayList<>());
            sessionConsumerMap.put(mqttSessionKey, new ArrayList<>());
        } finally {
            wLock.unlock();
        }
    }

    @Override
    protected void onPublish(ChannelHandlerContext ctx, MqttSessionKey mqttSessionKey,
                             MqttPublishMessage msg) throws Exception {
        String topic = msg.variableHeader().topicName();
        Producer<byte[]> producer = getOrCreateProducer(mqttSessionKey, topic);
        int len = msg.payload().readableBytes();
        byte[] messageBytes = new byte[len];
        msg.payload().getBytes(msg.payload().readerIndex(), messageBytes);
        switch (msg.fixedHeader().qosLevel()) {
            case AT_MOST_ONCE -> producer.sendAsync(messageBytes).
                    thenAccept(messageId -> log.info("clientId [{}],"
                                    + " username [{}]. send message to pulsar success messageId: {}",
                            mqttSessionKey.getClientId(), mqttSessionKey.getUsername(), messageId))
                    .exceptionally((e) -> {
                        log.error("clientId [{}], username [{}]. send message to pulsar fail: ",
                                mqttSessionKey.getClientId(), mqttSessionKey.getUsername(), e);
                        return null;
                    });
            case AT_LEAST_ONCE -> {
                try {
                    MessageId messageId = producer.send(messageBytes);
                    MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK,
                            false, MqttQoS.AT_MOST_ONCE, false, 0);
                    MqttPubAckMessage pubAckMessage = (MqttPubAckMessage) MqttMessageFactory.newMessage(fixedHeader,
                            MqttMessageIdVariableHeader.from(msg.variableHeader().packetId()), null);
                    log.info("clientId [{}], username [{}]. send pulsar success. messageId: {}",
                            mqttSessionKey.getClientId(), mqttSessionKey.getUsername(), messageId);
                    ctx.writeAndFlush(pubAckMessage);
                } catch (PulsarClientException e) {
                    log.error("clientId [{}], username [{}]. send pulsar error: {}",
                            mqttSessionKey.getClientId(), mqttSessionKey.getUsername(), e.getMessage());
                }
            }
            case EXACTLY_ONCE, FAILURE -> throw new IllegalStateException(
                    String.format("mqttSessionKey %s can not reach here", mqttSessionKey));
        }
    }

    private Producer<byte[]> getOrCreateProducer(MqttSessionKey mqttSessionKey, String topic) throws Exception {
        MqttTopicKey mqttTopicKey = new MqttTopicKey();
        mqttTopicKey.setMqttSessionKey(mqttSessionKey);
        mqttTopicKey.setTopic(topic);

        rLock.lock();
        try {
            Producer<byte[]> producer = producerMap.get(mqttTopicKey);
            if (producer != null) {
                return producer;
            }
        } finally {
            rLock.unlock();
        }

        wLock.lock();
        try {
            Producer<byte[]> producer = producerMap.get(mqttTopicKey);
            if (producer == null) {
                producer = createProducer(topic);
                sessionProducerMap.compute(mqttSessionKey, (mqttSessionKey1, mqttTopicKeys) -> {
                    if (mqttTopicKeys == null) {
                        mqttTopicKeys = new ArrayList<>();
                    }
                    mqttTopicKeys.add(mqttTopicKey);
                    return mqttTopicKeys;
                });
                producerMap.put(mqttTopicKey, producer);
            }
            return producer;
        } finally {
            wLock.unlock();
        }
    }

    protected Producer<byte[]> createProducer(String topic) throws Exception {
        return pulsarClient.newProducer(Schema.BYTES).topic(topic).create();
    }

    @Override
    protected void onSubscribe(ChannelHandlerContext ctx, MqttSessionKey mqttSessionKey,
                               MqttSubscribePayload subscribePayload) throws Exception {
        for (MqttTopicSubscription topicSubscription : subscribePayload.topicSubscriptions()) {
            subscribe(ctx, mqttSessionKey, topicSubscription.topicName());
        }
    }

    protected void subscribe(ChannelHandlerContext ctx, MqttSessionKey mqttSessionKey,
                             String topic) throws Exception {
        MqttTopicKey mqttTopicKey = new MqttTopicKey();
        mqttTopicKey.setMqttSessionKey(mqttSessionKey);
        mqttTopicKey.setTopic(topic);

        wLock.lock();
        try {
            Consumer<byte[]> consumer = consumerMap.get(mqttTopicKey);
            if (consumer == null) {
                consumer = createConsumer(ctx, mqttSessionKey.getUsername(), topic);
                sessionConsumerMap.compute(mqttSessionKey, (mqttSessionKey1, mqttTopicKeys) -> {
                    if (mqttTopicKeys == null) {
                        mqttTopicKeys = new ArrayList<>();
                    }
                    mqttTopicKeys.add(mqttTopicKey);
                    return mqttTopicKeys;
                });
                consumerMap.put(mqttTopicKey, consumer);
            }
        } finally {
            wLock.unlock();
        }
    }

    protected Consumer<byte[]> createConsumer(ChannelHandlerContext ctx, String username,
                                              String topic) throws Exception {
        BoundInt boundInt = new BoundInt(65535);
        try {
            PartitionedTopicStats partitionedStats = pulsarAdmin.topics().getPartitionedStats(topic, false);
            log.info("topic {} partitioned stats {}", topic, partitionedStats);
        } catch (PulsarAdminException.NotFoundException nfe) {
            log.info("topic {} not found", topic);
            pulsarAdmin.topics().createPartitionedTopic(topic, 1);
        }
        return pulsarClient.newConsumer(Schema.BYTES).topic(topic)
                .messageListener((consumer, msg) -> {
                    log.info("receive message from pulsar, topic {}, message {}", topic, msg.getMessageId());
                    MqttPublishMessage mqttPublishMessage = MqttMessageUtil.publishMessage(
                            MqttQoS.AT_LEAST_ONCE, topic, boundInt.nextVal(), msg.getData());
                    ctx.writeAndFlush(mqttPublishMessage);
                })
                .subscriptionName(username).subscribe();
    }

    @Override
    protected void onDisconnect(MqttSessionKey mqttSessionKey) {
        wLock.lock();
        try {
            List<MqttTopicKey> produceTopicKeys = sessionProducerMap.get(mqttSessionKey);
            if (produceTopicKeys != null) {
                for (MqttTopicKey mqttTopicKey : produceTopicKeys) {
                    Producer<byte[]> producer = producerMap.get(mqttTopicKey);
                    if (producer != null) {
                        ClosableUtils.close(producer);
                        producerMap.remove(mqttTopicKey);
                    }
                }
            }
            sessionProducerMap.remove(mqttSessionKey);
            List<MqttTopicKey> consumeTopicKeys = sessionConsumerMap.get(mqttSessionKey);
            if (consumeTopicKeys != null) {
                for (MqttTopicKey mqttTopicKey : consumeTopicKeys) {
                    Consumer<byte[]> consumer = consumerMap.get(mqttTopicKey);
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
