package io.github.protocol.mqtt.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;

public interface MqttProcessor {

    void processConnect(ChannelHandlerContext ctx, MqttConnectMessage msg) throws Exception;

    void processConnAck(ChannelHandlerContext ctx, MqttConnAckMessage msg) throws Exception;

    void processPublish(ChannelHandlerContext ctx, MqttPublishMessage msg) throws Exception;

    void processPubAck(ChannelHandlerContext ctx, MqttPubAckMessage msg) throws Exception;

    void processPubRec(ChannelHandlerContext ctx, MqttMessage msg) throws Exception;

    void processPubRel(ChannelHandlerContext ctx, MqttMessage msg) throws Exception;

    void processPubComp(ChannelHandlerContext ctx, MqttMessage msg) throws Exception;

    void processSubscribe(ChannelHandlerContext ctx, MqttSubscribeMessage msg) throws Exception;

    void processSubAck(ChannelHandlerContext ctx, MqttSubAckMessage msg) throws Exception;

    void processUnsubscribe(ChannelHandlerContext ctx, MqttUnsubscribeMessage msg) throws Exception;

    void processUnsubAck(ChannelHandlerContext ctx, MqttUnsubAckMessage msg) throws Exception;

    void processPingReq(ChannelHandlerContext ctx, MqttMessage msg) throws Exception;

    void processPingResp(ChannelHandlerContext ctx, MqttMessage msg) throws Exception;

    void processDisconnect(ChannelHandlerContext ctx) throws Exception;

    void processAuth(ChannelHandlerContext ctx, MqttMessage msg) throws Exception;

}
