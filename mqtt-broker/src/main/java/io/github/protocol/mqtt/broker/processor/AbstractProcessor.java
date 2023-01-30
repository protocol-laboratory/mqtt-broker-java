package io.github.protocol.mqtt.broker.processor;

import io.github.protocol.mqtt.broker.MqttSessionKey;
import io.github.protocol.mqtt.broker.auth.MqttAuth;
import io.github.protocol.mqtt.broker.util.ChannelUtils;
import io.github.protocol.mqtt.broker.util.MqttMessageUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.stream.IntStream;

@Slf4j
public abstract class AbstractProcessor implements MqttProcessor {

    protected final MqttAuth mqttAuth;

    public AbstractProcessor(MqttAuth mqttAuth) {
        this.mqttAuth = mqttAuth;
    }

    @Override
    public void processConnect(ChannelHandlerContext ctx, MqttConnectMessage msg) throws Exception {
        String clientId = msg.payload().clientIdentifier();
        String username = msg.payload().userName();
        byte[] pwd = msg.payload().passwordInBytes();
        if (StringUtils.isBlank(clientId) || StringUtils.isBlank(username)) {
            MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                    new MqttFixedHeader(MqttMessageType.CONNACK,
                            false, MqttQoS.AT_MOST_ONCE, false, 0),
                    new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED,
                            false), null);
            ctx.writeAndFlush(connAckMessage);
            log.error("the clientId username pwd cannot be empty, client address[{}]", ctx.channel().remoteAddress());
            ctx.close();
            return;
        }
        if (!mqttAuth.connAuth(clientId, username, pwd)) {
            MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                    new MqttFixedHeader(MqttMessageType.CONNACK,
                            false, MqttQoS.AT_MOST_ONCE, false, 0),
                    new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD,
                            false), null);
            ctx.writeAndFlush(connAckMessage);
            log.error("the clientId username pwd cannot be empty, client address[{}]", ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        MqttSessionKey mqttSessionKey = new MqttSessionKey();
        mqttSessionKey.setUsername(username);
        mqttSessionKey.setClientId(clientId);
        ChannelUtils.setMqttSession(ctx.channel(), mqttSessionKey);
        log.info("username {} clientId {} remote address {} connected",
                username, clientId, ctx.channel().remoteAddress());
        onConnect(mqttSessionKey);
        MqttConnAckMessage mqttConnectMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.CONNACK,
                        false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false),
                null);
        ctx.writeAndFlush(mqttConnectMessage);
    }

    protected void onConnect(MqttSessionKey mqttSessionKey) {
    }

    @Override
    public void processConnAck(ChannelHandlerContext ctx, MqttConnAckMessage msg) throws Exception {
        MqttSessionKey mqttSession = ChannelUtils.getMqttSession(ctx.channel());
        if (mqttSession == null) {
            log.error("conn ack, client address {} not authed", ctx.channel().remoteAddress());
            ctx.close();
        }
    }

    @Override
    public void processPublish(ChannelHandlerContext ctx, MqttPublishMessage msg) throws Exception {
        MqttSessionKey mqttSession = ChannelUtils.getMqttSession(ctx.channel());
        if (mqttSession == null) {
            log.error("publish, client address {} not authed", ctx.channel().remoteAddress());
            ctx.close();
            return;
        }
        if (msg.fixedHeader().qosLevel() == MqttQoS.FAILURE) {
            log.error("failure. clientId {}, username {} ", mqttSession.getClientId(), mqttSession.getUsername());
            return;
        }
        if (msg.fixedHeader().qosLevel() == MqttQoS.EXACTLY_ONCE) {
            log.error("does not support QoS2 protocol. clientId {}, username {} ",
                    mqttSession.getClientId(), mqttSession.getUsername());
            return;
        }
        onPublish(ctx, mqttSession, msg);
    }

    protected void onPublish(ChannelHandlerContext ctx, MqttSessionKey mqttSessionKey,
                             MqttPublishMessage msg) throws Exception {
    }

    @Override
    public void processPubAck(ChannelHandlerContext ctx, MqttPubAckMessage msg) throws Exception {
        MqttSessionKey mqttSession = ChannelUtils.getMqttSession(ctx.channel());
        if (mqttSession == null) {
            log.error("pub ack, client address {} not authed", ctx.channel().remoteAddress());
            ctx.close();
        }
    }

    @Override
    public void processPubRec(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
        MqttSessionKey mqttSession = ChannelUtils.getMqttSession(ctx.channel());
        if (mqttSession == null) {
            log.error("pub rec, client address {} not authed", ctx.channel().remoteAddress());
            ctx.close();
        }
    }

    @Override
    public void processPubRel(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
        MqttSessionKey mqttSession = ChannelUtils.getMqttSession(ctx.channel());
        if (mqttSession == null) {
            log.error("pub rel, client address {} not authed", ctx.channel().remoteAddress());
            ctx.close();
        }
    }

    @Override
    public void processPubComp(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
        MqttSessionKey mqttSession = ChannelUtils.getMqttSession(ctx.channel());
        if (mqttSession == null) {
            log.error("pub comp, client address {} not authed", ctx.channel().remoteAddress());
            ctx.close();
        }
    }

    @Override
    public void processSubscribe(ChannelHandlerContext ctx, MqttSubscribeMessage msg) throws Exception {
        MqttSessionKey mqttSession = ChannelUtils.getMqttSession(ctx.channel());
        if (mqttSession == null) {
            log.error("sub, client address {} not authed", ctx.channel().remoteAddress());
            ctx.close();
        }
        onSubscribe(ctx, mqttSession, msg.payload());
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK,
                false, MqttQoS.AT_MOST_ONCE, false, 0);
        IntStream intStream = msg.payload().topicSubscriptions().stream().mapToInt(s -> s.qualityOfService().value());
        MqttSubAckPayload payload = new MqttSubAckPayload(intStream.toArray());
        ctx.writeAndFlush(MqttMessageFactory.newMessage(
                fixedHeader,
                MqttMessageIdVariableHeader.from(msg.variableHeader().messageId()),
                payload));
    }

    protected void onSubscribe(ChannelHandlerContext ctx, MqttSessionKey mqttSessionKey,
                               MqttSubscribePayload subscribePayload) throws Exception {
    }

    @Override
    public void processSubAck(ChannelHandlerContext ctx, MqttSubAckMessage msg) throws Exception {
        MqttSessionKey mqttSession = ChannelUtils.getMqttSession(ctx.channel());
        if (mqttSession == null) {
            log.error("sub ack, client address {} not authed", ctx.channel().remoteAddress());
            ctx.close();
        }
    }

    @Override
    public void processUnsubscribe(ChannelHandlerContext ctx, MqttUnsubscribeMessage msg) throws Exception {
        MqttSessionKey mqttSession = ChannelUtils.getMqttSession(ctx.channel());
        if (mqttSession == null) {
            log.error("unsub, client address {} not authed", ctx.channel().remoteAddress());
            ctx.close();
        }
    }

    @Override
    public void processUnsubAck(ChannelHandlerContext ctx, MqttUnsubAckMessage msg) throws Exception {
        MqttSessionKey mqttSession = ChannelUtils.getMqttSession(ctx.channel());
        if (mqttSession == null) {
            log.error("unsub ack, client address {} not authed", ctx.channel().remoteAddress());
            ctx.close();
        }
    }

    @Override
    public void processPingReq(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
        ctx.writeAndFlush(MqttMessageUtil.pingResp());
    }

    @Override
    public void processPingResp(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
        MqttSessionKey mqttSession = ChannelUtils.getMqttSession(ctx.channel());
        if (mqttSession == null) {
            log.error("ping resp, client address {} not authed", ctx.channel().remoteAddress());
            ctx.close();
        }
    }

    @Override
    public void processDisconnect(ChannelHandlerContext ctx) throws Exception {
        MqttSessionKey mqttSession = ChannelUtils.getMqttSession(ctx.channel());
        if (mqttSession == null) {
            log.error("disconnect, client address {} not authed", ctx.channel().remoteAddress());
        }
        onDisconnect(mqttSession);
    }

    protected void onDisconnect(MqttSessionKey mqttSessionKey) {
    }

    @Override
    public void processAuth(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
        MqttSessionKey mqttSession = ChannelUtils.getMqttSession(ctx.channel());
        if (mqttSession == null) {
            log.error("auth, client address {} not authed", ctx.channel().remoteAddress());
            ctx.close();
        }
    }
}
