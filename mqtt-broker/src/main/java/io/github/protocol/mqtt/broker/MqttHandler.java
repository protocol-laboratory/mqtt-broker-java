package io.github.protocol.mqtt.broker;

import com.google.common.base.Preconditions;
import io.github.protocol.mqtt.broker.processor.MqttProcessor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttIdentifierRejectedException;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnacceptableProtocolVersionException;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MqttHandler extends ChannelInboundHandlerAdapter {

    private final MqttProcessor processor;

    public MqttHandler(MqttProcessor processor) {
        this.processor = processor;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        Preconditions.checkArgument(message instanceof MqttMessage);
        MqttMessage msg = (MqttMessage) message;
        try {
            if (msg.decoderResult().isFailure()) {
                Throwable cause = msg.decoderResult().cause();
                if (cause instanceof MqttUnacceptableProtocolVersionException) {
                    // Unsupported protocol version
                    MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                            new MqttFixedHeader(MqttMessageType.CONNACK,
                                    false, MqttQoS.AT_MOST_ONCE, false, 0),
                            new MqttConnAckVariableHeader(
                                    MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION,
                                    false), null);
                    ctx.writeAndFlush(connAckMessage);
                    log.error("connection refused due to invalid protocol, client address [{}]",
                            ctx.channel().remoteAddress());
                    ctx.close();
                    return;
                } else if (cause instanceof MqttIdentifierRejectedException) {
                    // ineligible clientId
                    MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                            new MqttFixedHeader(MqttMessageType.CONNACK,
                                    false, MqttQoS.AT_MOST_ONCE, false, 0),
                            new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED,
                                    false), null);
                    ctx.writeAndFlush(connAckMessage);
                    log.error("ineligible clientId, client address [{}]", ctx.channel().remoteAddress());
                    ctx.close();
                    return;
                }
                throw new IllegalStateException(msg.decoderResult().cause().getMessage());
            }
            MqttMessageType messageType = msg.fixedHeader().messageType();
            if (log.isDebugEnabled()) {
                log.debug("Processing MQTT Inbound handler message, type={}", messageType);
            }
            switch (messageType) {
                case CONNECT -> {
                    Preconditions.checkArgument(msg instanceof MqttConnectMessage);
                    processor.processConnect(ctx, (MqttConnectMessage) msg);
                }
                case CONNACK -> {
                    Preconditions.checkArgument(msg instanceof MqttConnAckMessage);
                    processor.processConnAck(ctx, (MqttConnAckMessage) msg);
                }
                case PUBLISH -> {
                    Preconditions.checkArgument(msg instanceof MqttPublishMessage);
                    processor.processPublish(ctx, (MqttPublishMessage) msg);
                }
                case PUBACK -> {
                    Preconditions.checkArgument(msg instanceof MqttPubAckMessage);
                    processor.processPubAck(ctx, (MqttPubAckMessage) msg);
                }
                case PUBREC -> processor.processPubRec(ctx, msg);
                case PUBREL -> processor.processPubRel(ctx, msg);
                case PUBCOMP -> processor.processPubComp(ctx, msg);
                case SUBSCRIBE -> {
                    Preconditions.checkArgument(msg instanceof MqttSubscribeMessage);
                    processor.processSubscribe(ctx, (MqttSubscribeMessage) msg);
                }
                case SUBACK -> {
                    Preconditions.checkArgument(msg instanceof MqttSubAckMessage);
                    processor.processSubAck(ctx, (MqttSubAckMessage) msg);
                }
                case UNSUBSCRIBE -> {
                    Preconditions.checkArgument(msg instanceof MqttUnsubscribeMessage);
                    processor.processUnsubscribe(ctx, (MqttUnsubscribeMessage) msg);
                }
                case UNSUBACK -> {
                    Preconditions.checkArgument(msg instanceof MqttUnsubAckMessage);
                    processor.processUnsubAck(ctx, (MqttUnsubAckMessage) msg);
                }
                case PINGREQ -> processor.processPingReq(ctx, msg);
                case PINGRESP -> processor.processPingResp(ctx, msg);
                case DISCONNECT -> processor.processDisconnect(ctx);
                case AUTH -> processor.processAuth(ctx, msg);
                default -> throw new UnsupportedOperationException("Unknown MessageType: " + messageType);
            }
        } catch (Throwable ex) {
            ReferenceCountUtil.safeRelease(msg);
            log.error("Exception was caught while processing MQTT message, ", ex);
            ctx.close();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        processor.processDisconnect(ctx);
    }
}

