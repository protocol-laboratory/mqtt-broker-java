package io.github.protocol.mqtt.broker;

import io.github.protocol.mqtt.broker.processor.KafkaProcessor;
import io.github.protocol.mqtt.broker.processor.MqttProcessor;
import io.github.protocol.mqtt.broker.processor.PulsarProcessor;
import io.github.protocol.mqtt.broker.util.EventLoopUtil;
import io.github.protocol.mqtt.broker.util.SocketUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MqttServer {

    private final MqttServerConfig mqttServerConfig;

    public MqttServer() {
        this(new MqttServerConfig());
    }

    public MqttServer(MqttServerConfig mqttServerConfig) {
        this.mqttServerConfig = mqttServerConfig;
        if (mqttServerConfig.getPort() == 0) {
            mqttServerConfig.setPort(SocketUtil.getFreePort());
        }
    }

    public void start() throws Exception {
        EventLoopGroup acceptorGroup = EventLoopUtil.newEventLoopGroup(1,
                new DefaultThreadFactory("mqtt-acceptor"));
        EventLoopGroup workerGroup = EventLoopUtil.newEventLoopGroup(1,
                new DefaultThreadFactory("mqtt-worker"));
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(acceptorGroup, workerGroup)
                    .channel(EventLoopUtil.getServerSocketChannelClass(workerGroup))
                    .option(ChannelOption.SO_BACKLOG, 100)
                    .option(ChannelOption.SO_KEEPALIVE, false)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            // decoder
                            p.addLast(new MqttDecoder());
                            p.addLast(MqttEncoder.INSTANCE);
                            p.addLast(new MqttHandler(processor(mqttServerConfig)));
                        }
                    });

            // Start the server.
            ChannelFuture f = b.bind(mqttServerConfig.getPort()).sync();

            // Wait until the server socket is closed.
            f.channel().closeFuture().sync();
        } finally {
            // Shut down all event loops to terminate all threads.
            acceptorGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private MqttProcessor processor(MqttServerConfig config) {
        return switch (config.getProcessorType()) {
            case KAFKA -> new KafkaProcessor(config.getMqttAuth(), config.getKafkaProcessorConfig());
            case PULSAR -> new PulsarProcessor(config.getMqttAuth(), config.getPulsarProcessorConfig());
        };
    }

    public int getPort() {
        return mqttServerConfig.getPort();
    }

}
