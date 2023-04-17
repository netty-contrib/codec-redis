/*
 * Copyright 2023 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.contrib.handler.codec.redis.example;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.contrib.handler.codec.redis.RedisArrayAggregator;
import io.netty.contrib.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.contrib.handler.codec.redis.RedisDecoder;
import io.netty.contrib.handler.codec.redis.RedisEncoder;

public final class RedisServer {

    private static final int PORT = Integer.parseInt(System.getProperty("port", "6379"));

    public static void main(String[] args) throws Exception {
        final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        final EventLoopGroup workerGroup = new NioEventLoopGroup();

        // This latch will become zero once `RedisServerHandler` receives a SHUTDOWN command.
        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        final ConcurrentMap<String, String> map = new ConcurrentHashMap<>();

        try {
            final ServerBootstrap b = new ServerBootstrap();
            b.channel(NioServerSocketChannel.class);
            b.group(bossGroup, workerGroup);
            b.childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    final ChannelPipeline p = ch.pipeline();
                    p.addLast(new RedisDecoder());
                    p.addLast(new RedisBulkStringAggregator());
                    p.addLast(new RedisArrayAggregator());
                    p.addLast(new RedisEncoder());
                    p.addLast(new RedisServerHandler(map, shutdownLatch));
                }
            });
            final Channel ch = b.bind(PORT).sync().channel();
            System.err.println("An example Redis server now listening at " + ch.localAddress() + " ..");

            // Wait until the latch becomes zero.
            shutdownLatch.await();
            System.err.println("Received a SHUTDOWN command; shutting down ..");
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private RedisServer() {}
}
