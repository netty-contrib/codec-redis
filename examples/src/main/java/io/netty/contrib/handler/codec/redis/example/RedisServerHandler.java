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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.contrib.handler.codec.redis.ArrayRedisMessage;
import io.netty.contrib.handler.codec.redis.ErrorRedisMessage;
import io.netty.contrib.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.contrib.handler.codec.redis.IntegerRedisMessage;
import io.netty.contrib.handler.codec.redis.RedisMessage;
import io.netty.contrib.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.util.ReferenceCountUtil;

final class RedisServerHandler extends ChannelInboundHandlerAdapter {

    private final ConcurrentMap<String, String> map;
    private final CountDownLatch shutdownLatch;

    RedisServerHandler(ConcurrentMap<String, String> map, CountDownLatch shutdownLatch) {
        this.map = map;
        this.shutdownLatch = shutdownLatch;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try {
            if (!(msg instanceof ArrayRedisMessage)) {
                rejectMalformedRequest(ctx);
                return;
            }

            final ArrayRedisMessage req = (ArrayRedisMessage) msg;
            final List<RedisMessage> args = req.children();
            for (RedisMessage a : args) {
                if (!(a instanceof FullBulkStringRedisMessage)) {
                    rejectMalformedRequest(ctx);
                    return;
                }
            }

            // For simplicity, convert all arguments into strings.
            // In a real server, you need to handle them as they are or convert to byte[].
            final List<String> strArgs =
                    args.stream()
                        .map(a -> {
                            final FullBulkStringRedisMessage bulkStr = (FullBulkStringRedisMessage) a;
                            if (!bulkStr.isNull()) {
                                return bulkStr.content().toString(StandardCharsets.UTF_8);
                            } else {
                                return null;
                            }
                        })
                        .collect(Collectors.toList());

            System.err.println(ctx.channel() + " RCVD: " + strArgs);

            final String command = strArgs.get(0);
            switch (command) {
                case "COMMAND":
                    ctx.writeAndFlush(ArrayRedisMessage.EMPTY_INSTANCE);
                    break;
                case "GET": {
                    handleGet(ctx, strArgs);
                    break;
                }
                case "SET": {
                    handleSet(ctx, strArgs);
                    break;
                }
                case "DEL": {
                    handleDel(ctx, strArgs);
                    break;
                }
                case "SHUTDOWN":
                    ctx.writeAndFlush(new SimpleStringRedisMessage("OK"))
                       .addListener((ChannelFutureListener) f -> {
                           shutdownLatch.countDown();
                       });
                    break;
                default:
                    reject(ctx, "ERR Unsupported command");
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private void handleGet(ChannelHandlerContext ctx, List<String> strArgs) {
        if (strArgs.size() < 2) {
            reject(ctx, "ERR A GET command requires a key argument.");
            return;
        }

        final String key = strArgs.get(1);
        if (key == null) {
            rejectNilKey(ctx);
            return;
        }

        final String value = map.get(key);
        final FullBulkStringRedisMessage reply;
        if (value != null) {
            reply = newBulkStringMessage(value);
        } else {
            reply = FullBulkStringRedisMessage.NULL_INSTANCE;
        }

        ctx.writeAndFlush(reply);
    }

    private void handleSet(ChannelHandlerContext ctx, List<String> strArgs) {
        if (strArgs.size() < 3) {
            reject(ctx, "ERR A GET command requires a key argument.");
            return;
        }
        final String key = strArgs.get(1);
        final String value = strArgs.get(2);
        if (key == null) {
            rejectNilKey(ctx);
            return;
        }
        if (value == null) {
            reject(ctx, "ERR A nil value is not allowed.");
            return;
        }

        // Very naive simple detection of 'GET' option.
        // Note that we don't support other options which might appear before
        // the 'GET' option in reality.
        final boolean shouldReplyOldValue =
                strArgs.size() > 3 && "GET".equals(strArgs.get(3));

        final String oldValue = map.put(key, value);
        final RedisMessage reply;
        if (shouldReplyOldValue) {
            if (oldValue != null) {
                reply = newBulkStringMessage(oldValue);
            } else {
                reply = FullBulkStringRedisMessage.NULL_INSTANCE;
            }
        } else {
            reply = new SimpleStringRedisMessage("OK");
        }

        ctx.writeAndFlush(reply);
    }

    private void handleDel(ChannelHandlerContext ctx, List<String> strArgs) {
        if (strArgs.size() < 2) {
            reject(ctx, "ERR A DEL command requires at least one key argument.");
            return;
        }
        int removedEntries = 0;
        for (int i = 1; i < strArgs.size(); i++) {
            final String key = strArgs.get(i);
            if (key == null) {
                continue;
            }
            if (map.remove(key) != null) {
                removedEntries++;
            }
        }

        ctx.writeAndFlush(new IntegerRedisMessage(removedEntries));
    }

    private static FullBulkStringRedisMessage newBulkStringMessage(String oldValue) {
        return new FullBulkStringRedisMessage(
                Unpooled.copiedBuffer(oldValue, StandardCharsets.UTF_8));
    }

    private static void rejectMalformedRequest(ChannelHandlerContext ctx) {
        reject(ctx, "ERR Client request bust be an array of bulk strings.");
    }

    private static void rejectNilKey(ChannelHandlerContext ctx) {
        reject(ctx, "ERR A nil key is not allowed.");
    }

    private static void reject(ChannelHandlerContext ctx, String error) {
        ctx.writeAndFlush(new ErrorRedisMessage(error));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.err.println("Unexpected exception handling " + ctx.channel());
        cause.printStackTrace(System.err);
        ctx.close();
    }
}
