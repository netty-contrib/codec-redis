/*
 * Copyright 2021 The Netty Project
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
package io.netty.contrib.handler.codec.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.netty.contrib.handler.codec.redis.RedisCodecTestUtil.byteBufOf;
import static io.netty.contrib.handler.codec.redis.RedisCodecTestUtil.bytesOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.internal.bytebuddy.implementation.FixedValue.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verifies the correct functionality of the {@link RedisDecoder} and {@link RedisArrayAggregator}.
 */
@SuppressWarnings("OverlyStrongTypeCast")
public class RedisDecoderTest {

    private EmbeddedChannel channel;

    @BeforeEach
    public void setup() throws Exception {
        channel = newChannel(false);
    }

    private static EmbeddedChannel newChannel(boolean decodeInlineCommands) {
        return new EmbeddedChannel(
                new RedisDecoder(decodeInlineCommands),
                new RedisBulkStringAggregator(),
                new RedisArrayAggregator(),
                new RedisMapAggregator());
    }

    @AfterEach
    public void tearDown() throws Exception {
        assertFalse(channel.finish());
    }

    @Test
    public void splitEOLDoesNotInfiniteLoop() throws Exception {
        assertFalse(channel.writeInbound(byteBufOf("$6\r\nfoobar\r")));
        assertTrue(channel.writeInbound(byteBufOf("\n")));

        RedisMessage msg = channel.readInbound();
        assertTrue(msg instanceof FullBulkStringRedisMessage);
        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldNotDecodeInlineCommandByDefault() {
        assertThrows(DecoderException.class, () -> {
            assertFalse(channel.writeInbound(byteBufOf("P")));
            assertFalse(channel.writeInbound(byteBufOf("I")));
            assertFalse(channel.writeInbound(byteBufOf("N")));
            assertFalse(channel.writeInbound(byteBufOf("G")));
            assertTrue(channel.writeInbound(byteBufOf("\r\n")));

            channel.readInbound();
        });
    }

    @Test
    public void shouldDecodeInlineCommand() {
        channel = newChannel(true);

        assertFalse(channel.writeInbound(byteBufOf("P")));
        assertFalse(channel.writeInbound(byteBufOf("I")));
        assertFalse(channel.writeInbound(byteBufOf("N")));
        assertFalse(channel.writeInbound(byteBufOf("G")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        InlineCommandRedisMessage msg = channel.readInbound();

        assertThat(msg.content()).isEqualTo("PING");

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeSimpleString() {
        assertFalse(channel.writeInbound(byteBufOf("+")));
        assertFalse(channel.writeInbound(byteBufOf("O")));
        assertFalse(channel.writeInbound(byteBufOf("K")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        SimpleStringRedisMessage msg = channel.readInbound();

        assertThat(msg.content()).isEqualTo("OK");

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeTwoSimpleStrings() {
        assertFalse(channel.writeInbound(byteBufOf("+")));
        assertFalse(channel.writeInbound(byteBufOf("O")));
        assertFalse(channel.writeInbound(byteBufOf("K")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n+SEC")));
        assertTrue(channel.writeInbound(byteBufOf("OND\r\n")));

        SimpleStringRedisMessage msg1 = channel.readInbound();
        assertThat(msg1.content()).isEqualTo("OK");
        ReferenceCountUtil.release(msg1);

        SimpleStringRedisMessage msg2 = channel.readInbound();
        assertThat(msg2.content()).isEqualTo("SECOND");
        ReferenceCountUtil.release(msg2);
    }

    @Test
    public void shouldDecodeError() {
        String content = "ERROR sample message";
        assertFalse(channel.writeInbound(byteBufOf("-")));
        assertFalse(channel.writeInbound(byteBufOf(content)));
        assertFalse(channel.writeInbound(byteBufOf("\r")));
        assertTrue(channel.writeInbound(byteBufOf("\n")));

        ErrorRedisMessage msg = channel.readInbound();

        assertThat(msg.content()).isEqualTo(content);

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeInteger() {
        long value = 1234L;
        byte[] content = bytesOf(value);
        assertFalse(channel.writeInbound(byteBufOf(":")));
        assertFalse(channel.writeInbound(byteBufOf(content)));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        IntegerRedisMessage msg = channel.readInbound();

        assertThat(msg.value()).isEqualTo(value);

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeBulkString() {
        String buf1 = "bulk\nst";
        String buf2 = "ring\ntest\n1234";
        byte[] content = bytesOf(buf1 + buf2);
        assertFalse(channel.writeInbound(byteBufOf("$")));
        assertFalse(channel.writeInbound(byteBufOf(Integer.toString(content.length))));
        assertFalse(channel.writeInbound(byteBufOf("\r\n")));
        assertFalse(channel.writeInbound(byteBufOf(buf1)));
        assertFalse(channel.writeInbound(byteBufOf(buf2)));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        FullBulkStringRedisMessage msg = channel.readInbound();

        assertThat(bytesOf(msg.content())).isEqualTo(content);

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeEmptyBulkString() {
        byte[] content = bytesOf("");
        assertFalse(channel.writeInbound(byteBufOf("$")));
        assertFalse(channel.writeInbound(byteBufOf(Integer.toString(content.length))));
        assertFalse(channel.writeInbound(byteBufOf("\r\n")));
        assertFalse(channel.writeInbound(byteBufOf(content)));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        FullBulkStringRedisMessage msg = channel.readInbound();

        assertThat(bytesOf(msg.content())).isEqualTo(content);

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeNullBulkString() {
        assertFalse(channel.writeInbound(byteBufOf("$")));
        assertFalse(channel.writeInbound(byteBufOf(Integer.toString(-1))));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        assertTrue(channel.writeInbound(byteBufOf("$")));
        assertTrue(channel.writeInbound(byteBufOf(Integer.toString(-1))));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        FullBulkStringRedisMessage msg1 = channel.readInbound();
        assertThat(msg1.isNull()).isTrue();
        ReferenceCountUtil.release(msg1);

        FullBulkStringRedisMessage msg2 = channel.readInbound();
        assertThat(msg2.isNull()).isTrue();
        ReferenceCountUtil.release(msg2);

        FullBulkStringRedisMessage msg3 = channel.readInbound();
        assertThat(msg3).isNull();
    }

    @Test
    public void shouldDecodeSimpleArray() throws Exception {
        assertFalse(channel.writeInbound(byteBufOf("*3\r\n")));
        assertFalse(channel.writeInbound(byteBufOf(":1234\r\n")));
        assertFalse(channel.writeInbound(byteBufOf("+sim")));
        assertFalse(channel.writeInbound(byteBufOf("ple\r\n-err")));
        assertTrue(channel.writeInbound(byteBufOf("or\r\n")));

        ArrayRedisMessage msg = channel.readInbound();
        List<RedisMessage> children = msg.children();

        assertThat(msg.children().size()).isEqualTo(3);

        assertThat(children.get(0)).isInstanceOf(IntegerRedisMessage.class);
        assertThat(((IntegerRedisMessage) children.get(0)).value()).isEqualTo(1234L);
        assertThat(children.get(1)).isInstanceOf(SimpleStringRedisMessage.class);
        assertThat(((SimpleStringRedisMessage) children.get(1)).content()).isEqualTo("simple");
        assertThat(children.get(2)).isInstanceOf(ErrorRedisMessage.class);
        assertThat(((ErrorRedisMessage) children.get(2)).content()).isEqualTo("error");

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeNestedArray() throws Exception {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(byteBufOf("*2\r\n"));
        buf.writeBytes(byteBufOf("*3\r\n:1\r\n:2\r\n:3\r\n"));
        buf.writeBytes(byteBufOf("*2\r\n+Foo\r\n-Bar\r\n"));
        assertTrue(channel.writeInbound(buf));

        ArrayRedisMessage msg = channel.readInbound();
        List<RedisMessage> children = msg.children();

        assertThat(msg.children().size()).isEqualTo(2);

        ArrayRedisMessage intArray = (ArrayRedisMessage) children.get(0);
        ArrayRedisMessage strArray = (ArrayRedisMessage) children.get(1);

        assertThat(intArray.children().size()).isEqualTo(3);
        assertThat(((IntegerRedisMessage) intArray.children().get(0)).value()).isEqualTo(1L);
        assertThat(((IntegerRedisMessage) intArray.children().get(1)).value()).isEqualTo(2L);
        assertThat(((IntegerRedisMessage) intArray.children().get(2)).value()).isEqualTo(3L);

        assertThat(strArray.children().size()).isEqualTo(2);
        assertThat(((SimpleStringRedisMessage) strArray.children().get(0)).content()).isEqualTo("Foo");
        assertThat(((ErrorRedisMessage) strArray.children().get(1)).content()).isEqualTo("Bar");

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldErrorOnDoubleReleaseArrayReferenceCounted() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(byteBufOf("*2\r\n"));
        buf.writeBytes(byteBufOf("*3\r\n:1\r\n:2\r\n:3\r\n"));
        buf.writeBytes(byteBufOf("*2\r\n+Foo\r\n-Bar\r\n"));
        assertTrue(channel.writeInbound(buf));

        ArrayRedisMessage msg = channel.readInbound();

        ReferenceCountUtil.release(msg);
        assertThrows(IllegalReferenceCountException.class, () -> ReferenceCountUtil.release(msg));
    }

    @Test
    public void shouldErrorOnReleaseArrayChildReferenceCounted() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(byteBufOf("*2\r\n"));
        buf.writeBytes(byteBufOf("*3\r\n:1\r\n:2\r\n:3\r\n"));
        buf.writeBytes(byteBufOf("$3\r\nFoo\r\n"));
        assertTrue(channel.writeInbound(buf));

        ArrayRedisMessage msg = channel.readInbound();

        List<RedisMessage> children = msg.children();
        ReferenceCountUtil.release(msg);
        assertThrows(IllegalReferenceCountException.class, () -> ReferenceCountUtil.release(children.get(1)));
    }

    @Test
    public void shouldErrorOnReleaseContentOfArrayChildReferenceCounted() throws Exception {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(byteBufOf("*2\r\n"));
        buf.writeBytes(byteBufOf("$3\r\nFoo\r\n$3\r\nBar\r\n"));
        assertTrue(channel.writeInbound(buf));

        ArrayRedisMessage msg = channel.readInbound();

        List<RedisMessage> children = msg.children();
        ByteBuf childBuf = ((FullBulkStringRedisMessage) children.get(0)).content();
        ReferenceCountUtil.release(msg);
        assertThrows(IllegalReferenceCountException.class, () -> ReferenceCountUtil.release(childBuf));
    }

    @Test
    public void testPredefinedMessagesNotEqual() {
        // both EMPTY_INSTANCE and NULL_INSTANCE have EMPTY_BUFFER as their 'data',
        // however we need to check that they are not equal between themselves.
        assertNotEquals(FullBulkStringRedisMessage.EMPTY_INSTANCE, FullBulkStringRedisMessage.NULL_INSTANCE);
        assertNotEquals(FullBulkStringRedisMessage.NULL_INSTANCE, FullBulkStringRedisMessage.EMPTY_INSTANCE);
    }

    @Test
    public void shouldDecodeNull() {
        assertFalse(channel.writeInbound(byteBufOf("_")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        RedisMessage msg = channel.readInbound();

        assertTrue(msg instanceof NullRedisMessage);

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeBoolean() {
        assertFalse(channel.writeInbound(byteBufOf("#")));
        assertFalse(channel.writeInbound(byteBufOf("t")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        assertTrue(channel.writeInbound(byteBufOf("#f\r\n")));

        BooleanRedisMessage msgTrue = channel.readInbound();
        assertTrue(msgTrue.value());
        ReferenceCountUtil.release(msgTrue);

        BooleanRedisMessage msgFalse = channel.readInbound();
        assertFalse(msgFalse.value());
        ReferenceCountUtil.release(msgFalse);
    }

    @Test
    public void shouldDecodeDouble() {
        assertFalse(channel.writeInbound(byteBufOf(",")));
        assertFalse(channel.writeInbound(byteBufOf("1.23")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        DoubleRedisMessage msg = channel.readInbound();
        assertThat(msg.value()).isEqualTo(1.23d);

        assertTrue(channel.writeInbound(byteBufOf(",-1.23\r\n")));

        msg = channel.readInbound();
        assertThat(msg.value()).isEqualTo(-1.23d);

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeInfinityDouble() {
        assertFalse(channel.writeInbound(byteBufOf(",inf")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));
        assertTrue(channel.writeInbound(byteBufOf(",-inf\r\n")));

        DoubleRedisMessage msg = channel.readInbound();
        assertThat(msg.value()).isEqualTo(Double.MAX_VALUE);

        msg = channel.readInbound();
        assertThat(msg.value()).isEqualTo(Double.MIN_VALUE);

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldErrorOnDecodeDoubleOnNotValidRepresentation() {
        assertThrows(DecoderException.class, new Executable() {
            @Override
            public void execute() {
                assertFalse(channel.writeInbound(byteBufOf(",-1.23a")));
                assertTrue(channel.writeInbound(byteBufOf("\r\n")));
                BigNumberRedisMessage msg = channel.readInbound();
                ReferenceCountUtil.release(msg);
            }
        });
    }

    @Test
    public void shouldDecodeBigNumber() {
        assertFalse(channel.writeInbound(byteBufOf("(3492890328409238509324850943850943825024385")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        BigNumberRedisMessage msg = channel.readInbound();
        assertThat(msg.value()).isEqualTo("3492890328409238509324850943850943825024385");

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldErrorOnDecodeBigNumberOnNotValidRepresentation() {
        assertThrows(DecoderException.class, new Executable() {
            @Override
            public void execute() {
                assertFalse(channel.writeInbound(byteBufOf("(3492890328409238509324850943850943825024385")));
                assertFalse(channel.writeInbound(byteBufOf("Error")));
                assertTrue(channel.writeInbound(byteBufOf("\r\n")));
                BigNumberRedisMessage msg = channel.readInbound();
                ReferenceCountUtil.release(msg);
            }
        });
    }

    @Test
    public void shouldDecodeBulkErrorString() {
        String buf1 = "bulk\nst";
        String buf2 = "ring\ntest\n1234";
        byte[] content = bytesOf(buf1 + buf2);
        assertFalse(channel.writeInbound(byteBufOf("!")));
        assertFalse(channel.writeInbound(byteBufOf(Integer.toString(content.length))));
        assertFalse(channel.writeInbound(byteBufOf("\r\n")));
        assertFalse(channel.writeInbound(byteBufOf(buf1)));
        assertFalse(channel.writeInbound(byteBufOf(buf2)));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        FullBulkErrorStringRedisMessage msg = channel.readInbound();

        assertThat(bytesOf(msg.content())).isEqualTo(content);

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeBulkVerbatimString() {
        String buf1 = "txt:bulk\nst";
        String buf2 = "ring\ntest\n1234";
        byte[] content = bytesOf(buf1 + buf2);
        assertFalse(channel.writeInbound(byteBufOf("=")));
        assertFalse(channel.writeInbound(byteBufOf(Integer.toString(content.length))));
        assertFalse(channel.writeInbound(byteBufOf("\r\n")));
        assertFalse(channel.writeInbound(byteBufOf(buf1)));
        assertFalse(channel.writeInbound(byteBufOf(buf2)));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        FullBulkVerbatimStringRedisMessage msg = channel.readInbound();

        assertThat(bytesOf(msg.content())).isEqualTo(content);
        assertThat(msg.format()).isEqualTo("txt");
        assertThat(msg.realContent()).isEqualTo("bulk\nstring\ntest\n1234");

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeSet() {
        assertFalse(channel.writeInbound(byteBufOf("~3\r\n")));
        assertFalse(channel.writeInbound(byteBufOf(":1234\r\n")));
        assertFalse(channel.writeInbound(byteBufOf("+sim")));
        assertFalse(channel.writeInbound(byteBufOf("ple\r\n-err")));
        assertTrue(channel.writeInbound(byteBufOf("or\r\n")));

        SetRedisMessage msg = channel.readInbound();
        Set<RedisMessage> children = msg.children();

        assertThat(children.size()).isEqualTo(3);
        for (RedisMessage child : children) {
            if (child instanceof IntegerRedisMessage) {
                assertThat(((IntegerRedisMessage) child).value()).isEqualTo(1234L);
            } else if (child instanceof SimpleStringRedisMessage) {
                assertThat(((SimpleStringRedisMessage) child).content()).isEqualTo("simple");
            } else if (child instanceof ErrorRedisMessage) {
                assertThat(((ErrorRedisMessage) child).content()).isEqualTo("error");
            } else {
                fail("Unexpected types");
            }
        }

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeEmptySet() {
        assertTrue(channel.writeInbound(byteBufOf("~0\r\n")));

        RedisMessage msg = channel.readInbound();

        assertTrue(msg instanceof SetRedisMessage);
        assertThat(((SetRedisMessage) msg).children().size()).isEqualTo(0);

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeMap() {
        assertFalse(channel.writeInbound(byteBufOf("%2\r\n")));
        assertFalse(channel.writeInbound(byteBufOf("+first\r\n")));
        assertFalse(channel.writeInbound(byteBufOf(":1\r\n")));
        assertFalse(channel.writeInbound(byteBufOf("+second\r\n-err")));
        assertTrue(channel.writeInbound(byteBufOf("or\r\n")));

        MapRedisMessage msg = channel.readInbound();
        Map<RedisMessage, RedisMessage> children = msg.children();

        assertThat(children.size()).isEqualTo(2);

        for (Map.Entry<RedisMessage, RedisMessage> messageEntry : children.entrySet()) {
            assertThat(messageEntry.getKey()).isInstanceOf(SimpleStringRedisMessage.class);
            String key = ((SimpleStringRedisMessage) messageEntry.getKey()).content();
            if ("first".equals(key)) {
                assertThat(messageEntry.getValue()).isInstanceOf(IntegerRedisMessage.class);
                assertThat(((IntegerRedisMessage) messageEntry.getValue()).value()).isEqualTo(1L);
            } else if ("second".equals(key)) {
                assertThat(messageEntry.getValue()).isInstanceOf(ErrorRedisMessage.class);
                assertThat(((ErrorRedisMessage) messageEntry.getValue()).content()).isEqualTo("error");
            } else {
                fail("Unexpected key");
            }
        }

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeEmptyMap() {
        assertTrue(channel.writeInbound(byteBufOf("%0\r\n")));

        RedisMessage msg = channel.readInbound();

        assertTrue(msg instanceof MapRedisMessage);
        assertThat(((MapRedisMessage) msg).children().size()).isEqualTo(0);

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldNotBeDecodedMapOnNotEnoughMessage() {
        assertFalse(channel.writeInbound(byteBufOf("%2\r\n")));
        assertFalse(channel.writeInbound(byteBufOf("+first\r\n")));
        assertFalse(channel.writeInbound(byteBufOf(":1\r\n")));
        assertFalse(channel.writeInbound(byteBufOf("+second\r\n")));

        MapRedisMessage msg = channel.readInbound();

        assertThat(msg).isNull();
    }

    @Test
    public void shouldDecodePush() throws Exception {
        assertFalse(channel.writeInbound(byteBufOf(">3\r\n")));
        assertFalse(channel.writeInbound(byteBufOf(":1234\r\n")));
        assertFalse(channel.writeInbound(byteBufOf("+sim")));
        assertFalse(channel.writeInbound(byteBufOf("ple\r\n-err")));
        assertTrue(channel.writeInbound(byteBufOf("or\r\n")));

        PushRedisMessage msg = channel.readInbound();
        List<RedisMessage> children = msg.children();

        assertThat(msg.children().size()).isEqualTo(3);

        assertThat(children.get(0)).isInstanceOf(IntegerRedisMessage.class);
        assertThat(((IntegerRedisMessage) children.get(0)).value()).isEqualTo(1234L);
        assertThat(children.get(1)).isInstanceOf(SimpleStringRedisMessage.class);
        assertThat(((SimpleStringRedisMessage) children.get(1)).content()).isEqualTo("simple");
        assertThat(children.get(2)).isInstanceOf(ErrorRedisMessage.class);
        assertThat(((ErrorRedisMessage) children.get(2)).content()).isEqualTo("error");

        ReferenceCountUtil.release(msg);
    }

}
