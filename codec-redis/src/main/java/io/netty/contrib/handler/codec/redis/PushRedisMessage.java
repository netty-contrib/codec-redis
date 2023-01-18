/*
 * Copyright 2021 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.netty.contrib.handler.codec.redis;

import io.netty.util.internal.UnstableApi;

import java.util.Collections;
import java.util.List;

@UnstableApi
public final class PushRedisMessage extends AbstractCollectionRedisMessage {

    private PushRedisMessage() {
        super(Collections.<RedisMessage>emptySet());
    }

    /**
     * Creates a {@link PushRedisMessage} for the given {@code content}.
     *
     * @param children the children.
     */
    public PushRedisMessage(List<RedisMessage> children) {
        super(children);
    }

    /**
     * Get children of this Set. It can be null or empty.
     *
     * @return List of {@link RedisMessage}s.
     */
    @Override
    public List<RedisMessage> children() {
        return (List<RedisMessage>) children;
    }
}
