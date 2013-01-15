/**
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 */

package org.mule.module.redis;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mule.api.callback.SourceCallback;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.util.SafeEncoder;

public final class RedisPubSubListener extends BinaryJedisPubSub {
    private static final Log LOGGER = LogFactory.getLog(RedisPubSubListener.class);

    private final SourceCallback callback;

    public RedisPubSubListener(final SourceCallback callback) {
        super();
        this.callback = callback;
    }

    @Override
    public void onSubscribe(final byte[] channel, final int subscribedChannels) {
        LOGGER.info("Subscribed to channel: " + SafeEncoder.encode(channel));
    }

    @Override
    public void onPSubscribe(final byte[] pattern, final int subscribedChannels) {
        LOGGER.info("Subscribed to pattern: " + SafeEncoder.encode(pattern));
    }

    @Override
    public void onUnsubscribe(final byte[] channel, final int subscribedChannels) {
        LOGGER.info("Unsubscribed from channel: " + SafeEncoder.encode(channel));
    }

    @Override
    public void onPUnsubscribe(final byte[] pattern, final int subscribedChannels) {
        LOGGER.info("Unubscribed from pattern: " + SafeEncoder.encode(pattern));
    }

    @Override
    public void onPMessage(final byte[] pattern, final byte[] channel, final byte[] message) {
        final Map<String, Object> props = new HashMap<String, Object>();
        props.put(RedisConstants.REDIS_PUBSUB_PATTERN, SafeEncoder.encode(pattern));
        props.put(RedisConstants.REDIS_PUBSUB_CHANNEL, SafeEncoder.encode(channel));
        deliver(message, props);
    }

    @Override
    public void onMessage(final byte[] channel, final byte[] message) {
        deliver(message, Collections.singletonMap(RedisConstants.REDIS_PUBSUB_CHANNEL, (Object) SafeEncoder.encode(channel)));
    }

    private void deliver(final Object payload, final Map<String, Object> properties) {
        try {
            callback.process(payload, properties);
        } catch (final Exception e) {
            LOGGER.error("Failed to deliver: " + payload + " [" + properties + "]", e);
        }
    }
}