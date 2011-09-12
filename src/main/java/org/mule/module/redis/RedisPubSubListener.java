/*
 * $Id$
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.module.redis;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mule.DefaultMuleMessage;
import org.mule.api.MuleContext;
import org.mule.api.annotations.callback.SourceCallback;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.util.SafeEncoder;

public final class RedisPubSubListener extends BinaryJedisPubSub {
    private static final Log LOGGER = LogFactory.getLog(RedisPubSubListener.class);

    private final MuleContext muleContext;
    private final SourceCallback callback;

    public RedisPubSubListener(final MuleContext muleContext, final SourceCallback callback) {
        super();
        this.muleContext = muleContext;
        this.callback = callback;
    }

    @Override
    public void onSubscribe(final byte[] channel, final int subscribedChannels) {
        LOGGER.info("Subscribed to channel: " + SafeEncoder.encode(channel));
    }

    @Override
    public void onPSubscribe(final byte[] pattern, final int subscribedChannels) {
        LOGGER.info("Subscribed from pattern: " + SafeEncoder.encode(pattern));
    }

    @Override
    public void onUnsubscribe(final byte[] channel, final int subscribedChannels) {
        LOGGER.info("Unsubscribed from channel: " + SafeEncoder.encode(channel));
    }

    @Override
    public void onPUnsubscribe(final byte[] pattern, final int subscribedChannels) {
        LOGGER.info("Unubscribed to pattern: " + SafeEncoder.encode(pattern));
    }

    @Override
    public void onPMessage(final byte[] pattern, final byte[] channel, final byte[] message) {
        final Map<String, Object> props = new HashMap<String, Object>();
        props.put(RedisConstants.REDIS_PUBSUB_PATTERN, SafeEncoder.encode(pattern));
        props.put(RedisConstants.REDIS_PUBSUB_CHANNEL, SafeEncoder.encode(channel));
        final DefaultMuleMessage muleMessage = new DefaultMuleMessage(message, props, muleContext);
        deliver(muleMessage);
    }

    @Override
    public void onMessage(final byte[] channel, final byte[] message) {
        final DefaultMuleMessage muleMessage = new DefaultMuleMessage(message, Collections.singletonMap(
                RedisConstants.REDIS_PUBSUB_CHANNEL, (Object) SafeEncoder.encode(channel)), muleContext);
        deliver(muleMessage);
    }

    private void deliver(final DefaultMuleMessage muleMessage) {
        try {
            callback.process(muleMessage);
        } catch (final Exception e) {
            LOGGER.error("Failed to deliver: " + muleMessage, e);
        }
    }
}