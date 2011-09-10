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

import java.io.ObjectStreamConstants;
import java.io.Serializable;

import org.apache.commons.lang.SerializationUtils;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.util.SafeEncoder;

public abstract class RedisUtils {
    public static final String REDIS_HASH_KEY_PREFIX = "mule.objectstore.";

    public static abstract class RedisAction<R> {
        protected volatile BinaryJedis redis;

        R runWithJedis(final Jedis jedis) {
            redis = jedis;
            return run();
        }

        public abstract R run();
    }

    private RedisUtils() {
        throw new UnsupportedOperationException("do not instantiate");
    }

    public static byte[] toBytes(final Serializable serializable) {
        if (serializable == null) {
            return null;
        }

        // preserve strings if possible
        if (serializable instanceof String) {
            return SafeEncoder.encode((String) serializable);
        }
        // serialize anything that isn't a string
        return SerializationUtils.serialize(serializable);
    }

    public static Serializable fromBytes(final byte[] bytes) {
        if ((bytes == null) || (bytes.length == 0)) {
            return null;
        }

        if ((bytes[0] == (byte) ((ObjectStreamConstants.STREAM_MAGIC >>> 8) & 0xFF))) {
            final Object deserialized = SerializationUtils.deserialize(bytes);
            if (deserialized instanceof Serializable) {
                return (Serializable) deserialized;
            } else {
                return bytes;
            }
        } else {
            return SafeEncoder.encode(bytes);
        }
    }

    public static byte[] hashKey(final String partitionName) {
        return SafeEncoder.encode(REDIS_HASH_KEY_PREFIX + partitionName);
    }

    public static <R> R run(final JedisPool jedisPool, final RedisAction<R> action) {
        final Jedis jedis = jedisPool.getResource();
        try {
            return action.runWithJedis(jedis);
        } finally {
            jedisPool.returnResource(jedis);
        }

    }
}
