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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.mule.RequestContext;
import org.mule.api.MuleContext;
import org.mule.api.MuleException;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.Module;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.Source;
import org.mule.api.annotations.callback.SourceCallback;
import org.mule.api.annotations.param.Default;
import org.mule.api.annotations.param.Optional;
import org.mule.api.context.MuleContextAware;
import org.mule.api.store.ObjectAlreadyExistsException;
import org.mule.api.store.ObjectDoesNotExistException;
import org.mule.api.store.ObjectStoreException;
import org.mule.api.store.PartitionableObjectStore;
import org.mule.config.i18n.MessageFactory;
import org.mule.module.redis.RedisUtils.RedisAction;

import redis.clients.jedis.BinaryTransaction;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Response;
import redis.clients.util.SafeEncoder;

@Module(name = "redis", namespace = "http://www.mulesoft.org/schema/mule/redis", schemaLocation = "http://www.mulesoft.org/schema/mule/redis/3.2/mule-redis.xsd")
public class RedisModule implements PartitionableObjectStore<Serializable>, MuleContextAware {
    private static final String DEFAULT_PARTITION_NAME = "_default";

    private static final Log LOGGER = LogFactory.getLog(RedisModule.class);

    @Configurable
    @Optional
    @Default("localhost")
    private String host;

    @Configurable
    @Optional
    @Default("6379")
    private int port;

    @Configurable
    @Optional
    @Default("2000")
    private int connectionTimeout;

    @Configurable
    @Optional
    private String password;

    @Configurable
    @Optional
    private Config poolConfig = new JedisPoolConfig();

    private MuleContext muleContext;
    private JedisPool jedisPool;

    /*----------------------------------------------------------
                Lifecycle Implementation
    ----------------------------------------------------------*/
    public void setMuleContext(final MuleContext muleContext) {
        this.muleContext = muleContext;
    }

    @PostConstruct
    public void initializeJedis() {
        jedisPool = new JedisPool(poolConfig, host, port, connectionTimeout, password);

        LOGGER.info(String.format("Redis connector ready, host: %s, port: %d, timeout: %d, password: %s, pool config: %s", host, port,
                connectionTimeout, StringUtils.repeat("*", StringUtils.length(password)),
                ToStringBuilder.reflectionToString(poolConfig, ToStringStyle.SHORT_PREFIX_STYLE)));
    }

    @PreDestroy
    public void destroyJedis() {
        jedisPool.destroy();
        LOGGER.info("Redis connector terminated");
    }

    /*----------------------------------------------------------
                Datastructure Commands
    ----------------------------------------------------------*/
    @Processor
    public byte[] set(final String key, @Optional final Integer expire, @Optional @Default("false") final boolean ifNotExists)
            throws Exception {

        final byte[] message = RequestContext.getEvent().getMessageAsBytes();

        return RedisUtils.run(jedisPool, new RedisAction<byte[]>() {
            @Override
            public byte[] run() {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                byte[] result = message;

                if (ifNotExists) {
                    if (redis.setnx(keyAsBytes, message) == 0) {
                        result = null;
                    }
                } else {
                    redis.set(keyAsBytes, message);
                }

                if (expire != null) {
                    redis.expire(keyAsBytes, expire);
                }

                return result;
            }
        });
    }

    @Processor
    public byte[] get(final String key) {
        return RedisUtils.run(jedisPool, new RedisAction<byte[]>() {
            @Override
            public byte[] run() {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                return redis.get(keyAsBytes);
            }
        });
    }

    @Processor(name = "hash-set")
    public byte[] setInHash(final String key, final String field, @Optional @Default("false") final boolean ifNotExists)
            throws MuleException {

        final byte[] message = RequestContext.getEvent().getMessageAsBytes();

        return RedisUtils.run(jedisPool, new RedisAction<byte[]>() {
            @Override
            public byte[] run() {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                final byte[] fieldAsBytes = SafeEncoder.encode(field);
                byte[] result = message;

                if (ifNotExists) {
                    if (redis.hsetnx(keyAsBytes, fieldAsBytes, message) == 0) {
                        result = null;
                    }
                } else {
                    redis.hset(keyAsBytes, fieldAsBytes, message);
                }

                return result;
            }
        });
    }

    @Processor(name = "hash-get")
    public byte[] getFromHash(final String key, final String field) {
        return RedisUtils.run(jedisPool, new RedisAction<byte[]>() {
            @Override
            public byte[] run() {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                final byte[] fieldAsBytes = SafeEncoder.encode(field);
                return redis.hget(keyAsBytes, fieldAsBytes);
            }
        });
    }

    /*----------------------------------------------------------
                Pub/Sub Implementation
    ----------------------------------------------------------*/
    @Processor
    public void publish(final String channel) throws Exception {
        final byte[] message = RequestContext.getEvent().getMessageAsBytes();
        RedisUtils.run(jedisPool, new RedisAction<Long>() {
            @Override
            public Long run() {
                // this blocks until Redis gets disconnected
                return redis.publish(SafeEncoder.encode(channel), message);
            }
        });
    }

    @Source
    public void subscribe(final List<String> channels, final SourceCallback callback) {
        final RedisPubSubListener listener = RedisUtils.run(jedisPool, new RedisAction<RedisPubSubListener>() {
            @Override
            public RedisPubSubListener run() {
                // this blocks until Redis gets disconnected
                final RedisPubSubListener listener = new RedisPubSubListener(muleContext, callback);
                redis.psubscribe(listener, RedisUtils.getPatternsFromChannels(channels));
                return listener;
            }
        });

        listener.punsubscribe();
    }

    /*----------------------------------------------------------
                ObjectStore Implementation
    ----------------------------------------------------------*/
    public boolean isPersistent() {
        return true;
    }

    public boolean contains(final Serializable key) throws ObjectStoreException {
        return contains(key, DEFAULT_PARTITION_NAME);
    }

    public void store(final Serializable key, final Serializable value) throws ObjectStoreException {
        store(key, value, DEFAULT_PARTITION_NAME);
    }

    public Serializable retrieve(final Serializable key) throws ObjectStoreException {
        return retrieve(key, DEFAULT_PARTITION_NAME);
    }

    public Serializable remove(final Serializable key) throws ObjectStoreException {
        return remove(key, DEFAULT_PARTITION_NAME);
    }

    /*----------------------------------------------------------
               ListableObjectStore Implementation
    ----------------------------------------------------------*/
    public void open() throws ObjectStoreException {
        open(DEFAULT_PARTITION_NAME);
    }

    public void close() throws ObjectStoreException {
        close(DEFAULT_PARTITION_NAME);
    }

    public List<Serializable> allKeys() throws ObjectStoreException {
        return allKeys(DEFAULT_PARTITION_NAME);
    }

    /*----------------------------------------------------------
             PartitionableObjectStore Implementation
    ----------------------------------------------------------*/
    public boolean contains(final Serializable key, final String partitionName) throws ObjectStoreException {
        return RedisUtils.run(jedisPool, new RedisAction<Boolean>() {
            @Override
            public Boolean run() {
                return redis.hexists(RedisUtils.getPartitionHashKey(partitionName), RedisUtils.toBytes(key));
            }
        });
    }

    public void store(final Serializable key, final Serializable value, final String partitionName) throws ObjectStoreException {
        final Long result = RedisUtils.run(jedisPool, new RedisAction<Long>() {
            @Override
            public Long run() {
                return redis.hsetnx(RedisUtils.getPartitionHashKey(partitionName), RedisUtils.toBytes(key), RedisUtils.toBytes(value));
            }
        });

        if (result == 0) {
            throw new ObjectAlreadyExistsException(MessageFactory.createStaticMessage("There is already a value for: " + key));
        }
    }

    public Serializable retrieve(final Serializable key, final String partitionName) throws ObjectStoreException {
        final Serializable result = RedisUtils.run(jedisPool, new RedisAction<Serializable>() {
            @Override
            public Serializable run() {
                return RedisUtils.fromBytes(redis.hget(RedisUtils.getPartitionHashKey(partitionName), RedisUtils.toBytes(key)));
            }
        });

        if (result == null) {
            throw new ObjectDoesNotExistException(MessageFactory.createStaticMessage("No value found for key: " + key));
        }

        return result;
    }

    public Serializable remove(final Serializable key, final String partitionName) throws ObjectStoreException {
        final Serializable result = RedisUtils.run(jedisPool, new RedisAction<Serializable>() {
            @Override
            public Serializable run() {
                final byte[] keyAsBytes = RedisUtils.toBytes(key);

                final BinaryTransaction t = redis.multi();
                final Response<byte[]> getResult = t.hget(RedisUtils.getPartitionHashKey(partitionName), keyAsBytes);
                final Response<Long> delResult = t.hdel(RedisUtils.getPartitionHashKey(partitionName), keyAsBytes);
                t.exec();

                if (delResult.get() != 1) {
                    return null;
                }

                return RedisUtils.fromBytes(getResult.get());
            }
        });

        if (result == null) {
            throw new ObjectDoesNotExistException(MessageFactory.createStaticMessage("No value found for key: " + key));
        }

        return result;
    }

    public List<Serializable> allKeys(final String partitionName) throws ObjectStoreException {
        return RedisUtils.run(jedisPool, new RedisAction<List<Serializable>>() {
            @Override
            public List<Serializable> run() {
                final List<Serializable> keys = new ArrayList<Serializable>();
                for (final byte[] key : redis.hkeys(RedisUtils.getPartitionHashKey(partitionName))) {
                    keys.add(RedisUtils.fromBytes(key));
                }
                return keys;
            }
        });
    }

    public List<String> allPartitions() throws ObjectStoreException {
        return RedisUtils.run(jedisPool, new RedisAction<List<String>>() {
            @Override
            public List<String> run() {
                final List<String> partitions = new ArrayList<String>();
                final Set<byte[]> keys = redis.keys((RedisConstants.OBJECTSTORE_HASH_KEY_PREFIX + "*").getBytes());
                for (final byte[] key : keys) {
                    final String partition = StringUtils.substringAfter(SafeEncoder.encode(key), RedisConstants.OBJECTSTORE_HASH_KEY_PREFIX);
                    partitions.add(partition);
                }
                return partitions;
            }
        });
    }

    public void open(final String partitionName) throws ObjectStoreException {
        // ignored
    }

    public void close(final String partitionName) throws ObjectStoreException {
        // ignored
    }

    public void disposePartition(final String partitionName) throws ObjectStoreException {
        RedisUtils.run(jedisPool, new RedisAction<Long>() {
            @Override
            public Long run() {
                return redis.del(RedisUtils.getPartitionHashKey(partitionName));
            }
        });
    }

    /*----------------------------------------------------------
                        Java Accessors Gong Show
     ----------------------------------------------------------*/
    public String getHost() {
        return host;
    }

    public void setHost(final String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(final int port) {
        this.port = port;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(final int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(final String password) {
        this.password = password;
    }

    public Config getPoolConfig() {
        return poolConfig;
    }

    public void setPoolConfig(final Config poolConfig) {
        this.poolConfig = poolConfig;
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }
}
