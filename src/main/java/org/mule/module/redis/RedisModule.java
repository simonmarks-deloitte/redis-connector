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

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.Module;
import org.mule.api.annotations.param.Default;
import org.mule.api.annotations.param.Optional;
import org.mule.api.store.ObjectAlreadyExistsException;
import org.mule.api.store.ObjectDoesNotExistException;
import org.mule.api.store.ObjectStore;
import org.mule.api.store.ObjectStoreException;
import org.mule.config.i18n.MessageFactory;
import org.mule.module.redis.RedisUtils.RedisAction;

import redis.clients.jedis.BinaryTransaction;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;

@Module(name = "redis", namespace = "http://www.mulesoft.org/schema/mule/redis", schemaLocation = "http://www.mulesoft.org/schema/mule/redis/3.2/mule-redis.xsd")
public class RedisModule implements ObjectStore<Serializable> {
    protected static final Log LOGGER = LogFactory.getLog(RedisModule.class);

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
    private int timeout;

    @Configurable
    @Optional
    private String password;

    @Configurable
    @Optional
    private Config poolConfig = new Config();

    private JedisPool jedisPool;

    /*
     * Lifecycle
     */
    @PostConstruct
    public void initializeJedis() {
        jedisPool = new JedisPool(poolConfig, host, port, timeout, password);

        LOGGER.info(String.format("Redis connector ready, host: %s, port: %d, timeout: %d, password: %s, pool config: %s", host, port,
                timeout, StringUtils.repeat("*", StringUtils.length(password)),
                ToStringBuilder.reflectionToString(poolConfig, ToStringStyle.SHORT_PREFIX_STYLE)));
    }

    @PreDestroy
    public void destroyJedis() {
        jedisPool.destroy();
        LOGGER.info("Redis connector terminated");
    }

    /*
     * Object Store Implementation
     */
    public boolean isPersistent() {
        return true;
    }

    public boolean contains(final Serializable key) throws ObjectStoreException {
        return RedisUtils.run(jedisPool, new RedisAction<Boolean>() {
            @Override
            public Boolean run() {
                return redis.exists(RedisUtils.toBytes(key));
            }
        });
    }

    public void store(final Serializable key, final Serializable value) throws ObjectStoreException {
        final Long result = RedisUtils.run(jedisPool, new RedisAction<Long>() {
            @Override
            public Long run() {
                return redis.setnx(RedisUtils.toBytes(key), RedisUtils.toBytes(value));
            }
        });

        if (result == 0) {
            throw new ObjectAlreadyExistsException(MessageFactory.createStaticMessage("There is already a value for: " + key));
        }
    }

    public Serializable retrieve(final Serializable key) throws ObjectStoreException {
        final Serializable result = RedisUtils.run(jedisPool, new RedisAction<Serializable>() {
            @Override
            public Serializable run() {
                return RedisUtils.fromBytes(redis.get(RedisUtils.toBytes(key)));
            }
        });

        if (result == null) {
            throw new ObjectDoesNotExistException(MessageFactory.createStaticMessage("No value found for key: " + key));
        }

        return result;
    }

    public Serializable remove(final Serializable key) throws ObjectStoreException {
        final Serializable result = RedisUtils.run(jedisPool, new RedisAction<Serializable>() {
            @Override
            public Serializable run() {
                final byte[] keyAsBytes = RedisUtils.toBytes(key);

                final BinaryTransaction t = redis.multi();
                final Response<byte[]> getResult = t.get(keyAsBytes);
                final Response<Long> delResult = t.del(keyAsBytes);
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

    /*
     * Java Accessors Gong Show
     */
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

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(final int timeout) {
        this.timeout = timeout;
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
}
