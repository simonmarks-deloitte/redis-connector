/**
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 */

package org.mule.module.redis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.mule.api.MuleEvent;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.Module;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.Source;
import org.mule.api.annotations.param.Default;
import org.mule.api.annotations.param.Optional;
import org.mule.api.annotations.param.Payload;
import org.mule.api.callback.SourceCallback;
import org.mule.api.store.ObjectAlreadyExistsException;
import org.mule.api.store.ObjectDoesNotExistException;
import org.mule.api.store.ObjectStore;
import org.mule.api.store.ObjectStoreException;
import org.mule.api.store.PartitionableObjectStore;
import org.mule.config.i18n.MessageFactory;
import org.mule.module.redis.RedisUtils.RedisAction;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.BinaryTransaction;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.SafeEncoder;

/**
 * Redis is an open-source, networked, in-memory, persistent, journaled, key-value data store.
 * Provides Redis connectivity to Mule:
 * <ul>
 * <li>Supports Redis Publish/Subscribe model for asynchronous message exchanges,</li>
 * <li>Allows direct reading and writing operations in Redis collections,</li>
 * <li>Allows using Redis as a {@link ObjectStore} for Mule components that require persistence.</li>
 * </ul>
 * 
 * @author MuleSoft, Inc.
 */
@Module(name = "redis", schemaVersion = "3.4", friendlyName = "Redis", minMuleVersion = "3.4.0", description = "Redis Module")
public class RedisModule implements PartitionableObjectStore<Serializable>
{
    private static final String DEFAULT_PARTITION_NAME = "_default";

    private static final Log LOGGER = LogFactory.getLog(RedisModule.class);

    /**
     * Redis host
     */
    @Configurable
    @Optional
    @Default("localhost")
    private String host;

    /**
     * Redis port
     */
    @Configurable
    @Optional
    @Default("6379")
    private int port;

    /**
     * Connection timeout in milliseconds
     */
    @Configurable
    @Optional
    @Default("2000")
    private int connectionTimeout;

    /**
     * Reconnection frequency in milliseconds
     */
    @Configurable
    @Optional
    @Default("5000")
    private int reconnectionFrequency;

    /**
     * Redis password
     */
    @Configurable
    @Optional
    private String password;

    /**
     * Object pool configuration
     */
    @Configurable
    @Optional
    private Config poolConfig = new JedisPoolConfig();

    private JedisPool jedisPool;

    private volatile boolean running = true;

    /*----------------------------------------------------------
                Lifecycle Implementation
    ----------------------------------------------------------*/
    @PostConstruct
    public void initializeJedis()
    {
        jedisPool = new JedisPool(poolConfig, host, port, connectionTimeout, password);

        LOGGER.info(String.format(
            "Redis connector ready, host: %s, port: %d, timeout: %d, password: %s, pool config: %s", host,
            port, connectionTimeout, StringUtils.repeat("*", StringUtils.length(password)),
            ToStringBuilder.reflectionToString(poolConfig, ToStringStyle.SHORT_PREFIX_STYLE)));
    }

    @PreDestroy
    public void destroyJedis()
    {
        running = false;
        jedisPool.destroy();
        LOGGER.info("Redis connector terminated");
    }

    /*----------------------------------------------------------
                Datastructure Commands
    ----------------------------------------------------------*/

    /**
     * Set key to hold the payload. If key already holds a value, it is overwritten, regardless of
     * its type as long as ifNotExists is false.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:set}
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:set-value}
     * 
     * @param key Key used to store payload
     * @param expire Set a timeout on the specified key. After the timeout the key will be
     *            automatically deleted by the server. A key with an associated timeout is said to
     *            be volatile in Redis terminology.
     * @param ifNotExists If true, then execute SETNX on the Redis server, otherwise execute SET
     * @param value The value to set.
     * @param muleEvent The current {@link MuleEvent}.
     * @return If the key already exists and ifNotExists is true, null is returned. Otherwise the
     *         message is returned.
     */
    @Processor
    @Inject
    public byte[] set(final String key,
                      @Optional final Integer expire,
                      @Optional @Default("false") final Boolean ifNotExists,
                      @Optional @Default("#[message.payloadAs(java.lang.String)]") final String value,
                      final MuleEvent muleEvent)
    {
        return RedisUtils.run(jedisPool, new RedisAction<byte[]>()
        {
            @Override
            public byte[] run()
            {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                byte[] valueAsBytes = RedisUtils.toBytes(value, muleEvent.getEncoding());

                if (ifNotExists)
                {
                    if (redis.setnx(keyAsBytes, valueAsBytes) == 0)
                    {
                        valueAsBytes = null;
                    }
                }
                else
                {
                    redis.set(keyAsBytes, valueAsBytes);
                }

                if (expire != null)
                {
                    redis.expire(keyAsBytes, expire);
                }

                return valueAsBytes;
            }
        });
    }

    /**
     * Get the value of the specified key. If the key does not exist null is returned. If the value
     * stored at key is not a string an error is returned because GET can only handle string values.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:get}
     * 
     * @param key Key that will be used for GET
     * @return A byte array with the content of the key
     */
    @Processor
    public byte[] get(final String key)
    {
        return RedisUtils.run(jedisPool, new RedisAction<byte[]>()
        {
            @Override
            public byte[] run()
            {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                return redis.get(keyAsBytes);
            }
        });
    }

    /**
     * Test if the specified key exists.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:exists}
     * 
     * @param key Key that will be used for EXISTS
     * @return A boolean that represents the existence of the key.
     */
    @Processor
    public Boolean exists(final String key)
    {
        return RedisUtils.run(jedisPool, new RedisAction<Boolean>()
        {
            @Override
            public Boolean run()
            {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                return redis.exists(keyAsBytes);
            }
        });
    }

    /**
     * Increments the number stored at key by step. If the key does not exist, it is set to 0 before
     * performing the operation. An error is returned if the key contains a value of the wrong type
     * or contains a string that can not be represented as integer.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:increment}
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:increment-step}
     * 
     * @param key Key that will be used for INCR.
     * @param step Step used for the increment.
     * @return the incremented number.
     */
    @Processor
    public Long increment(final String key, @Optional @Default("1") final long step)
    {
        return RedisUtils.run(jedisPool, new RedisAction<Long>()
        {
            @Override
            public Long run()
            {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                return step == 1L ? redis.incr(keyAsBytes) : redis.incrBy(keyAsBytes, step);
            }
        });
    }

    // LATER add http://redis.io/commands/incrbyfloat when Jedis supports it

    /**
     * Decrements the number stored at key by step. If the key does not exist, it is set to 0 before
     * performing the operation. An error is returned if the key contains a value of the wrong type
     * or contains a string that can not be represented as integer.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:decrement}
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:decrement-step}
     * 
     * @param key Key that will be used for DECR.
     * @param step Step used for the increment.
     * @return A byte array with the content of the key
     */
    @Processor
    public Long decrement(final String key, @Optional @Default("1") final long step)
    {
        return RedisUtils.run(jedisPool, new RedisAction<Long>()
        {
            @Override
            public Long run()
            {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                return step == 1L ? redis.decr(keyAsBytes) : redis.decrBy(keyAsBytes, step);
            }
        });
    }

    // ************** Hashes **************

    /**
     * Set the specified hash field to the message payload. If key does not exist, a new key holding
     * a hash is created as long as ifNotExists is true.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:hash-set}
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:hash-set-value}
     * 
     * @param key Key that will be used for HSET
     * @param field Field that will be used for HSET
     * @param ifNotExists If true execute HSETNX otherwise HSET
     * @param value The value to set.
     * @param muleEvent The current {@link MuleEvent}.
     * @return If the field already exists and ifNotExists is true, null is returned, otherwise if a
     *         new field is created the message is returned.
     */
    @Processor(name = "hash-set")
    @Inject
    public byte[] setInHash(final String key,
                            final String field,
                            @Optional @Default("false") final Boolean ifNotExists,
                            @Optional @Default("#[message.payloadAs(java.lang.String)]") final String value,
                            final MuleEvent muleEvent)
    {
        return RedisUtils.run(jedisPool, new RedisAction<byte[]>()
        {
            @Override
            public byte[] run()
            {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                final byte[] fieldAsBytes = SafeEncoder.encode(field);
                final byte[] valueAsBytes = RedisUtils.toBytes(value, muleEvent.getEncoding());

                if (ifNotExists)
                {
                    if (redis.hsetnx(keyAsBytes, fieldAsBytes, valueAsBytes) == 0)
                    {
                        return null;
                    }
                }
                else
                {
                    redis.hset(keyAsBytes, fieldAsBytes, valueAsBytes);
                }

                return valueAsBytes;
            }
        });
    }

    /**
     * Get the value stored at the specified field in the hash at the specified key. If the field or
     * the hash don't exist, null is returned.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:hash-get}
     * 
     * @param key Key that will be used for HGET
     * @param field Field that will be used for HGET
     * @return The value or null.
     */
    @Processor(name = "hash-get")
    public byte[] getFromHash(final String key, final String field)
    {
        return RedisUtils.run(jedisPool, new RedisAction<byte[]>()
        {
            @Override
            public byte[] run()
            {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                final byte[] fieldAsBytes = SafeEncoder.encode(field);
                return redis.hget(keyAsBytes, fieldAsBytes);
            }
        });
    }

    /**
     * Increments the number stored at field in the hash stored at key by increment. If key does not
     * exist, a new key holding a hash is created. If field does not exist the value is set to 0
     * before the operation is performed.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:hash-increment}
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:hash-increment-step}
     * 
     * @param key Key that will be used for HGET
     * @param field Field that will be used for HGET
     * @param step Step used for the increment.
     * @return the incremented number.
     */
    @Processor(name = "hash-increment")
    public Long incrementHash(final String key, final String field, @Optional @Default("1") final long step)
    {
        return RedisUtils.run(jedisPool, new RedisAction<Long>()
        {
            @Override
            public Long run()
            {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                final byte[] fieldAsBytes = SafeEncoder.encode(field);
                return redis.hincrBy(keyAsBytes, fieldAsBytes, step);
            }
        });
    }

    // LATER add http://redis.io/commands/hincrbyfloat when Jedis supports it

    // ************** Lists **************

    public static enum ListPushSide
    {
        LEFT
        {
            @Override
            byte[] push(final BinaryJedis redis,
                        final byte[] key,
                        final byte[] message,
                        final boolean ifExists)
            {
                if (ifExists)
                {
                    if (redis.lpushx(key, message) == 0)
                    {
                        return null;
                    }
                }
                else
                {
                    redis.lpush(key, message);
                }
                return message;
            }

            @Override
            byte[] pop(final BinaryJedis redis, final byte[] key)
            {
                return redis.lpop(key);
            }
        },
        RIGHT
        {
            @Override
            byte[] push(final BinaryJedis redis,
                        final byte[] key,
                        final byte[] message,
                        final boolean ifExists)
            {
                if (ifExists)
                {
                    if (redis.rpushx(key, message) == 0)
                    {
                        return null;
                    }
                }
                else
                {
                    redis.rpush(key, message);
                }
                return message;
            }

            @Override
            byte[] pop(final BinaryJedis redis, final byte[] key)
            {
                return redis.rpop(key);
            }
        };

        abstract byte[] push(BinaryJedis redis, byte[] key, byte[] message, boolean ifNotExists);

        abstract byte[] pop(BinaryJedis redis, final byte[] key);
    }

    /**
     * Push the message payload to the desired side (LEFT or RIGHT) of the list stored at the
     * specified key. If key does not exist, a new key holding a list is created as long as ifExists
     * is not true.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:list-push}
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:list-push-value}
     * 
     * @param key Key that will be used for LPUSH/RPUSH/LPUSHX/RPUSH
     * @param side The side where to push the payload, either LEFT or RIGHT
     * @param ifExists If true execute LPUSHX/RPUSH otherwise LPUSH/RPUSH
     * @param value The value to push.
     * @param muleEvent The current {@link MuleEvent}.
     * @return If the key doesn't already exist and ifExists is true, null is returned. Otherwise
     *         the message is returned.
     */
    @Processor(name = "list-push")
    @Inject
    public byte[] pushToList(final String key,
                             final ListPushSide side,
                             @Optional @Default("false") final Boolean ifExists,
                             @Optional @Default("#[message.payloadAs(java.lang.String)]") final String value,
                             final MuleEvent muleEvent)
    {
        return RedisUtils.run(jedisPool, new RedisAction<byte[]>()
        {
            @Override
            public byte[] run()
            {
                final byte[] valueAsBytes = RedisUtils.toBytes(value, muleEvent.getEncoding());
                return side.push(redis, SafeEncoder.encode(key), valueAsBytes, ifExists);
            }
        });
    }

    /**
     * Pop a value from the desired side of the list stored at the specified key.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:list-pop}
     * 
     * @param key Key that will be used for LPOP/RPOP
     * @param side The side where to pop the value from, either LEFT or RIGHT
     * @return The popped value or null if either the list is empty or no list exists at the key
     */
    @Processor(name = "list-pop")
    public byte[] popFromList(final String key, final ListPushSide side)
    {
        return RedisUtils.run(jedisPool, new RedisAction<byte[]>()
        {
            @Override
            public byte[] run()
            {
                return side.pop(redis, SafeEncoder.encode(key));
            }
        });
    }

    // ************** Sets **************

    /**
     * Add the message payload to the set stored at the specified key. If key does not exist, a new
     * key holding a set is created.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:set-add}
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:set-add-value}
     * 
     * @param key Key that will be used for SADD
     * @param mustSucceed If true, ensures that adding to the set was successful (ie no pre-existing
     *            identical value in the set)
     * @param value The value to set.
     * @param muleEvent The current {@link MuleEvent}.
     * @return If no new entry has been added to the set and mustSucceed is true, null is returned.
     *         Otherwise the message is returned.
     */
    @Processor(name = "set-add")
    @Inject
    public byte[] addToSet(final String key,
                           @Optional @Default("false") final Boolean mustSucceed,
                           @Optional @Default("#[message.payloadAs(java.lang.String)]") final String value,
                           final MuleEvent muleEvent)
    {
        return RedisUtils.run(jedisPool, new RedisAction<byte[]>()
        {
            @Override
            public byte[] run()
            {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                final byte[] valueAsBytes = RedisUtils.toBytes(value, muleEvent.getEncoding());

                final long result = redis.sadd(keyAsBytes, valueAsBytes);
                return !mustSucceed || result > 0 ? valueAsBytes : null;
            }
        });
    }

    /**
     * Pops a random value from the set stored at the specified key.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:set-pop}
     * 
     * @param key Key that will be used for SPOP
     * @return The popped value or null if either the set is empty or no set exists at the key
     */
    @Processor(name = "set-pop")
    public byte[] popFromSet(final String key)
    {
        return RedisUtils.run(jedisPool, new RedisAction<byte[]>()
        {
            @Override
            public byte[] run()
            {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                final byte[] result = redis.spop(keyAsBytes);
                return result;
            }
        });
    }

    /**
     * Reads a random value from the set stored at the specified key.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:set-fetch-random-member}
     * 
     * @param key Key that will be used for SRANDMEMBER
     * @return The random value or null if either the set is empty or no set exists at the key
     */
    @Processor(name = "set-fetch-random-member")
    public byte[] randomMemberFromSet(final String key)
    {
        return RedisUtils.run(jedisPool, new RedisAction<byte[]>()
        {
            @Override
            public byte[] run()
            {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                final byte[] result = redis.srandmember(keyAsBytes);
                return result;
            }
        });
    }

    // ************** Sorted Sets **************

    /**
     * Add the message payload with the desired score to the sorted set stored at the specified key.
     * If key does not exist, a new key holding a sorted set is created.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:sorted-set-add}
     * 
     * @param key Key that will be used for ZADD
     * @param score Score to use for the value
     * @param mustSucceed If true, ensures that adding to the sorted set was successful (ie no
     *            pre-existing identical value in the set)
     * @param message The payload of the message as a byte array
     * @return If no new entry has been added to the sorted set and mustSucceed is true, null is
     *         returned. Otherwise the message is returned.
     */
    @Processor(name = "sorted-set-add")
    public byte[] addToSortedSet(final String key,
                                 final double score,
                                 @Optional @Default("false") final Boolean mustSucceed,
                                 @Payload final byte[] message)
    {
        return RedisUtils.run(jedisPool, new RedisAction<byte[]>()
        {
            @Override
            public byte[] run()
            {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                final long result = redis.zadd(keyAsBytes, score, message);
                return !mustSucceed || result > 0 ? message : null;
            }
        });
    }

    public static enum SortedSetOrder
    {
        ASCENDING
        {
            @Override
            Set<byte[]> getRangeByIndex(final BinaryJedis redis,
                                        final byte[] key,
                                        final int start,
                                        final int end)
            {
                return redis.zrange(key, start, end);
            }

            @Override
            Set<byte[]> getRangeByScore(final BinaryJedis redis,
                                        final byte[] key,
                                        final double min,
                                        final double max)
            {
                return redis.zrangeByScore(key, min, max);
            }
        },
        DESCENDING
        {
            @Override
            Set<byte[]> getRangeByIndex(final BinaryJedis redis,
                                        final byte[] key,
                                        final int start,
                                        final int end)
            {
                return redis.zrevrange(key, start, end);
            }

            @Override
            Set<byte[]> getRangeByScore(final BinaryJedis redis,
                                        final byte[] key,
                                        final double min,
                                        final double max)
            {
                return redis.zrevrangeByScore(key, min, max);
            }
        };

        abstract Set<byte[]> getRangeByIndex(BinaryJedis redis, final byte[] key, int start, int end);

        abstract Set<byte[]> getRangeByScore(BinaryJedis redis, final byte[] key, double min, double max);
    }

    /**
     * Retrieve a range of values from the sorted set stored at the specified key. The range of
     * values is defined by indices in the sorted set and sorted as desired.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample
     * redis:sorted-set-select-range-by-index}
     * 
     * @param key Key that will be used for ZRANGE/ZREVRANGE
     * @param start Range start index
     * @param end Range end index
     * @param order Index order for sorting the range, either ASCENDING or DESCENDING
     * @return the values in the specified range in the desired order as Set<byte[]>
     */
    @Processor(name = "sorted-set-select-range-by-index")
    public Set<byte[]> getRangeByIndex(final String key,
                                       final int start,
                                       final int end,
                                       @Optional @Default("ASCENDING") final SortedSetOrder order)
    {
        return RedisUtils.run(jedisPool, new RedisAction<Set<byte[]>>()
        {
            @Override
            public Set<byte[]> run()
            {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                return order.getRangeByIndex(redis, keyAsBytes, start, end);
            }
        });
    }

    /**
     * Retrieve a range of values from the sorted set stored at the specified key. The range of
     * values is defined by scores in the sorted set and sorted as desired.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample
     * redis:sorted-set-select-range-by-score}
     * 
     * @param key Key that will be used for ZRANGEBYSCORE/ZREVRANGEBYSCORE
     * @param min Range start score
     * @param max Range end score
     * @param order Score order for sorting the range, either ASCENDING or DESCENDING
     * @return the values in the specified range in the desired order as Set<byte[]>
     */
    @Processor(name = "sorted-set-select-range-by-score")
    public Set<byte[]> getRangeByScore(final String key,
                                       final double min,
                                       final double max,
                                       @Optional @Default("ASCENDING") final SortedSetOrder order)
    {
        return RedisUtils.run(jedisPool, new RedisAction<Set<byte[]>>()
        {
            @Override
            public Set<byte[]> run()
            {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                return order.getRangeByScore(redis, keyAsBytes, min, max);
            }
        });
    }

    /**
     * Increments the score of member in the sorted set stored at key by increment. If member does
     * not exist in the sorted set, it is added with increment as its score (as if its previous
     * score was 0.0). If key does not exist, a new sorted set with the specified member as its sole
     * member is created.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:sorted-set-increment}
     * 
     * @param key the key in the sorted set.
     * @param step the step to use to increment the score.
     * @param member The payload of the message as a byte array.
     * @return the new score of the member.
     */
    @Processor(name = "sorted-set-increment")
    public Double incrementSortedSet(final String key, final double step, @Payload final byte[] member)
    {
        return RedisUtils.run(jedisPool, new RedisAction<Double>()
        {
            @Override
            public Double run()
            {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                return redis.zincrby(keyAsBytes, step, member);
            }
        });
    }

    // ************** Key Volatility **************

    /**
     * Set a timeout on the specified key.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:expire}
     * 
     * @param key the key in the sorted set.
     * @param seconds the time to live in seconds.
     * @return true if EXPIRE was successful, false otherwise.
     */
    @Processor
    public Boolean expire(final String key, final int seconds)
    {
        return RedisUtils.run(jedisPool, new RedisAction<Boolean>()
        {
            @Override
            public Boolean run()
            {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                return redis.expire(keyAsBytes, seconds) == 1L;
            }
        });
    }

    /**
     * Set a timeout in the form of a UNIX timestamp (Number of seconds elapsed since 1 Jan 1970) on
     * the specified key.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:expire-at}
     * 
     * @param key the key in the sorted set.
     * @param unixTime the UNIX timestamp in seconds.
     * @return true if EXPIREAT was successful, false otherwise.
     */
    @Processor
    public Boolean expireAt(final String key, final long unixTime)
    {
        return RedisUtils.run(jedisPool, new RedisAction<Boolean>()
        {
            @Override
            public Boolean run()
            {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                return redis.expireAt(keyAsBytes, unixTime) == 1L;
            }
        });
    }

    /**
     * Undo an expire or expireAt ; turning the volatile key into a normal key.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:persist}
     * 
     * @param key the key in the sorted set.
     * @return true if PERSIST was successful, false otherwise.
     */
    @Processor
    public Boolean persist(final String key)
    {
        return RedisUtils.run(jedisPool, new RedisAction<Boolean>()
        {
            @Override
            public Boolean run()
            {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                return redis.persist(keyAsBytes) == 1L;
            }
        });
    }

    /**
     * Get the remaining time to live in seconds of a volatile key.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:get-ttl}
     * 
     * @param key the key in the sorted set.
     * @return the remaining time to live in seconds, -2 when key does not exist or -1 when key does
     *         not have a timeout.
     */
    @Processor
    public Long getTtl(final String key)
    {
        return RedisUtils.run(jedisPool, new RedisAction<Long>()
        {
            @Override
            public Long run()
            {
                final byte[] keyAsBytes = SafeEncoder.encode(key);
                return redis.ttl(keyAsBytes);
            }
        });
    }

    // LATER add PEXPIRE PEXPIREAT PTTL when Jedis supports it

    /*----------------------------------------------------------
                Pub/Sub Implementation
    ----------------------------------------------------------*/

    /**
     * Publish the message payload to the specified channel.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:publish}
     * 
     * @param channel Destination of the published message
     * @param mustSucceed Enforces the fact that the message must have been delivered to at least
     *            one consumer
     * @param message The payload of the message as a byte array
     * @return If no consumer is subscribed to the channel and mustSucceed is true, null is
     *         returned. Otherwise the message is returned.
     */
    @Processor
    public byte[] publish(final String channel,
                          @Optional @Default("false") final Boolean mustSucceed,
                          @Payload final byte[] message)
    {
        return RedisUtils.run(jedisPool, new RedisAction<byte[]>()
        {
            @Override
            public byte[] run()
            {
                final Long numberOfSubscribers = redis.publish(SafeEncoder.encode(channel), message);
                return (!mustSucceed || (mustSucceed && numberOfSubscribers > 0)) ? message : null;
            }
        });
    }

    /**
     * Subscribe to the specified channels.
     * <p/>
     * {@sample.xml ../../../doc/mule-module-redis.xml.sample redis:subscribe}
     * 
     * @param channels A list of channel names or globbing patterns.
     * @param callback Called when messages arrive in any of the specified channels.
     */
    @Source
    public void subscribe(final List<String> channels, final SourceCallback callback)
    {
        while (running)
        {
            try
            {
                RedisUtils.run(jedisPool, new RedisAction<Void>()
                {
                    @Override
                    public Void run()
                    {
                        // this blocks until Redis gets disconnected
                        final RedisPubSubListener listener = new RedisPubSubListener(callback);

                        redis.psubscribe(listener, RedisUtils.getPatternsFromChannels(channels));
                        return null;
                    }
                });
            }
            catch (final JedisConnectionException jce)
            {
                LOGGER.warn("Subscriber disconnected from channels: " + channels
                            + ", will retry connecting in: " + reconnectionFrequency + "ms.", jce);

                try
                {
                    if (running)
                    {
                        Thread.sleep(reconnectionFrequency);
                    }
                }
                catch (final InterruptedException ie)
                {
                    // connector stopping, let's restore interrupted state
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /*----------------------------------------------------------
                ObjectStore Implementation
    ----------------------------------------------------------*/
    @Override
    public boolean isPersistent()
    {
        return true;
    }

    @Override
    public boolean contains(final Serializable key) throws ObjectStoreException
    {
        return contains(key, DEFAULT_PARTITION_NAME);
    }

    @Override
    public void store(final Serializable key, final Serializable value) throws ObjectStoreException
    {
        store(key, value, DEFAULT_PARTITION_NAME);
    }

    @Override
    public Serializable retrieve(final Serializable key) throws ObjectStoreException
    {
        return retrieve(key, DEFAULT_PARTITION_NAME);
    }

    @Override
    public Serializable remove(final Serializable key) throws ObjectStoreException
    {
        return remove(key, DEFAULT_PARTITION_NAME);
    }

    /*----------------------------------------------------------
               ListableObjectStore Implementation
    ----------------------------------------------------------*/
    @Override
    public void open() throws ObjectStoreException
    {
        open(DEFAULT_PARTITION_NAME);
    }

    @Override
    public void close() throws ObjectStoreException
    {
        close(DEFAULT_PARTITION_NAME);
    }

    @Override
    public List<Serializable> allKeys() throws ObjectStoreException
    {
        return allKeys(DEFAULT_PARTITION_NAME);
    }

    /*----------------------------------------------------------
             PartitionableObjectStore Implementation
    ----------------------------------------------------------*/
    @Override
    public boolean contains(final Serializable key, final String partitionName) throws ObjectStoreException
    {
        return RedisUtils.run(jedisPool, new RedisAction<Boolean>()
        {
            @Override
            public Boolean run()
            {
                return redis.hexists(RedisUtils.getPartitionHashKey(partitionName), RedisUtils.toBytes(key));
            }
        });
    }

    @Override
    public void store(final Serializable key, final Serializable value, final String partitionName)
        throws ObjectStoreException
    {
        final Long result = RedisUtils.run(jedisPool, new RedisAction<Long>()
        {
            @Override
            public Long run()
            {
                return redis.hsetnx(RedisUtils.getPartitionHashKey(partitionName), RedisUtils.toBytes(key),
                    RedisUtils.toBytes(value));
            }
        });

        if (result == 0)
        {
            throw new ObjectAlreadyExistsException(
                MessageFactory.createStaticMessage("There is already a value for: " + key));
        }
    }

    @Override
    public Serializable retrieve(final Serializable key, final String partitionName)
        throws ObjectStoreException
    {
        final Serializable result = RedisUtils.run(jedisPool, new RedisAction<Serializable>()
        {
            @Override
            public Serializable run()
            {
                return RedisUtils.fromBytes(redis.hget(RedisUtils.getPartitionHashKey(partitionName),
                    RedisUtils.toBytes(key)));
            }
        });

        if (result == null)
        {
            throw new ObjectDoesNotExistException(
                MessageFactory.createStaticMessage("No value found for key: " + key));
        }

        return result;
    }

    @Override
    public Serializable remove(final Serializable key, final String partitionName)
        throws ObjectStoreException
    {
        final Serializable result = RedisUtils.run(jedisPool, new RedisAction<Serializable>()
        {
            @Override
            public Serializable run()
            {
                final byte[] keyAsBytes = RedisUtils.toBytes(key);

                final BinaryTransaction t = redis.multi();
                final Response<byte[]> getResult = t.hget(RedisUtils.getPartitionHashKey(partitionName),
                    keyAsBytes);
                final Response<Long> delResult = t.hdel(RedisUtils.getPartitionHashKey(partitionName),
                    keyAsBytes);
                t.exec();

                if (delResult.get() != 1)
                {
                    return null;
                }

                return RedisUtils.fromBytes(getResult.get());
            }
        });

        if (result == null)
        {
            throw new ObjectDoesNotExistException(
                MessageFactory.createStaticMessage("No value found for key: " + key));
        }

        return result;
    }

    @Override
    public List<Serializable> allKeys(final String partitionName) throws ObjectStoreException
    {
        return RedisUtils.run(jedisPool, new RedisAction<List<Serializable>>()
        {
            @Override
            public List<Serializable> run()
            {
                final List<Serializable> keys = new ArrayList<Serializable>();
                for (final byte[] key : redis.hkeys(RedisUtils.getPartitionHashKey(partitionName)))
                {
                    keys.add(RedisUtils.fromBytes(key));
                }
                return keys;
            }
        });
    }

    @Override
    public List<String> allPartitions() throws ObjectStoreException
    {
        return RedisUtils.run(jedisPool, new RedisAction<List<String>>()
        {
            @Override
            public List<String> run()
            {
                final List<String> partitions = new ArrayList<String>();
                final Set<byte[]> keys = redis.keys((RedisConstants.OBJECTSTORE_HASH_KEY_PREFIX + "*").getBytes());
                for (final byte[] key : keys)
                {
                    final String partition = StringUtils.substringAfter(SafeEncoder.encode(key),
                        RedisConstants.OBJECTSTORE_HASH_KEY_PREFIX);
                    partitions.add(partition);
                }
                return partitions;
            }
        });
    }

    @Override
    public void open(final String partitionName) throws ObjectStoreException
    {
        // ignored
    }

    @Override
    public void close(final String partitionName) throws ObjectStoreException
    {
        // ignored
    }

    @Override
    public void disposePartition(final String partitionName) throws ObjectStoreException
    {
        RedisUtils.run(jedisPool, new RedisAction<Long>()
        {
            @Override
            public Long run()
            {
                return redis.del(RedisUtils.getPartitionHashKey(partitionName));
            }
        });
    }

    /*----------------------------------------------------------
                        Java Accessors Gong Show
     ----------------------------------------------------------*/
    public String getHost()
    {
        return host;
    }

    public void setHost(final String host)
    {
        this.host = host;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(final int port)
    {
        this.port = port;
    }

    public int getConnectionTimeout()
    {
        return connectionTimeout;
    }

    public void setConnectionTimeout(final int connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
    }

    public int getReconnectionFrequency()
    {
        return reconnectionFrequency;
    }

    public void setReconnectionFrequency(final int reconnectionFrequency)
    {
        this.reconnectionFrequency = reconnectionFrequency;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(final String password)
    {
        this.password = password;
    }

    public Config getPoolConfig()
    {
        return poolConfig;
    }

    public void setPoolConfig(final Config poolConfig)
    {
        this.poolConfig = poolConfig;
    }

    public JedisPool getJedisPool()
    {
        return jedisPool;
    }
}
