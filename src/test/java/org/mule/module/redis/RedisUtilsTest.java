/**
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 */

package org.mule.module.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mule.module.redis.RedisUtils.RedisAction;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * @author flbulgarelli
 */
@RunWith(Theories.class)
public class RedisUtilsTest
{
    @DataPoints
    public static final Serializable[] SERIALIZABLES = {"Hello World", 340,
        (Serializable) Arrays.asList(4, 5, 6), TimeUnit.HOURS, null, /* XXX "" */};

    @Theory
    public void fromBytesIsInverseOfFromBytes(Serializable value)
    {
        assertEquals(value, RedisUtils.fromBytes(RedisUtils.toBytes(value)));
    }

    @Test
    public void runAnswersActionResultWhenSucceds() throws Exception
    {
        JedisPool poolMock = mock(JedisPool.class);
        String result = RedisUtils.run(poolMock, new RedisAction<String>()
        {
            @Override
            public String run()
            {
                return "Hello";
            }
        });
        assertEquals("Hello", result);
        verify(poolMock).getResource();
        verify(poolMock).returnResource(any(Jedis.class));
    }

    @Test
    public void runThrowsExceptionWhenActionFails() throws Exception
    {
        JedisPool poolMock = mock(JedisPool.class);
        try
        {
            RedisUtils.run(poolMock, new RedisAction<String>()
            {
                @Override
                public String run()
                {
                    throw new JedisConnectionException("ups!");
                }
            });
            fail();
        }
        catch (Exception e)
        {
            //OK
        }
        verify(poolMock).getResource();
        verify(poolMock).returnBrokenResource(any(Jedis.class));
    }

}
