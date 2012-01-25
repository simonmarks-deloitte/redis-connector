/**
 * Mule Redis Connector
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */


package org.mule.module.redis;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

/**
 * @author flbulgarelli
 */
@RunWith(Theories.class)
public class RedisUtilsTest
{
    @DataPoints
    public static final Serializable[] SERIALIZABLES = {"Hello World", 340,
        (Serializable) Arrays.asList(4, 5, 6), TimeUnit.HOURS, null, /*XXX "" */ };

    @Theory
    public void fromBytesIsInverseOfFromBytes(Serializable value)
    {
        assertEquals(value, RedisUtils.fromBytes(RedisUtils.toBytes(value)));
    }



}
