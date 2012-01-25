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

import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.mule.api.callback.SourceCallback;

/**
 * @author flbulgarelli
 */
@SuppressWarnings("unchecked")
public class RedisPubSubListenerUnitTest
{
    private SourceCallback sourceCallbackMock;
    private RedisPubSubListener redisPubSubListener;

    @Before
    public void setup()
    {
        sourceCallbackMock = mock(SourceCallback.class);
        redisPubSubListener = new RedisPubSubListener(sourceCallbackMock);
    }

    @Test
    public void messagesAreProcessedByCallback() throws Exception
    {
        redisPubSubListener.onMessage("Channel01".getBytes(), "Hello World".getBytes());

        verify(sourceCallbackMock).process(eq("Hello World".getBytes()), anyMap());
    }

    @Test
    public void pMessagesAreProcessedByCallback() throws Exception
    {
        redisPubSubListener.onPMessage("Pattern".getBytes(), "Channel01".getBytes(), "Hello World".getBytes());

        verify(sourceCallbackMock).process(eq("Hello World".getBytes()), anyMap());
    }

}
