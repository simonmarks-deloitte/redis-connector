/**
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 */

package org.mule.module.redis;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;
import org.mule.api.MuleException;
import org.mule.module.client.MuleClient;
import org.mule.tck.functional.CountdownCallback;
import org.mule.tck.functional.FunctionalTestComponent;
import org.mule.tck.junit4.FunctionalTestCase;

public class RedisPubSubITCase extends FunctionalTestCase
{
    @Override
    protected String getConfigResources()
    {
        return "redis-pubsub-tests-config.xml";
    }

    @Test
    public void testChannelPubSub() throws Exception
    {
        testPubSub("mule.test.single.channel");
    }

    @Test
    public void testPatternPubSub() throws Exception
    {
        testPubSub("mule.test.multi.channel.abc");
    }

    private void testPubSub(final String targetChannel) throws MuleException, Exception, InterruptedException
    {
        final String testPayload1 = RandomStringUtils.randomAlphanumeric(20);
        final String testPayload2 = RandomStringUtils.randomAlphanumeric(20);

        final Map<String, Object> props = new HashMap<String, Object>();
        props.put("target-channel", targetChannel);
        props.put("second-message-payload", testPayload2);

        new MuleClient(muleContext).dispatch("vm://publisher.in", testPayload1, props);

        final CountdownCallback cc = new CountdownCallback(2);
        final FunctionalTestComponent ftc = getFunctionalTestComponent("subscriber");
        ftc.setEventCallback(cc);

        cc.await(1000L * getTestTimeoutSecs());
        assertEquals(testPayload1, new String((byte[]) ftc.getReceivedMessage(1)));
        assertEquals(testPayload2, new String((byte[]) ftc.getReceivedMessage(2)));
    }
}
