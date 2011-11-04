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

import java.util.Collections;

import org.apache.commons.lang.RandomStringUtils;
import org.mule.api.MuleException;
import org.mule.module.client.MuleClient;
import org.mule.tck.FunctionalTestCase;
import org.mule.tck.functional.CountdownCallback;
import org.mule.tck.functional.FunctionalTestComponent;

public class RedisPubSubITCase extends FunctionalTestCase {

    @Override
    protected String getConfigResources() {
        return "redis-pubsub-tests-config.xml";
    }

    public void testChannelPubSub() throws Exception {
        testPubSub("mule.test.single.channel");
    }

    public void testPatternPubSub() throws Exception {
        testPubSub("mule.test.multi.channel.abc");
    }

    private void testPubSub(final String targetChannel) throws MuleException, Exception, InterruptedException {
        final String testPayload = RandomStringUtils.randomAlphanumeric(20);
        new MuleClient(muleContext).dispatch("vm://publisher.in", testPayload, Collections.singletonMap("target-channel", targetChannel));

        final CountdownCallback cc = new CountdownCallback(1);
        final FunctionalTestComponent ftc = getFunctionalTestComponent("subscriber");
        ftc.setEventCallback(cc);
        cc.await(1000L * getTestTimeoutSecs());
        assertEquals(testPayload, new String((byte[]) ftc.getLastReceivedMessage()));
    }
}
