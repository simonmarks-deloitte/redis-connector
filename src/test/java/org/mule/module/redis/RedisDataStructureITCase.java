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

import org.apache.commons.lang.RandomStringUtils;
import org.mule.module.client.MuleClient;
import org.mule.tck.FunctionalTestCase;
import org.mule.transport.NullPayload;
import org.mule.util.UUID;

public class RedisDataStructureITCase extends FunctionalTestCase {
    private static final String FIELD_PROP = "field";
    private static final String KEY_PROP = "key";
    private static final String TEST_KEY_PREFIX = "mule.tests.";
    private MuleClient muleClient;

    @Override
    protected String getConfigResources() {
        return "redis-datastructures-tests-config.xml";
    }

    @Override
    protected void doSetUp() throws Exception {
        super.doSetUp();
        muleClient = new MuleClient(muleContext);
    }

    public void testStrings() throws Exception {
        final String testPayload = RandomStringUtils.randomAlphanumeric(20);
        final String testKey = TEST_KEY_PREFIX + UUID.getUUID();
        muleClient.send("vm://strings-writer.in", testPayload, Collections.singletonMap(KEY_PROP, testKey));

        // wait a little more than the TTL of 1 second
        Thread.sleep(2000L);

        assertEquals(testPayload, muleClient.send("vm://strings-reader.in", "ignored", Collections.singletonMap(KEY_PROP, testKey))
                .getPayloadAsString());
        assertEquals(NullPayload.getInstance(),
                muleClient.send("vm://strings-reader.in", "ignored", Collections.singletonMap(KEY_PROP, testKey + ".ttl")).getPayload());
        assertEquals(testPayload,
                muleClient.send("vm://strings-reader.in", "ignored", Collections.singletonMap(KEY_PROP, testKey + ".other"))
                        .getPayloadAsString());
    }

    public void testHashes() throws Exception {
        final String testPayload = RandomStringUtils.randomAlphanumeric(20);
        final String testKey = TEST_KEY_PREFIX + UUID.getUUID();
        final String testField = UUID.getUUID();

        final Map<String, String> props = new HashMap<String, String>();
        props.put(KEY_PROP, testKey);
        props.put(FIELD_PROP, testField);

        muleClient.send("vm://hashes-writer.in", testPayload, props);

        assertEquals(testPayload, muleClient.send("vm://hashes-reader.in", "ignored", props).getPayloadAsString());
        props.put(FIELD_PROP, testField + ".other");
        assertEquals(testPayload, muleClient.send("vm://hashes-reader.in", "ignored", props).getPayloadAsString());
    }
}
