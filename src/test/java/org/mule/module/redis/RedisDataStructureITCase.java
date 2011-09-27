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
import java.util.Set;

import org.apache.commons.lang.RandomStringUtils;
import org.mule.api.MuleMessageCollection;
import org.mule.module.client.MuleClient;
import org.mule.tck.FunctionalTestCase;
import org.mule.transport.NullPayload;
import org.mule.util.UUID;

public class RedisDataStructureITCase extends FunctionalTestCase {
    private static final String SIDE_PROP = "side";
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

    public void testLists() throws Exception {
        final String testPayload = RandomStringUtils.randomAlphanumeric(20);
        final String testKey = TEST_KEY_PREFIX + UUID.getUUID();
        muleClient.send("vm://lists-writer.in", testPayload, Collections.singletonMap(KEY_PROP, testKey));

        final Map<String, String> props = new HashMap<String, String>();
        props.put(KEY_PROP, testKey);
        props.put(SIDE_PROP, "LEFT");
        assertEquals(testPayload, muleClient.send("vm://lists-reader.in", "ignored", props).getPayloadAsString());
        props.put(SIDE_PROP, "RIGHT");
        assertEquals(testPayload, muleClient.send("vm://lists-reader.in", "ignored", props).getPayloadAsString());
    }

    public void testSets() throws Exception {
        final String testPayload = RandomStringUtils.randomAlphanumeric(20);
        final String testKey = TEST_KEY_PREFIX + UUID.getUUID();

        final MuleMessageCollection writerResults = (MuleMessageCollection) muleClient.send("vm://sets-writer.in", testPayload,
                Collections.singletonMap(KEY_PROP, testKey));
        assertEquals(3, writerResults.size());
        assertEquals(testPayload, writerResults.getMessage(0).getPayloadAsString());
        assertEquals(NullPayload.getInstance(), writerResults.getMessage(1).getPayload());
        assertEquals(testPayload, writerResults.getMessage(2).getPayloadAsString());

        final MuleMessageCollection readerResults = (MuleMessageCollection) muleClient.send("vm://sets-reader.in", "ignored",
                Collections.singletonMap(KEY_PROP, testKey));
        assertEquals(3, readerResults.size());
        assertEquals(testPayload, readerResults.getMessage(0).getPayloadAsString());
        assertEquals(testPayload, readerResults.getMessage(1).getPayloadAsString());
        assertEquals(NullPayload.getInstance(), readerResults.getMessage(2).getPayload());
    }

    public void testSortedSets() throws Exception {
        String testPayload = RandomStringUtils.randomAlphanumeric(20);
        final String testKey = TEST_KEY_PREFIX + UUID.getUUID();

        final Map<String, String> props = new HashMap<String, String>();
        props.put(KEY_PROP, testKey);
        props.put("score", "1");
        MuleMessageCollection writerResults = (MuleMessageCollection) muleClient.send("vm://sorted-sets-writer.in", testPayload, props);
        assertEquals(3, writerResults.size());
        assertEquals(testPayload, writerResults.getMessage(0).getPayloadAsString());
        assertEquals(NullPayload.getInstance(), writerResults.getMessage(1).getPayload());
        assertEquals(testPayload, writerResults.getMessage(2).getPayloadAsString());

        testPayload = RandomStringUtils.randomAlphanumeric(20);
        props.put("score", "2.5");
        writerResults = (MuleMessageCollection) muleClient.send("vm://sorted-sets-writer.in", testPayload, props);
        assertEquals(3, writerResults.size());
        assertEquals(testPayload, writerResults.getMessage(0).getPayloadAsString());
        assertEquals(NullPayload.getInstance(), writerResults.getMessage(1).getPayload());
        assertEquals(testPayload, writerResults.getMessage(2).getPayloadAsString());

        final MuleMessageCollection readerResults = (MuleMessageCollection) muleClient.send("vm://sorted-sets-reader.in", "ignored",
                Collections.singletonMap(KEY_PROP, testKey));
        assertEquals(4, readerResults.size());
        for (int i = 0; i < 4; i++) {
            final Object payload = readerResults.getMessage(i).getPayload();
            assertTrue(testPayload, payload instanceof Set<?>);
            assertEquals("size of " + i, 2, ((Set<?>) readerResults.getMessage(i).getPayload()).size());
        }
    }
}
