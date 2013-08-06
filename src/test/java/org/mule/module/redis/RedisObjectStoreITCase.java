/**
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 */

package org.mule.module.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;
import org.mule.api.store.ObjectAlreadyExistsException;
import org.mule.api.store.ObjectDoesNotExistException;
import org.mule.api.store.ObjectStoreException;
import org.mule.api.store.PartitionableObjectStore;
import org.mule.tck.functional.CountdownCallback;
import org.mule.tck.functional.FunctionalTestComponent;
import org.mule.tck.junit4.FunctionalTestCase;

public class RedisObjectStoreITCase extends FunctionalTestCase
{
    private PartitionableObjectStore<String> stringObjectStore;

    @Override
    protected String getConfigResources()
    {
        return "redis-objectstore-tests-config.xml";
    }

    @Override
    protected void doSetUp() throws Exception
    {
        super.doSetUp();

        stringObjectStore = muleContext.getRegistry()
            .lookupObject(FakeObjectStoreUser.class)
            .getObjectStore();
    }

    @Test
    public void testListableObjectStoreOperations() throws ObjectStoreException
    {
        // open and close are noops
        stringObjectStore.open();
        stringObjectStore.close();

        assertTrue(stringObjectStore.isPersistent());

        final String testKey = RandomStringUtils.randomAlphanumeric(20);
        final String testValue = RandomStringUtils.randomAlphanumeric(20);

        assertFalse(stringObjectStore.contains(testKey));
        assertFalse(stringObjectStore.allKeys().contains(testKey));

        try
        {
            stringObjectStore.retrieve(testKey);
            fail("should have got an ObjectDoesNotExistException");
        }
        catch (final ObjectDoesNotExistException odnee)
        {
            // NOOP
        }

        stringObjectStore.store(testKey, testValue);
        assertTrue(stringObjectStore.contains(testKey));
        assertTrue(stringObjectStore.allKeys().contains(testKey));

        try
        {
            stringObjectStore.store(testKey, testValue);
            fail("should have got an ObjectAlreadyExistsException");
        }
        catch (final ObjectAlreadyExistsException oaee)
        {
            // NOOP
        }

        assertEquals(testValue, stringObjectStore.retrieve(testKey));

        assertEquals(testValue, stringObjectStore.remove(testKey));
        assertFalse(stringObjectStore.contains(testKey));
        assertFalse(stringObjectStore.allKeys().contains(testKey));

        try
        {
            stringObjectStore.remove(testKey);
            fail("should have got an ObjectDoesNotExistException");
        }
        catch (final ObjectDoesNotExistException odnee)
        {
            // NOOP
        }
    }

    @Test
    public void testPartitionableObjectStoreOperations() throws ObjectStoreException
    {
        final String testPartition = RandomStringUtils.randomAlphanumeric(20);

        // open and close are noops
        stringObjectStore.open(testPartition);
        stringObjectStore.close(testPartition);

        assertTrue(stringObjectStore.isPersistent());

        final String testKey = RandomStringUtils.randomAlphanumeric(20);
        final String testValue = RandomStringUtils.randomAlphanumeric(20);

        assertFalse(stringObjectStore.contains(testKey, testPartition));
        assertFalse(stringObjectStore.allKeys(testPartition).contains(testKey));

        try
        {
            stringObjectStore.retrieve(testKey, testPartition);
            fail("should have got an ObjectDoesNotExistException");
        }
        catch (final ObjectDoesNotExistException odnee)
        {
            // NOOP
        }

        stringObjectStore.store(testKey, testValue, testPartition);
        assertTrue(stringObjectStore.contains(testKey, testPartition));
        assertTrue(stringObjectStore.allKeys(testPartition).contains(testKey));

        try
        {
            stringObjectStore.store(testKey, testValue, testPartition);
            fail("should have got an ObjectAlreadyExistsException");
        }
        catch (final ObjectAlreadyExistsException oaee)
        {
            // NOOP
        }

        assertEquals(testValue, stringObjectStore.retrieve(testKey, testPartition));
        assertTrue(stringObjectStore.allPartitions().contains(testPartition));

        assertEquals(testValue, stringObjectStore.remove(testKey, testPartition));
        assertFalse(stringObjectStore.contains(testKey, testPartition));
        assertFalse(stringObjectStore.allKeys(testPartition).contains(testKey));

        try
        {
            stringObjectStore.remove(testKey, testPartition);
            fail("should have got an ObjectDoesNotExistException");
        }
        catch (final ObjectDoesNotExistException odnee)
        {
            // NOOP
        }

        stringObjectStore.store(testKey, testValue, testPartition);
        stringObjectStore.disposePartition(testPartition);
        assertFalse(stringObjectStore.contains(testKey, testPartition));
        assertFalse(stringObjectStore.allPartitions().contains(testPartition));
    }

    @Test
    public void testIdempotentFlow() throws Exception
    {
        final String payload1 = UUID.randomUUID().toString();
        final String payload2 = UUID.randomUUID().toString();

        final CountdownCallback cc = new CountdownCallback(2);
        final FunctionalTestComponent ftc = getFunctionalTestComponent("idempotentFlow");
        ftc.setEventCallback(cc);

        muleContext.getClient().dispatch("vm://idempotentFlow.in", payload1, null);
        muleContext.getClient().dispatch("vm://idempotentFlow.in", payload1, null);
        muleContext.getClient().dispatch("vm://idempotentFlow.in", payload2, null);

        cc.await(1000L * getTestTimeoutSecs());

        assertEquals(2, ftc.getReceivedMessagesCount());

        final Set<String> receivedPayloads = new HashSet<String>();
        receivedPayloads.add((String) ftc.getReceivedMessage(1));
        receivedPayloads.add((String) ftc.getReceivedMessage(2));

        assertEquals(new HashSet<String>(Arrays.asList(payload1, payload2)), receivedPayloads);
    }
}
