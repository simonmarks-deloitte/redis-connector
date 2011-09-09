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

import org.apache.commons.lang.RandomStringUtils;
import org.mule.api.store.ObjectAlreadyExistsException;
import org.mule.api.store.ObjectDoesNotExistException;
import org.mule.api.store.ObjectStore;
import org.mule.api.store.ObjectStoreException;
import org.mule.tck.FunctionalTestCase;

public class RedisObjectStoreITCase extends FunctionalTestCase {

    private ObjectStore<String> stringObjectStore;

    @Override
    protected String getConfigResources() {
        return "redis-objectstore-tests-config.xml";
    }

    @Override
    protected void doSetUp() throws Exception {
        super.doSetUp();

        stringObjectStore = muleContext.getRegistry().lookupObject(FakeObjectStoreUser.class).getObjectStore();
    }

    public void testObjectStoreOperations() throws ObjectStoreException {
        assertTrue(stringObjectStore.isPersistent());

        final String testKey = RandomStringUtils.randomAlphanumeric(20);
        final String testValue = RandomStringUtils.randomAlphanumeric(20);

        assertFalse(stringObjectStore.contains(testKey));

        try {
            stringObjectStore.retrieve(testKey);
            fail("should have got an ObjectDoesNotExistException");
        } catch (final ObjectDoesNotExistException odnee) {
            // NOOP
        }

        stringObjectStore.store(testKey, testValue);
        assertTrue(stringObjectStore.contains(testKey));

        try {
            stringObjectStore.store(testKey, testValue);
            fail("should have got an ObjectAlreadyExistsException");
        } catch (final ObjectAlreadyExistsException oaee) {
            // NOOP
        }

        assertEquals(testValue, stringObjectStore.retrieve(testKey));

        assertEquals(testValue, stringObjectStore.remove(testKey));
        assertFalse(stringObjectStore.contains(testKey));

        try {
            stringObjectStore.remove(testKey);
            fail("should have got an ObjectDoesNotExistException");
        } catch (final ObjectDoesNotExistException odnee) {
            // NOOP
        }
    }
}
