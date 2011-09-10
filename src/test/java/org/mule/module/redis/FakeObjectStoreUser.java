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

import org.mule.api.store.PartitionableObjectStore;
import org.springframework.beans.factory.annotation.Required;

public class FakeObjectStoreUser {

    private PartitionableObjectStore<String> objectStore;

    public PartitionableObjectStore<String> getObjectStore() {
        return objectStore;
    }

    @Required
    public void setObjectStore(final PartitionableObjectStore<String> objectStore) {
        this.objectStore = objectStore;
    }
}
