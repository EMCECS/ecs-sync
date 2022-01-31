/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.emc.ecs.sync.util;

import com.emc.ecs.sync.model.ObjectAcl;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.TestStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

public class LazyTest {
    @Test
    public void testLazyValue() {
        final AtomicBoolean initialized = new AtomicBoolean(false);

        LazyValue<Boolean> value = new LazyValue<Boolean>() {
            @Override
            public Boolean get() {
                initialized.set(true);
                return initialized.get();
            }
        };

        Assertions.assertFalse(initialized.get());

        Assertions.assertTrue(value.get());

        Assertions.assertTrue(initialized.get());
    }

    @Test
    public void testLazyObject() throws Exception {
        final AtomicBoolean streamInitialized = new AtomicBoolean(false);
        final AtomicBoolean aclInitialized = new AtomicBoolean(false);

        LazyValue<InputStream> lazyStream = new LazyValue<InputStream>() {
            @Override
            public InputStream get() {
                streamInitialized.set(true);
                return new InputStream() {
                    @Override
                    public int read() throws IOException {
                        return -1;
                    }
                };
            }
        };

        LazyValue<ObjectAcl> lazyAcl = new LazyValue<ObjectAcl>() {
            @Override
            public ObjectAcl get() {
                aclInitialized.set(true);
                return new ObjectAcl();
            }
        };

        SyncObject object = new SyncObject(new TestStorage(), "foo", new ObjectMetadata()).withLazyStream(lazyStream).withLazyAcl(lazyAcl);

        Assertions.assertFalse(aclInitialized.get());
        Assertions.assertFalse(streamInitialized.get());

        InputStream stream = object.getDataStream();
        Assertions.assertTrue(streamInitialized.get());

        // just for safety
        Assertions.assertEquals(-1, stream.read(new byte[1]));

        object.getAcl();
        Assertions.assertTrue(aclInitialized.get());
    }
}
