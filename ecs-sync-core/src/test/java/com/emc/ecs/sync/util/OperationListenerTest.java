/*
 * Copyright (c) 2022 Dell Inc. or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.ecs.sync.util;

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.RoleType;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.storage.TestStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class OperationListenerTest {
    @Test
    public void testTestOperations() {
        AtomicInteger sourceGetCount = new AtomicInteger(), targetGetCount = new AtomicInteger(),
                sourcePutCount = new AtomicInteger(), targetPutCount = new AtomicInteger(),
                readSourceCount = new AtomicInteger();

        OperationListener listener = operationDetails -> {
            Assertions.assertNotNull(operationDetails);
            Assertions.assertNotNull(operationDetails.getRole());
            Assertions.assertNotNull(operationDetails.getOperation());
            Assertions.assertNotNull(operationDetails.getIdentifier());
            Assertions.assertTrue(operationDetails.getDurationMs() >= 0);
            Assertions.assertNull(operationDetails.getException());

            if (TestStorage.OPERATION_GET_KEY.equals(operationDetails.getOperation())) {
                if (RoleType.Source == operationDetails.getRole()) sourceGetCount.incrementAndGet();
                else targetGetCount.incrementAndGet();
            } else if (TestStorage.OPERATION_PUT_KEY.equals(operationDetails.getOperation())) {
                if (RoleType.Source == operationDetails.getRole()) sourcePutCount.incrementAndGet();
                else targetPutCount.incrementAndGet();
            } else if (TestStorage.OPERATION_READ_FROM_SOURCE.equals(operationDetails.getOperation())) {
                readSourceCount.incrementAndGet();
                Assertions.assertEquals(RoleType.Target, operationDetails.getRole());
            } else {
                Assertions.fail("unknown operation: " + operationDetails.getOperation());
            }
        };

        SyncConfig syncConfig = new SyncConfig()
                .withSource(new TestConfig()
                        .withChanceOfChildren(0)
                        .withObjectCount(100)
                        .withDiscardData(false))
                .withTarget(new TestConfig().withDiscardData(false))
                .withOptions(new SyncOptions().withVerify(true));
        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.addOperationListener(listener);
        sync.run();

        // note that in TestStorage, there is no operation to get the data from TestSyncObject, so the "verify" option
        // will not add a read call
        Assertions.assertEquals(100, sourceGetCount.get());
        Assertions.assertEquals(200, targetGetCount.get());
        Assertions.assertEquals(100, sourcePutCount.get());
        Assertions.assertEquals(100, targetPutCount.get());
        Assertions.assertEquals(100, readSourceCount.get());
    }
}
