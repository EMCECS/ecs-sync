/*
 * Copyright (c) 2021-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync;

import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.storage.TestStorage;
import com.emc.ecs.sync.test.TestUtil;
import com.emc.ecs.sync.util.VerifyUtil;
import org.junit.jupiter.api.Test;

public class EndToEndTest extends AbstractEndToEndTest {
    @Test
    public void testTestPlugins() {
        TestConfig config = new TestConfig().withObjectCount(SM_OBJ_COUNT).withMaxSize(SM_OBJ_MAX_SIZE)
                .withReadData(true).withDiscardData(false);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withSource(config).withTarget(config));
        TestUtil.run(sync);

        TestStorage source = (TestStorage) sync.getSource();
        TestStorage target = (TestStorage) sync.getTarget();

        VerifyUtil.verifyObjects(source, source.getRootObjects(), target, target.getRootObjects(), true);
    }
}
