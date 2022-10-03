/*
 * Copyright (c) 2017-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.filter.PathShardingConfig;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.model.ObjectSummary;
import com.emc.ecs.sync.storage.TestStorage;
import com.emc.ecs.sync.test.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.xml.bind.DatatypeConverter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;

public class PathShardingTest {
    @Test
    public void testPathSharding() throws Exception {
        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setSource(new TestConfig().withDiscardData(false).withReadData(false).withChanceOfChildren(0).withObjectCount(10));
        syncConfig.setTarget(new TestConfig().withDiscardData(false).withReadData(false));
        syncConfig.setFilters(Collections.singletonList(new PathShardingConfig())); // use defaults

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        TestUtil.run(sync);

        Assertions.assertEquals(10, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());

        TestStorage source = (TestStorage) sync.getSource();
        TestStorage target = (TestStorage) sync.getTarget();

        for (ObjectSummary summary : source.allObjects()) {
            String relPath = source.getRelativePath(summary.getIdentifier(), false);
            byte[] md5Bin = MessageDigest.getInstance("MD5").digest(relPath.getBytes(StandardCharsets.UTF_8));
            String md5 = DatatypeConverter.printHexBinary(md5Bin).toLowerCase();
            // defaults is 2 subdirs with 2 chars each
            String shardedPath = md5.substring(0, 2) + "/" + md5.substring(2, 4) + "/" + relPath;
            // make sure the sharded path exists in target (should be sufficient)
            Assertions.assertNotNull(target.loadObject(target.getIdentifier(shardedPath, false)));
        }
    }
}
