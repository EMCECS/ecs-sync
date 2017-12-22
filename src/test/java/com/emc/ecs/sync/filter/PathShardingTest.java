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
package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.filter.PathShardingConfig;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.model.ObjectSummary;
import com.emc.ecs.sync.storage.TestStorage;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Assert;
import org.junit.Test;

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
        sync.run();

        Assert.assertEquals(10, sync.getStats().getObjectsComplete());
        Assert.assertEquals(0, sync.getStats().getObjectsFailed());

        TestStorage source = (TestStorage) sync.getSource();
        TestStorage target = (TestStorage) sync.getTarget();

        for (ObjectSummary summary : source.allObjects()) {
            String relPath = source.getRelativePath(summary.getIdentifier(), false);
            String md5 = DigestUtils.md5Hex(relPath);
            // defaults is 2 subdirs with 2 chars each
            String shardedPath = md5.substring(0, 2) + "/" + md5.substring(2, 4) + "/" + relPath;
            // make sure the sharded path exists in target (should be sufficient)
            Assert.assertNotNull(target.loadObject(target.getIdentifier(shardedPath, false)));
        }
    }
}
