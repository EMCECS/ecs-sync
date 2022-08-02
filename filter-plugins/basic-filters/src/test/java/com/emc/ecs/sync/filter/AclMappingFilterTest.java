/*
 * Copyright (c) 2014-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.filter.AclMappingConfig;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.model.ObjectAcl;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.TestStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AclMappingFilterTest {
    private ObjectAcl sourceAcl1 = new ObjectAcl();
    private ObjectAcl sourceAcl2 = new ObjectAcl();
    private ObjectAcl sourceAcl3 = new ObjectAcl();
    private ObjectAcl targetAcl1 = new ObjectAcl();
    private ObjectAcl targetAcl2 = new ObjectAcl();
    private ObjectAcl targetAcl3 = new ObjectAcl();
    private Random random = new Random();

    @Test
    public void testPatterns() {
        Pattern mapPattern = Pattern.compile(AclMappingFilter.MAP_PATTERN);
        Matcher m = mapPattern.matcher("user.joe123=jane@company.com");
        Assertions.assertTrue(m.matches());
        Assertions.assertEquals(3, m.groupCount());
        Assertions.assertEquals("user", m.group(1));
        Assertions.assertEquals("joe123", m.group(2));
        Assertions.assertEquals("jane@company.com", m.group(3));
        m = mapPattern.matcher("user.joe@company.com=jane");
        Assertions.assertTrue(m.matches());
        Assertions.assertEquals(3, m.groupCount());
        Assertions.assertEquals("user", m.group(1));
        Assertions.assertEquals("joe@company.com", m.group(2));
        Assertions.assertEquals("jane", m.group(3));
        m = mapPattern.matcher("group.guys=gals");
        Assertions.assertTrue(m.matches());
        Assertions.assertEquals(3, m.groupCount());
        Assertions.assertEquals("group", m.group(1));
        Assertions.assertEquals("guys", m.group(2));
        Assertions.assertEquals("gals", m.group(3));
        m = mapPattern.matcher("permission.read=READ_ONLY");
        Assertions.assertTrue(m.matches());
        Assertions.assertEquals(3, m.groupCount());
        Assertions.assertEquals("permission", m.group(1));
        Assertions.assertEquals("read", m.group(2));
        Assertions.assertEquals("READ_ONLY", m.group(3));
        m = mapPattern.matcher("permission.READ_WRITE=read,write");
        Assertions.assertTrue(m.matches());
        Assertions.assertEquals(3, m.groupCount());
        Assertions.assertEquals("permission", m.group(1));
        Assertions.assertEquals("READ_WRITE", m.group(2));
        Assertions.assertEquals("read,write", m.group(3));
        m = mapPattern.matcher("permission1.read=READ_ONLY");
        Assertions.assertTrue(m.matches());
        Assertions.assertEquals(3, m.groupCount());
        Assertions.assertEquals("permission1", m.group(1));
        Assertions.assertEquals("read", m.group(2));
        Assertions.assertEquals("READ_ONLY", m.group(3));
    }

    @Test
    public void testFilter() throws Exception {
        // test user and group name mapping
        sourceAcl1.setOwner("joe");
        sourceAcl1.addUserGrant("joe", "not_mapped");
        sourceAcl1.addGroupGrant("guys", "not_mapped");
        targetAcl1.setOwner("jane@company.com");
        targetAcl1.addUserGrant("jane@company.com", "not_mapped");
        targetAcl1.addGroupGrant("gals", "not_mapped");

        // test one-to-one permission mapping
        sourceAcl2.setOwner("bob");
        sourceAcl2.addUserGrant("bob", "all");
        sourceAcl2.addGroupGrant("guys", "read");
        targetAcl2.setOwner("bob@company.com");
        targetAcl2.addUserGrant("bob@company.com", "EVERYTHING");
        targetAcl2.addGroupGrant("gals", "READ_ONLY");

        // test removal and pare-down
        sourceAcl3.setOwner("bob");
        sourceAcl3.addUserGrant("bob", "all");
        sourceAcl3.addUserGrant("remove_me", "all");
        sourceAcl3.addGroupGrant("guys", "read");
        sourceAcl3.addGroupGrant("guys", "write");
        sourceAcl3.addGroupGrant("bad_guys", "all");
        targetAcl3.setOwner("bob@company.com");
        targetAcl3.addUserGrant("bob@company.com", "EVERYTHING");
        targetAcl3.addGroupGrant("gals", "READ_WRITE");

        // write mapping file
        File tempFile = File.createTempFile("map-file", null);
        tempFile.deleteOnExit();

        BufferedWriter mapFile = new BufferedWriter(new FileWriter(tempFile));
        mapFile.write("user.joe=jane\n");
        mapFile.write("user.remove_me=\n");
        mapFile.write("group.guys=gals\n");
        mapFile.write("group.bad_guys=\n");
        mapFile.write("permission.all=EVERYTHING\n");
        mapFile.write("permission1.write=READ_WRITE\n");
        mapFile.write("permission1.read=READ_ONLY\n");
        mapFile.close();

        AclMappingConfig aclConfig = new AclMappingConfig();
        aclConfig.setAclMapFile(tempFile.getPath());
        aclConfig.setAclAppendDomain("company.com");

        TestConfig testConfig = new TestConfig().withObjectCount(100).withMaxSize(10240).withDiscardData(false);
        TestStorage source = new TestStorage();
        source.setConfig(testConfig);
        source.configure(source, null, null);
        tackAcls(source, source.getRootObjects());

        SyncConfig syncConfig = new SyncConfig().withTarget(new TestConfig().withReadData(true).withDiscardData(false))
                .withOptions(new SyncOptions().withSyncAcl(true))
                .withFilters(Collections.singletonList((Object) aclConfig));

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.setSource(source);
        sync.run();

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(sync.getEstimatedTotalObjects(), sync.getStats().getObjectsComplete());

        verifyObjectAcls((TestStorage) sync.getTarget(), ((TestStorage) sync.getTarget()).getRootObjects());
    }

    private void tackAcls(TestStorage storage, Collection<? extends SyncObject> objects) throws Exception {
        for (SyncObject object : objects) {
            switch (random.nextInt(3)) {
                case 0:
                    object.setAcl((ObjectAcl) sourceAcl1.clone());
                    break;
                case 1:
                    object.setAcl((ObjectAcl) sourceAcl2.clone());
                    break;
                case 2:
                    object.setAcl((ObjectAcl) sourceAcl3.clone());
                    break;
            }
            if (object.getMetadata().isDirectory())
                tackAcls(storage, storage.getChildren(storage.getIdentifier(object.getRelativePath(), object.getMetadata().isDirectory())));
        }
    }

    private void verifyObjectAcls(TestStorage storage, Collection<? extends SyncObject> targetObjects) {
        for (SyncObject targetObject : targetObjects) {
            verifyAcls(targetObject);
            if (targetObject.getMetadata().isDirectory())
                verifyObjectAcls(storage, storage.getChildren(storage.getIdentifier(targetObject.getRelativePath(), true)));
        }
    }

    private void verifyAcls(SyncObject targetObject) {
        Assertions.assertNotNull(targetObject.getMetadata());
        ObjectAcl targetAcl = targetObject.getAcl();
        Assertions.assertNotNull(targetAcl);
        // only assert that the target ACL is one of the 3 expected (source ACL has been modified by filter, so we
        // can't verify which of the 3 it should be)
        Assertions.assertTrue(targetAcl.equals(targetAcl1) || targetAcl.equals(targetAcl2) || targetAcl.equals(targetAcl3));
    }
}
