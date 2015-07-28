/*
 * Copyright 2015 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync.filter;

import com.emc.vipr.sync.ViPRSync;
import com.emc.vipr.sync.model.SyncAcl;
import com.emc.vipr.sync.test.TestObjectSource;
import com.emc.vipr.sync.test.TestObjectTarget;
import com.emc.vipr.sync.test.TestSyncObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AclMappingFilterTest {
    private SyncAcl sourceAcl1 = new SyncAcl();
    private SyncAcl sourceAcl2 = new SyncAcl();
    private SyncAcl sourceAcl3 = new SyncAcl();
    private SyncAcl targetAcl1 = new SyncAcl();
    private SyncAcl targetAcl2 = new SyncAcl();
    private SyncAcl targetAcl3 = new SyncAcl();
    private Random random = new Random();

    @Test
    public void testPatterns() {
        Pattern mapPattern = Pattern.compile(AclMappingFilter.MAP_PATTERN);
        Matcher m = mapPattern.matcher("user.joe123=jane@company.com");
        Assert.assertTrue(m.matches());
        Assert.assertEquals(3, m.groupCount());
        Assert.assertEquals("user", m.group(1));
        Assert.assertEquals("joe123", m.group(2));
        Assert.assertEquals("jane@company.com", m.group(3));
        m = mapPattern.matcher("user.joe@company.com=jane");
        Assert.assertTrue(m.matches());
        Assert.assertEquals(3, m.groupCount());
        Assert.assertEquals("user", m.group(1));
        Assert.assertEquals("joe@company.com", m.group(2));
        Assert.assertEquals("jane", m.group(3));
        m = mapPattern.matcher("group.guys=gals");
        Assert.assertTrue(m.matches());
        Assert.assertEquals(3, m.groupCount());
        Assert.assertEquals("group", m.group(1));
        Assert.assertEquals("guys", m.group(2));
        Assert.assertEquals("gals", m.group(3));
        m = mapPattern.matcher("permission.read=READ_ONLY");
        Assert.assertTrue(m.matches());
        Assert.assertEquals(3, m.groupCount());
        Assert.assertEquals("permission", m.group(1));
        Assert.assertEquals("read", m.group(2));
        Assert.assertEquals("READ_ONLY", m.group(3));
        m = mapPattern.matcher("permission.READ_WRITE=read,write");
        Assert.assertTrue(m.matches());
        Assert.assertEquals(3, m.groupCount());
        Assert.assertEquals("permission", m.group(1));
        Assert.assertEquals("READ_WRITE", m.group(2));
        Assert.assertEquals("read,write", m.group(3));
        m = mapPattern.matcher("permission1.read=READ_ONLY");
        Assert.assertTrue(m.matches());
        Assert.assertEquals(3, m.groupCount());
        Assert.assertEquals("permission1", m.group(1));
        Assert.assertEquals("read", m.group(2));
        Assert.assertEquals("READ_ONLY", m.group(3));
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

        AclMappingFilter aclMapper = new AclMappingFilter();
        aclMapper.setIncludeAcl(true);
        aclMapper.setAclMapFile(tempFile.getPath());
        aclMapper.setDomainToAppend("company.com");

        TestObjectSource testSource = new TestObjectSource(1000, 10240, null);
        tackAcls(testSource.getObjects());
        testSource.setIncludeAcl(true);
        TestObjectTarget target = new TestObjectTarget();
        target.setIncludeAcl(true);

        ViPRSync sync = new ViPRSync();
        sync.setSource(testSource);
        sync.setFilters(Arrays.asList((SyncFilter) aclMapper));
        sync.setTarget(target);
        sync.run();

        List<TestSyncObject> targetObjects = target.getRootObjects();

        verifyObjectAcls(targetObjects);
    }

    private void tackAcls(List<TestSyncObject> objects) throws Exception {
        for (TestSyncObject object : objects) {
            switch (random.nextInt(3)) {
                case 0:
                    object.getMetadata().setAcl((SyncAcl) sourceAcl1.clone());
                    break;
                case 1:
                    object.getMetadata().setAcl((SyncAcl) sourceAcl2.clone());
                    break;
                case 2:
                    object.getMetadata().setAcl((SyncAcl) sourceAcl3.clone());
                    break;
            }
            if (object.isDirectory()) tackAcls(object.getChildren());
        }
    }

    private void verifyObjectAcls(List<TestSyncObject> targetObjects) {
        for (TestSyncObject targetObject : targetObjects) {
            verifyAcls(targetObject);
            if (targetObject.isDirectory())
                verifyObjectAcls(targetObject.getChildren());
        }
    }

    private void verifyAcls(TestSyncObject targetObject) {
        Assert.assertNotNull(targetObject.getMetadata());
        SyncAcl targetAcl = targetObject.getMetadata().getAcl();
        Assert.assertNotNull(targetAcl);
        // only assert that the target ACL is one of the 3 expected (source ACL has been modified by filter, so we
        // can't verify which of the 3 it should be)
        Assert.assertTrue(targetAcl.equals(targetAcl1) || targetAcl.equals(targetAcl2) || targetAcl.equals(targetAcl3));
    }
}
