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
package com.emc.vipr.sync.test;

import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.SyncAcl;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.target.SyncTarget;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.File;
import java.util.*;

public class TestObjectSource extends SyncSource<TestSyncObject> {
    public static final char[] ALPHA_NUM_CHARS = "0123456789abcdefghijklmnopqrstuvwxyz".toCharArray();

    public static final int MAX_DEPTH = 5;
    public static final int MAX_CHILD_COUNT = 8;
    public static final int CHANCE_OF_CHILDREN = 30;
    public static final int MAX_META_TAGS = 5;

    private static final Random random = new Random();

    private String objectOwner;
    private SyncAcl aclTemplate;
    private List<String> validUsers;
    private List<String> validGroups;
    private List<String> validPermissions;
    private List<TestSyncObject> objects;

    public TestObjectSource(int objectCount, int maxSize, String objectOwner) {
        this(objectCount, maxSize, objectOwner, null, null, null, null);
    }

    public TestObjectSource(int objectCount, int maxSize, String objectOwner, SyncAcl aclTemplate, List<String> validUsers, List<String> validGroups, List<String> validPermissions) {
        this.objectOwner = objectOwner;
        this.aclTemplate = aclTemplate;
        this.validUsers = validUsers;
        this.validGroups = validGroups;
        this.validPermissions = validPermissions;
        objects = generateRandomObjects(null, objectCount, maxSize, 1);
    }

    public TestObjectSource(List<TestSyncObject> objects) {
        this.objects = objects;
    }

    @Override
    public boolean canHandleSource(String sourceUri) {
        return false;
    }

    @Override
    public Iterator<TestSyncObject> childIterator(TestSyncObject syncObject) {
        return syncObject.getChildren().iterator();
    }

    @Override
    public Iterator<TestSyncObject> iterator() {
        return new DeepCopyIterator(objects.iterator());
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getDocumentation() {
        return null;
    }

    @Override
    public Options getCustomOptions() {
        return null;
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
    }

    private List<TestSyncObject> generateRandomObjects(String parentPath, int levelCount, int maxSize, int level) {
        List<TestSyncObject> objects = new ArrayList<TestSyncObject>();
        if (level <= MAX_DEPTH) {
            for (int i = 0; i < levelCount; i++) {
                boolean hasChildren = random.nextInt(100) < CHANCE_OF_CHILDREN;

                String path = new File(parentPath, "random" + i + (hasChildren ? ".dir" : ".object")).getPath();

                TestSyncObject object = new TestSyncObject(path, path,
                        hasChildren ? null : randomData(maxSize),
                        hasChildren ? generateRandomObjects(path, random.nextInt(MAX_CHILD_COUNT), maxSize, level + 1) : null);
                object.setMetadata(randomMetadata());

                objects.add(object);
            }
        }
        return objects;
    }

    private byte[] randomData(int maxSize) {
        byte[] data = new byte[random.nextInt(maxSize)];
        random.nextBytes(data);
        return data;
    }

    private SyncMetadata randomMetadata() {
        SyncMetadata metadata = new SyncMetadata();

        metadata.setContentType("application/octet-stream");

        metadata.setModificationTime(new Date());

        metadata.setAcl(randomAcl());

        if (random.nextBoolean())
            metadata.setExpirationDate(new Date(System.currentTimeMillis() + random.nextInt(1000000) + 100000));

        for (int i = 0; i < MAX_META_TAGS; i++) {
            String key = randChars(random.nextInt(10) + 5, true); // objectives of this test does not include UTF-8 metadata keys
            String value = randChars(random.nextInt(20) + 5, false);
            metadata.setUserMetadataValue(key, value);
        }

        return metadata;
    }

    private SyncAcl randomAcl() {
        SyncAcl acl;
        try {
            acl = (aclTemplate == null) ? new SyncAcl() : (SyncAcl) aclTemplate.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        if (objectOwner != null) {
            acl.setOwner(objectOwner);

            if (validPermissions != null && !validPermissions.isEmpty()) {
                acl.addUserGrant(objectOwner, validPermissions.get(validPermissions.size() - 1));

                if (validUsers != null && !validUsers.isEmpty()) {
                    List<String> users = new ArrayList<String>(validUsers);
                    int numUsers = random.nextInt(Math.min(validUsers.size(), 3));
                    for (int i = 0; i < numUsers; i++) {
                        int userIdx = random.nextInt(users.size());
                        acl.addUserGrant(users.get(userIdx), validPermissions.get(random.nextInt(validPermissions.size())));
                        users.remove(userIdx);
                    }
                }

                if (validGroups != null && !validGroups.isEmpty()) {
                    List<String> groups = new ArrayList<String>(validGroups);
                    int numGroups = random.nextInt(Math.min(validGroups.size(), 3));
                    for (int i = 0; i < numGroups; i++) {
                        int groupIdx = random.nextInt(groups.size());
                        acl.addGroupGrant(groups.get(groupIdx), validPermissions.get(random.nextInt(validPermissions.size())));
                        groups.remove(groupIdx);
                    }
                }
            }
        }
        return acl;
    }

    private String randChars(int count, boolean alphaNumOnly) {
        char[] chars = new char[count];
        for (int i = 0; i < chars.length; i++) {
            if (alphaNumOnly) {
                chars[i] = ALPHA_NUM_CHARS[random.nextInt(36)];
            } else {
                chars[i] = (char) (' ' + random.nextInt(95));
                if (chars[i] == '+') chars[i] = '=';
            }
        }
        return new String(chars);
    }

    public List<TestSyncObject> getObjects() {
        return objects;
    }

    public String getObjectOwner() {
        return objectOwner;
    }

    public SyncAcl getAclTemplate() {
        return aclTemplate;
    }

    public List<String> getValidGroups() {
        return validGroups;
    }

    public List<String> getValidPermissions() {
        return validPermissions;
    }

    public List<String> getValidUsers() {
        return validUsers;
    }

    private class DeepCopyIterator implements Iterator<TestSyncObject> {
        private Iterator<TestSyncObject> delegate;

        public DeepCopyIterator(Iterator<TestSyncObject> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public TestSyncObject next() {
            try {
                return delegate.next().deepCopy();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException("deep copy failed", e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("cannot remove from original collection");
        }
    }
}
