/*
 * Copyright (c) 2016-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.storage;

import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.ObjectAcl;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.ObjectSummary;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.util.LazyValue;
import com.emc.ecs.sync.util.RandomInputStream;
import com.emc.ecs.sync.util.SyncUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

public class TestStorage extends AbstractStorage<TestConfig> {
    private static final Logger log = LoggerFactory.getLogger(TestStorage.class);

    public static final String ROOT_PATH = "/root";

    private static final char[] ALPHA_NUM_CHARS = "0123456789abcdefghijklmnopqrstuvwxyz".toCharArray();

    public static final String OPERATION_GET_KEY = "TestStorageGetKey";
    public static final String OPERATION_PUT_KEY = "TestStoragePutKey";
    public static final String OPERATION_READ_FROM_SOURCE = "TestStorageReadFromSource";

    private ObjectAcl aclTemplate;
    private final Map<String, TestSyncObject> idMap =
            Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Set<TestSyncObject>> childrenMap =
            Collections.synchronizedMap(new HashMap<>());

    @Override
    public String getRelativePath(String identifier, boolean directory) {
        return identifier.substring(ROOT_PATH.length() + 1);
    }

    @Override
    public String getIdentifier(String relativePath, boolean directory) {
        if (relativePath.isEmpty()) return ROOT_PATH;
        return ROOT_PATH + "/" + relativePath;
    }

    @Override
    public void configure(SyncStorage<?> source, Iterator<? extends SyncFilter<?>> filters, SyncStorage<?> target) {
        super.configure(source, filters, target);

        if (config.getMinSize() > config.getMaxSize())
            throw new ConfigurationException("min-size cannot be greater than max-size");

        if (!config.isDiscardData() && config.getMaxSize() > Integer.MAX_VALUE)
            throw new ConfigurationException("If max-size is greater than 2GB, you must discard data");

        if (config.isDiscardData() && (options.isVerify() || options.isVerifyOnly()))
            throw new ConfigurationException("You must not discard data if you wish to verify");

        if (this == source && idMap.isEmpty()) generateRandomObjects(ROOT_PATH, config.getObjectCount(), 1);
    }

    @Override
    protected ObjectSummary createSummary(String identifier) {
        return createSummary((TestSyncObject) loadObject(identifier));
    }

    private ObjectSummary createSummary(TestSyncObject object) {
        return new ObjectSummary(getIdentifier(object.getRelativePath(), object.getMetadata().isDirectory()),
                object.getMetadata().isDirectory(), object.getMetadata().getContentLength());
    }

    @Override
    public Iterable<ObjectSummary> allObjects() {
        List<ObjectSummary> summaries = new ArrayList<>();
        for (TestSyncObject rootObject : getRootObjects()) {
            summaries.add(createSummary(rootObject));
        }
        return summaries;
    }

    @Override
    public Iterable<ObjectSummary> children(ObjectSummary parent) {
        List<ObjectSummary> children = new ArrayList<>();
        for (TestSyncObject child : getChildren(parent.getIdentifier())) {
            children.add(createSummary(child));
        }
        return children;
    }

    @Override
    public SyncObject loadObject(String identifier) throws ObjectNotFoundException {
        TestSyncObject object = operationWrapper(() -> idMap.get(identifier), OPERATION_GET_KEY, null, identifier);
        if (object == null) throw new ObjectNotFoundException(identifier);
        return object.deepCopy();
    }

    @Override
    public void updateObject(String identifier, SyncObject object) {
        try {
            byte[] data = null;
            if (config.isReadData() && !object.getMetadata().isDirectory()) {
                data = operationWrapper((Callable<byte[]>) () -> {
                    if (config.isDiscardData()) {
                        SyncUtil.consumeAndCloseStream(object.getDataStream());
                        return null;
                    } else return SyncUtil.readAsBytes(object.getDataStream());
                }, OPERATION_READ_FROM_SOURCE, object, identifier);
            }

            if (!config.isDiscardData()) {
                TestSyncObject testObject = new TestSyncObject(this, object.getRelativePath(), object.getMetadata(), data);
                if (object.getAcl() != null) testObject.setAcl((ObjectAcl) object.getAcl().clone());
                ingest(identifier, testObject);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int getTotalObjectCount() {
        return idMap.size();
    }

    private void generateRandomObjects(String parentPath, long levelCount, int level) {
        if (level <= config.getMaxDepth()) {
            for (int i = 0; i < levelCount; i++) {
                boolean hasChildren = ThreadLocalRandom.current().nextInt(100) < config.getChanceOfChildren();

                String path = SyncUtil.combinedPath(parentPath, "random" + i + (hasChildren ? ".dir" : ".object"));

                log.info("generating object {}", path);

                // if min and max are same, or max is 0, set size to max
                long size = config.getMaxSize();
                // otherwise, pick random size between min and max
                if (config.getMaxSize() > 0 && config.getMaxSize() > config.getMinSize())
                    size = ThreadLocalRandom.current().nextLong(config.getMaxSize() - config.getMinSize() + 1) + config.getMinSize();

                ObjectMetadata metadata = randomMetadata(hasChildren, hasChildren ? 0 : size);
                ObjectAcl acl = randomAcl();

                TestSyncObject testSyncObject;
                if (config.isDiscardData()) {
                    testSyncObject = new TestSyncObject(this, getRelativePath(path, hasChildren), metadata);
                } else {
                    testSyncObject = new TestSyncObject(this, getRelativePath(path, hasChildren), metadata, randomData((int) size));
                }
                testSyncObject.setAcl(acl);

                ingest(path, testSyncObject);

                if (hasChildren)
                    generateRandomObjects(path, ThreadLocalRandom.current().nextInt(config.getMaxChildCount()), level + 1);
            }
        }
    }

    private byte[] randomData(int size) {
        byte[] data = new byte[size];
        ThreadLocalRandom.current().nextBytes(data);
        return data;
    }

    private ObjectMetadata randomMetadata(boolean directory, long size) {
        ObjectMetadata metadata = new ObjectMetadata();

        metadata.setDirectory(directory);

        metadata.setContentLength(size);

        metadata.setContentType("application/octet-stream");

        metadata.setModificationTime(new Date());
        metadata.setAccessTime(new Date());
        metadata.setMetaChangeTime(new Date());

        if (ThreadLocalRandom.current().nextBoolean())
            metadata.setExpirationDate(new Date(System.currentTimeMillis() + ThreadLocalRandom.current().nextInt(1000000) + 100000));

        for (int i = 0; i < config.getMaxMetadata(); i++) {
            String key = randChars(ThreadLocalRandom.current().nextInt(10) + 5, true); // objectives of this test does not include UTF-8 metadata keys
            String value = randChars(ThreadLocalRandom.current().nextInt(20) + 5, false);
            metadata.setUserMetadataValue(key, value);
        }

        return metadata;
    }

    private ObjectAcl randomAcl() {
        ObjectAcl acl;
        try {
            acl = (aclTemplate == null) ? new ObjectAcl() : (ObjectAcl) aclTemplate.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        String objectOwner = config.getObjectOwner();
        if (objectOwner != null) {
            acl.setOwner(objectOwner);

            List<String> validPermissions = new ArrayList<>();
            if (config.getValidPermissions() != null) validPermissions = Arrays.asList(config.getValidPermissions());
            List<String> validUsers = new ArrayList<>();
            if (config.getValidUsers() != null) validUsers = Arrays.asList(config.getValidUsers());
            List<String> validGroups = new ArrayList<>();
            if (config.getValidGroups() != null) validGroups = Arrays.asList(config.getValidGroups());
            if (!validPermissions.isEmpty()) {
                acl.addUserGrant(objectOwner, validPermissions.get(validPermissions.size() - 1));

                if (!validUsers.isEmpty()) {
                    List<String> users = new ArrayList<>(validUsers);
                    int numUsers = ThreadLocalRandom.current().nextInt(Math.min(validUsers.size(), 3));
                    for (int i = 0; i < numUsers; i++) {
                        int userIdx = ThreadLocalRandom.current().nextInt(users.size());
                        acl.addUserGrant(users.get(userIdx), validPermissions.get(ThreadLocalRandom.current().nextInt(validPermissions.size())));
                        users.remove(userIdx);
                    }
                }

                if (!validGroups.isEmpty()) {
                    List<String> groups = new ArrayList<>(validGroups);
                    int numGroups = ThreadLocalRandom.current().nextInt(Math.min(validGroups.size(), 3));
                    for (int i = 0; i < numGroups; i++) {
                        int groupIdx = ThreadLocalRandom.current().nextInt(groups.size());
                        acl.addGroupGrant(groups.get(groupIdx), validPermissions.get(ThreadLocalRandom.current().nextInt(validPermissions.size())));
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
                chars[i] = ALPHA_NUM_CHARS[ThreadLocalRandom.current().nextInt(36)];
            } else {
                chars[i] = (char) (' ' + ThreadLocalRandom.current().nextInt(95));
                if (chars[i] == '+') chars[i] = '=';
            }
        }
        return new String(chars);
    }

    private void mkdirs(String path) {
        String parent = SyncUtil.parentPath(path);
        // don't need to create the root path
        if (ROOT_PATH.equals(parent) || ROOT_PATH.equals(path)) return;
        mkdirs(parent);
        synchronized (this) {
            String parentParent = SyncUtil.parentPath(parent);
            if (parentParent == null) parentParent = "";
            // find parent among grandparent's children
            for (TestSyncObject object : getChildren(parentParent)) {
                if (parent.equals(getIdentifier(object.getRelativePath(), true))) return;
            }
            // create parent
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setDirectory(true);
            addChild(parentParent, new TestSyncObject(this, getRelativePath(parent, true), metadata, null));
        }
    }

    public synchronized Set<TestSyncObject> getChildren(String identifier) {
        return childrenMap.computeIfAbsent(identifier, k -> Collections.synchronizedSet(new HashSet<>()));
    }

    public void ingest(TestStorage source, String identifier) {
        Collection<TestSyncObject> objects = (identifier == null) ? source.getRootObjects() : source.getChildren(identifier);
        for (SyncObject object : objects) {
            String childIdentifier = getIdentifier(object.getRelativePath(), object.getMetadata().isDirectory());
            updateObject(childIdentifier, object);
            if (object.getMetadata().isDirectory()) ingest(source, childIdentifier);
        }
    }

    private void ingest(String identifier, TestSyncObject testObject) {
        // equivalent of mkdirs()
        mkdirs(identifier);

        // add to lookup
        operationWrapper(() -> idMap.put(identifier, testObject), OPERATION_PUT_KEY, testObject, identifier);

        // add to parent
        addChild(SyncUtil.parentPath(identifier), testObject);
    }

    private synchronized void addChild(String parentPath, TestSyncObject object) {
        Set<TestSyncObject> children = getChildren(parentPath);
        children.remove(object); // in case mkdirs already created a directory that is now being sync'd
        children.add(object);
    }

    public List<TestSyncObject> getRootObjects() {
        return new ArrayList<>(getChildren(ROOT_PATH));
    }

    public ObjectAcl getAclTemplate() {
        return aclTemplate;
    }

    public void setAclTemplate(ObjectAcl aclTemplate) {
        this.aclTemplate = aclTemplate;
    }

    public TestStorage withAclTemplate(ObjectAcl aclTemplate) {
        setAclTemplate(aclTemplate);
        return this;
    }

    public class TestSyncObject extends SyncObject {
        private byte[] data;

        public TestSyncObject(SyncStorage source, String relativePath, final ObjectMetadata metadata) {
            super(source, relativePath, metadata);
            if (!metadata.isDirectory()) {
                setLazyStream(new LazyValue<InputStream>() {
                    @Override
                    public InputStream get() {
                        return new RandomInputStream(metadata.getContentLength());
                    }
                });
            }
        }

        public TestSyncObject(SyncStorage source, String relativePath, ObjectMetadata metadata, byte[] data) {
            super(source, relativePath, metadata);
            this.data = data;
            if (data != null) setDataStream(new ByteArrayInputStream(data));
        }

        public byte[] getData() {
            return data;
        }

        /**
         * For cases when you don't want the sync to modify the original objects (perhaps you're comparing them to the
         * result of a sync)
         */
        public TestSyncObject deepCopy() {
            try {
                TestSyncObject object;

                if (config.isDiscardData())
                    object = new TestSyncObject(getSource(), getRelativePath(), copyMetadata());

                else
                    object = new TestSyncObject(getSource(), getRelativePath(), copyMetadata(),
                            (data == null ? null : Arrays.copyOf(data, data.length)));

                if (getAcl() != null) object.setAcl((ObjectAcl) getAcl().clone());

                return object;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }

        private ObjectMetadata copyMetadata() {
            ObjectMetadata metadata = getMetadata();
            ObjectMetadata metaCopy = new ObjectMetadata();
            metaCopy.setChecksum(metadata.getChecksum()); // might be hard to duplicate a running checksum
            metaCopy.setCacheControl(metadata.getCacheControl());
            metaCopy.setContentDisposition(metadata.getContentDisposition());
            metaCopy.setContentEncoding(metadata.getContentEncoding());
            metaCopy.setContentLength(metadata.getContentLength());
            metaCopy.setContentType(metadata.getContentType());
            metaCopy.setDirectory(metadata.isDirectory());
            metaCopy.setExpirationDate(metadata.getExpirationDate());
            metaCopy.setHttpExpires(metadata.getHttpExpires());
            metaCopy.setExpirationDate(metadata.getExpirationDate());
            metaCopy.setAccessTime(metadata.getAccessTime());
            metaCopy.setMetaChangeTime(metadata.getMetaChangeTime());
            metaCopy.setModificationTime(metadata.getModificationTime());
            metaCopy.setRetentionEndDate(metadata.getRetentionEndDate());
            metaCopy.setRetentionPolicy(metadata.getRetentionPolicy());
            metaCopy.setHttpEtag(metadata.getHttpEtag());
            for (String key : metadata.getUserMetadata().keySet()) {
                ObjectMetadata.UserMetadata um = metadata.getUserMetadata().get(key);
                metaCopy.setUserMetadataValue(key, um.getValue(), um.isIndexed());
            }
            return metaCopy;
        }
    }
}
