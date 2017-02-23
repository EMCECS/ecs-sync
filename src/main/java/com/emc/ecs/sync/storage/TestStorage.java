/*
 * Copyright 2013-2016 EMC Corporation. All Rights Reserved.
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
import com.emc.util.StreamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class TestStorage extends AbstractStorage<TestConfig> {
    private static final Logger log = LoggerFactory.getLogger(TestStorage.class);

    private static final String ROOT_PATH = "/root";

    private static final char[] ALPHA_NUM_CHARS = "0123456789abcdefghijklmnopqrstuvwxyz".toCharArray();

    private static final ThreadLocalRandom random = ThreadLocalRandom.current();

    private ObjectAcl aclTemplate;
    private Map<String, TestSyncObject> idMap =
            Collections.synchronizedMap(new HashMap<String, TestSyncObject>());
    private Map<String, Set<TestSyncObject>> childrenMap =
            Collections.synchronizedMap(new HashMap<String, Set<TestSyncObject>>());

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
    public void configure(SyncStorage source, Iterator<SyncFilter> filters, SyncStorage target) {
        super.configure(source, filters, target);

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
        TestSyncObject object = idMap.get(identifier);
        if (object == null) throw new ObjectNotFoundException(identifier);
        return object.deepCopy();
    }

    @Override
    public void updateObject(String identifier, SyncObject object) {
        try {
            byte[] data = null;
            if (config.isReadData() && !object.getMetadata().isDirectory()) {
                if (config.isDiscardData()) SyncUtil.consumeAndCloseStream(object.getDataStream());
                else data = StreamUtil.readAsBytes(object.getDataStream());
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

    private void generateRandomObjects(String parentPath, long levelCount, int level) {
        if (level <= config.getMaxDepth()) {
            for (int i = 0; i < levelCount; i++) {
                boolean hasChildren = random.nextInt(100) < config.getChanceOfChildren();

                String path = new File(parentPath, "random" + i + (hasChildren ? ".dir" : ".object")).getPath();

                log.info("generating object {}", path);

                long size = random.nextLong(config.getMaxSize());

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
                    generateRandomObjects(path, random.nextInt(config.getMaxChildCount()), level + 1);
            }
        }
    }

    private byte[] randomData(int size) {
        byte[] data = new byte[size];
        random.nextBytes(data);
        return data;
    }

    private ObjectMetadata randomMetadata(boolean directory, long size) {
        ObjectMetadata metadata = new ObjectMetadata();

        metadata.setDirectory(directory);

        metadata.setContentLength(size);

        metadata.setContentType("application/octet-stream");

        metadata.setModificationTime(new Date());

        if (random.nextBoolean())
            metadata.setExpirationDate(new Date(System.currentTimeMillis() + random.nextInt(1000000) + 100000));

        for (int i = 0; i < config.getMaxMetadata(); i++) {
            String key = randChars(random.nextInt(10) + 5, true); // objectives of this test does not include UTF-8 metadata keys
            String value = randChars(random.nextInt(20) + 5, false);
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
                    int numUsers = random.nextInt(Math.min(validUsers.size(), 3));
                    for (int i = 0; i < numUsers; i++) {
                        int userIdx = random.nextInt(users.size());
                        acl.addUserGrant(users.get(userIdx), validPermissions.get(random.nextInt(validPermissions.size())));
                        users.remove(userIdx);
                    }
                }

                if (!validGroups.isEmpty()) {
                    List<String> groups = new ArrayList<>(validGroups);
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

    private void mkdirs(File path) {
        File parent = path.getParentFile();
        // don't need to create the root path
        if (ROOT_PATH.equals(parent.getPath()) || ROOT_PATH.equals(path.getPath())) return;
        mkdirs(parent);
        synchronized (this) {
            String parentParent = parent.getParent();
            if (parentParent == null) parentParent = "";
            // find parent among grandparent's children
            for (TestSyncObject object : getChildren(parentParent)) {
                if (parent.getPath().equals(getIdentifier(object.getRelativePath(), true))) return;
            }
            // create parent
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setDirectory(true);
            addChild(parentParent, new TestSyncObject(this, getRelativePath(parent.getPath(), true), metadata, null));
        }
    }

    public synchronized Set<TestSyncObject> getChildren(String identifier) {
        Set<TestSyncObject> children = childrenMap.get(identifier);
        if (children == null) {
            children = Collections.synchronizedSet(new HashSet<TestSyncObject>());
            childrenMap.put(identifier, children);
        }
        return children;
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
        File file = new File(identifier);

        // equivalent of mkdirs()
        mkdirs(file);

        // add to lookup
        idMap.put(identifier, testObject);

        // add to parent
        addChild(file.getParent(), testObject);
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

        TestSyncObject(SyncStorage source, String relativePath, final ObjectMetadata metadata) {
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

        TestSyncObject(SyncStorage source, String relativePath, ObjectMetadata metadata, byte[] data) {
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
        TestSyncObject deepCopy() {
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
            metaCopy.setModificationTime(metadata.getModificationTime());
            for (String key : metadata.getUserMetadata().keySet()) {
                ObjectMetadata.UserMetadata um = metadata.getUserMetadata().get(key);
                metaCopy.setUserMetadataValue(key, um.getValue(), um.isIndexed());
            }
            return metaCopy;
        }
    }
}
