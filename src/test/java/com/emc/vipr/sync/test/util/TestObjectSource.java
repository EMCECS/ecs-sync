package com.emc.vipr.sync.test.util;

import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.BasicMetadata;
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

    public static List<TestSyncObject> generateRandomObjects(int thisMany, int maxSize) {
        return generateRandomObjects(null, thisMany, maxSize, 1);
    }

    private static List<TestSyncObject> generateRandomObjects(String parentPath, int thisMany, int maxSize, int level) {
        List<TestSyncObject> objects = new ArrayList<>();
        if (level <= MAX_DEPTH) {
            for (int i = 0; i < thisMany; i++) {
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

    private static byte[] randomData(int maxSize) {
        byte[] data = new byte[random.nextInt(maxSize)];
        random.nextBytes(data);
        return data;
    }

    private static SyncMetadata randomMetadata() {
        SyncMetadata metadata = new BasicMetadata();

        metadata.setContentType("application/octet-stream");

        metadata.setModifiedTime(new Date());

        Date retentionDate = new Date(System.currentTimeMillis() + random.nextInt(1000000) + 100000);
        metadata.setRetentionEnabled(random.nextBoolean());
        if (metadata.isRetentionEnabled())
            metadata.setRetentionEndDate(retentionDate);

        Date expirationDate = new Date(retentionDate.getTime() + random.nextInt(1000000) + 100000);
        metadata.setExpirationEnabled(random.nextBoolean());
        if (metadata.isExpirationEnabled())
            metadata.setExpirationDate(expirationDate);

        for (int i = 0; i < MAX_META_TAGS; i++) {
            String key = randChars(random.nextInt(10) + 5, true); // objectives of this test does not include UTF-8 metadata keys
            String value = randChars(random.nextInt(20) + 5, false);
            metadata.setUserMetadataProp(key, value);
        }

        return metadata;
    }

    private static String randChars(int count, boolean alphaNumOnly) {
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

    private List<TestSyncObject> objects;

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
        return objects.iterator();
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
}
