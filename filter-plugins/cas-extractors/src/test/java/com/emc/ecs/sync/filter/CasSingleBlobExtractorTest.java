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
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.filter.CasSingleBlobExtractorConfig;
import com.emc.ecs.sync.config.storage.CasConfig;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.storage.TestStorage;
import com.emc.ecs.sync.test.TestConfig;
import com.emc.ecs.sync.util.LoggingUtil;
import com.filepool.fplibrary.FPClip;
import com.filepool.fplibrary.FPLibraryConstants;
import com.filepool.fplibrary.FPPool;
import com.filepool.fplibrary.FPTag;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.*;
import java.util.*;

public class CasSingleBlobExtractorTest {
    private static final Logger log = LoggerFactory.getLogger(CasSingleBlobExtractorTest.class);
    private static final String TARGET_ATTRIBUTE_NAME = "key-name";
    private static final String[] TARGET_PATH_VALUE_PASS = {"test-1", "test-2", "test-3",};
    private static final String[] TARGET_PATH_VALUE_FAIL = {"test-4"};
    private static final String[] ATTR_NAMES = {"random-attr-1", "random-attr-2", "random-attr-3", "random-attr-4",
            "random-----1", "random------2", "random---------3", "x-emc-invalid-meta-names", TARGET_ATTRIBUTE_NAME};
    private static final String[] ATTR_VALUES = {"random-data-1", "random-data-2", "random-data-3", "random-data-4",
            "test-1", "test-2", "test-3", "random-服务器-1,random-АБВГ-2,random-спасибо-3"};
    private String casConnection;
    private byte[] blobContent;

    @BeforeEach
    public void setup() throws Exception {
        // TODO: handle log elevation in a different way
        LoggingUtil.setRootLogLevel(Level.INFO);
        try {
            Properties syncProperties = TestConfig.getProperties();
            casConnection = syncProperties.getProperty(TestConfig.PROP_CAS_CONNECT_STRING);
            Assumptions.assumeTrue(casConnection != null);
            blobContent = new byte[13 * 1024];
            new Random().nextBytes(this.blobContent);
        } catch (FileNotFoundException e) {
            Assumptions.assumeFalse(true, "Could not load ecs-sync.properties");
        }
    }

    @Test
    public void testCasSingleBlobExtractor() throws Exception {
        FPPool pool = new FPPool(casConnection);
        List<String> clipIds = createTestClips(pool);
        try {
            // write clip file
            File clipFile = File.createTempFile("clip", "lst");
            clipFile.deleteOnExit();
            BufferedWriter writer = new BufferedWriter(new FileWriter(clipFile));
            for (String clipId : clipIds) {
                log.debug("created {}", clipId);
                writer.write(clipId);
                writer.newLine();
            }
            writer.close();

            CasSingleBlobExtractorConfig filterConfig = new CasSingleBlobExtractorConfig();
            filterConfig.setMissingBlobsAreEmptyFiles(true);
            filterConfig.setPathAttribute(TARGET_ATTRIBUTE_NAME);
            filterConfig.setPathSource(CasSingleBlobExtractorConfig.PathSource.Attribute);
            filterConfig.setAttributeNameBehavior(CasSingleBlobExtractorConfig.AttributeNameBehavior.ReplaceBadCharacters);

            SyncConfig syncConfig = new SyncConfig()
                    .withSource(new CasConfig().withConnectionString(casConnection))
                    .withTarget(new com.emc.ecs.sync.config.storage.TestConfig().withDiscardData(false))
                    .withFilters(Collections.singletonList(filterConfig))
                    .withOptions(new SyncOptions().withSourceListFile(clipFile.getPath()));

            EcsSync sync = new EcsSync();
            sync.setSyncConfig(syncConfig);
            sync.run();

            Assertions.assertEquals(1, sync.getStats().getObjectsFailed());
            Assertions.assertEquals(3, sync.getStats().getObjectsComplete());

            TestStorage target = (TestStorage) sync.getTarget();
            verify(target);
        } catch (Exception e) {
            e.printStackTrace();
            if (e instanceof RuntimeException) throw e;
            throw new RuntimeException(e);
        } finally {
            delete(pool, clipIds);
            try {
                pool.Close();
            } catch (Throwable t) {
                log.warn("failed to close pool", t);
            }
        }
    }

    @Test
    public void testTargetPathCSV() throws Exception {
        FPPool pool = new FPPool(casConnection);
        List<String> clipIds = createTestClips(pool);
        try {
            // write clip file
            File clipFile = File.createTempFile("clip", "lst");
            clipFile.deleteOnExit();
            BufferedWriter writer = new BufferedWriter(new FileWriter(clipFile));
            int i = 1;
            for (String clipId : clipIds) {
                log.debug("created {}", clipId);
                String line = String.format("%s,test-%s", clipId, i++);
                writer.write(line);
                writer.newLine();
            }
            writer.close();

            CasSingleBlobExtractorConfig filterConfig = new CasSingleBlobExtractorConfig();
            filterConfig.setMissingBlobsAreEmptyFiles(true);
            filterConfig.setPathSource(CasSingleBlobExtractorConfig.PathSource.CSV);
            filterConfig.setAttributeNameBehavior(CasSingleBlobExtractorConfig.AttributeNameBehavior.SkipBadName);

            SyncConfig syncConfig = new SyncConfig()
                    .withSource(new CasConfig().withConnectionString(casConnection))
                    .withTarget(new com.emc.ecs.sync.config.storage.TestConfig().withDiscardData(false))
                    .withFilters(Collections.singletonList(filterConfig))
                    .withOptions(new SyncOptions().withSourceListFile(clipFile.getPath()));

            EcsSync sync = new EcsSync();
            sync.setSyncConfig(syncConfig);
            sync.run();

            Assertions.assertEquals(1, sync.getStats().getObjectsFailed());
            Assertions.assertEquals(3, sync.getStats().getObjectsComplete());

            TestStorage target = (TestStorage) sync.getTarget();
            verify(target);
        } catch (Exception e) {
            e.printStackTrace();
            if (e instanceof RuntimeException) throw e;
            throw new RuntimeException(e);
        } finally {
            delete(pool, clipIds);
            try {
                pool.Close();
            } catch (Throwable t) {
                log.warn("failed to close pool", t);
            }
        }
    }

    private void verify(TestStorage target) {
        for (String key : TARGET_PATH_VALUE_PASS) {
            TestStorage.TestSyncObject testSyncObject =
                    (TestStorage.TestSyncObject) target.loadObject("/root/" + key);
            Assertions.assertEquals(key, testSyncObject.getRelativePath(), "target path does not match");
            for (ObjectMetadata.UserMetadata uMeta : testSyncObject.getMetadata().getUserMetadata().values()) {
                Assertions.assertEquals(true, Arrays.asList(ATTR_NAMES).contains(uMeta.getKey()),
                        "attribute name found was not expected");
                Assertions.assertEquals(true, Arrays.asList(ATTR_VALUES).contains(uMeta.getValue()),
                        "attribute value was not expected");
            }
            if (testSyncObject.getData().length > 0) // might have come from clip with no blob
                Assertions.assertEquals(true, Arrays.equals(blobContent, testSyncObject.getData()),
                        "blob content did not match");
        }
    }

    private void delete(FPPool pool, List<String> clipIds) throws Exception {
        System.out.print("Deleting clips");

        for (String clipId : clipIds) {
            try {
                System.out.print(".");
                FPClip.Delete(pool, clipId);
            } catch (Exception e) {
                if (e instanceof RuntimeException) throw e;
                throw new RuntimeException(e);
            }
        }
        System.out.println();
    }

    private List<String> createTestClips(FPPool pool) throws Exception {
        System.out.print("Creating clips");

        List<String> clipIds = new ArrayList<>();
        String clipId;
        FPClip clip;
        FPTag topTag, tag_one, tag_two;

        // single blob clip with two tags (one nested)
        clip = new FPClip(pool);
        topTag = clip.getTopTag();

        tag_one = new FPTag(topTag, "tag1");
        tag_one.BlobWrite(new ByteArrayInputStream(blobContent), FPLibraryConstants.FP_OPTION_CLIENT_CALCID);
        tag_one.setAttribute("random-attr-1", "random-data-1");

        tag_two = new FPTag(tag_one, "tag2");
        tag_two.setAttribute(TARGET_ATTRIBUTE_NAME, TARGET_PATH_VALUE_PASS[0]);
        tag_two.setAttribute("random-attr-2", "random-data-2");
        tag_two.setAttribute("random-attr-3", "random-data-3");
        tag_two.Close();

        tag_one.Close();
        topTag.Close();
        clipId = clip.Write();
        clip.Close();

        clipIds.add(clipId);
        System.out.print(".");

        // zero blob clip with two tags (children of root)
        clip = new FPClip(pool);
        topTag = clip.getTopTag();

        tag_one = new FPTag(topTag, "tag1");
        tag_one.setAttribute(TARGET_ATTRIBUTE_NAME, TARGET_PATH_VALUE_PASS[1]);
        tag_one.setAttribute("random-attr-1", "random-data-1");
        tag_one.setAttribute("random-attr-2", "random-data-2");
        tag_one.Close();

        tag_two = new FPTag(topTag, "tag2");
        tag_two.setAttribute("random-attr-3", "random-data-3");
        tag_two.setAttribute("random-attr-4", "random-data-4");
        tag_two.Close();

        topTag.Close();
        clipId = clip.Write();
        clip.Close();

        clipIds.add(clipId);
        System.out.print(".");

        // zero blob clip with bad metadata characters
        clip = new FPClip(pool);
        topTag = clip.getTopTag();

        tag_one = new FPTag(topTag, "tag1");
        tag_one.setAttribute(TARGET_ATTRIBUTE_NAME, TARGET_PATH_VALUE_PASS[2]);
        tag_one.setAttribute("random-服务器-1", "random-data-1");
        tag_one.setAttribute("random-АБВГ-2", "random-data-2");
        tag_one.Close();

        tag_two = new FPTag(topTag, "tag2");
        tag_two.setAttribute("random-спасибо-3", "random-data-3");
        tag_two.Close();

        topTag.Close();
        clipId = clip.Write();
        clip.Close();

        clipIds.add(clipId);
        System.out.print(".");

        // multi blob clip with two tags (children of root)
        clip = new FPClip(pool);
        topTag = clip.getTopTag();

        tag_one = new FPTag(topTag, "tag1");
        tag_one.BlobWrite(new ByteArrayInputStream(blobContent), FPLibraryConstants.FP_OPTION_CLIENT_CALCID);
        tag_one.setAttribute(TARGET_ATTRIBUTE_NAME, TARGET_PATH_VALUE_FAIL[0]);
        tag_one.setAttribute("random-attr-1", "random-data-1");
        tag_one.Close();

        tag_two = new FPTag(topTag, "tag2");
        tag_two.BlobWrite(new ByteArrayInputStream(blobContent), FPLibraryConstants.FP_OPTION_CLIENT_CALCID);
        tag_two.setAttribute("random-attr-2", "random-data-2");
        tag_two.setAttribute("random-attr-3", "random-data-3");
        tag_two.Close();

        topTag.Close();
        clipId = clip.Write();
        clip.Close();

        clipIds.add(clipId);
        System.out.print(".");

        return clipIds;
    }
}
